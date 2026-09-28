# payment-channel-router-go

本地模拟的支付通道网络：节点 / 通道注册、多跳路由搜索、基于哈希时间锁（HTLC）
的原子跨通道结算，全部状态保存在进程内存并通过本地只追加 WAL（write-ahead
log）持久化，**不依赖任何外部链上服务**。

## 包结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 错误、节点 / 通道 / 路由 / 支付类型，费用策略 |
| `network.go` | 节点与通道注册、查询、方向余额管理，按通道互斥的原子变更 |
| `router.go` | 多跳路径搜索（反向标签 + 优先队列），策略可配、结果确定 |
| `payment.go` | 支付生命周期：预留（锁定）、前向结算、超时反向退款 |
| `store.go` | `Store` 接口与 `FileStore`（JSON-lines WAL，逐记录 fsync） |
| `recovery.go` | WAL 两阶段重放与中断支付的继续完成 / 安全回滚 |

## 快速上手

```go
store, _ := pchr.NewFileStore("wal.jsonl")
net := pchr.NewNetwork(store)
_ = net.RegisterNode("A", "addr-a", nil)
_ = net.RegisterNode("B", "addr-b", nil)
_ = net.OpenChannel(pchr.Channel{
    ID: "ab", PeerA: "A", PeerB: "B",
    BalanceA: 1000, BalanceB: 500,
    FeePolicy: pchr.FeePolicy{BaseFee: 1, FeeRateMillionths: 1000},
})

cfg := pchr.DefaultRoutingConfig() // 5 跳 / 费用上限 1_000_000 / 64 并发
mgr := pchr.NewManager(net, nil, store, cfg)

var preimage [32]byte // 由付款人生成并保密
_, _ = rand.Read(preimage[:])
pay, err := mgr.SendPayment("A", "B", 100, preimage)
```

进程重启后用同一个 WAL 文件恢复，全部节点 / 通道 / 支付状态自动重建：

```go
store, _ := pchr.NewFileStore("wal.jsonl")
net := pchr.NewNetwork(store)
mgr := pchr.NewManager(net, nil, store, cfg)
if err := pchr.Recover(store, net, mgr); err != nil { /* ... */ }
```

## 通道与余额模型

- 通道有两个对等节点 `PeerA / PeerB`，分别持有方向余额 `BalanceA / BalanceB`。
- 支付进行中会把资金从可花费余额移入 `LockedA / LockedB`：
  `可花费 = Balance - Locked`。
- 同一通道的所有变更通过该通道自己的 `sync.RWMutex` 串行化；一次支付涉及多个
  通道时，**按通道 ID 升序一次性获取全部通道锁**，既保证多跳预留的原子性，也
  消除了多支付并发时的锁顺序死锁。
- 支付相关的通道变更（预留 / 结算 / 退款）在内存中修改后，与支付游标一起写入
  **单条原子 WAL 记录**（见下），因此「同一对节点之间通道状态的修改」要么完整
  提交并可恢复，要么整体不可见。

## 路由语义

路径搜索从**收款方向付款方反向扩展**标签，这样每一跳的金额都按「下一跳实际
转发的金额」精确推导：

```
收款金额 amount（终端边不转发，不收转发费）
hop_n   : in_n = amount,                 fee_n = 0
hop_n-1 : in_n-1 = in_n + fee_{n-1}      （转发节点 n-1 对其出边收的费）
...
付款总额 = in_0, 总费用 = in_0 - amount
```

通道的 `FeePolicy` 表示该通道**接收方（转发节点）为把资金继续向下游转发而
收取的费用**，因此终端边（连接最终收款人的边）转发费为 0；这笔费用加在付款
侧进入该转发节点的那一跳上。费用策略为
`BaseFee + ceil(out * FeeRateMillionths / 1_000_000)`。

结算时每个通道都在本通道两端之间**全额转移该跳 HTLC 金额**（付款方 −in，
收款方 +in），手续费并不在单个通道内扣减；它自然体现为中间节点在两条通道上
的净头寸：`收入边 in − 支出边 out = 该节点转发费`。例如 A→B→C→D 支付 100，
三跳携带 103 / 102 / 100，则 B 净赚 1（103−102）、C 净赚 2（102−100）、
D 实收 100、A 实付 103，全网资金守恒。

可配置策略（`RoutingConfig.Strategy`）：

- `StrategyCheapestFee`（默认）：总转发费用最少，其次跳数最少；
- `StrategyShortest`：跳数最少，其次总费用最少。

确定性保证：节点的邻接通道按通道 ID 排序遍历；所有指标完全相同的候选路径，以
**通道 ID 序列字典序最小**者胜出。相同输入与网络状态永远得到相同路径。

搜索逐边校验：通道处于 `Active`、方向可花费余额（扣除已有锁定）足以承载该跳的
`amount in`、路径为简单路径（不重复节点）、跳数不超过 `MaxHops`。

## HTLC 原子结算语义

支付状态机：

```
pending -> locked -> settling -> succeeded
                   \-> refunding -> refunded
pending(失败) -> failed
```

1. **锁定（预留）**：付款人持有全部通道锁后，沿路径逐跳把 `amount in` 从可花费
   余额移入锁定余额。中间节点只是「预留」了上游和下游两段资金，**没有任何节点
   同时垫付两段**（上游资金锁定在先，且结算必须凭 preimage 才能完成）。
2. **原子提交锁定结果**：`payment_locked` 是一条 WAL 记录，同时包含支付与所有
   通道预留后的完整快照。它要么整体可见（恢复出「支付 + 预留」），要么整体不可
   见（恢复出支付前状态），因此不会出现「钱被锁但没有支付」或「有支付但无预
   留」。
3. **揭示 preimage（settling）**：结算开始前校验 `SHA256(preimage) == hash`，
   并先持久化 `payment_settling(preimage)`。preimage 一旦揭示，支付只能向前完成。
4. **前向结算**：跳 0 → n-1 逐跳提交。每一跳的「游标 `settled_hops` + 结算后通
   道快照」是一条原子 `hop_settled` 记录。崩溃恢复依据游标跳过已结算跳，每跳
   **至多结算一次**。
5. **超时退款**：preimage 揭示前且超过 `ExpiresAt` 的 `locked` 支付，按
   **最后一跳 → 第一跳** 的顺序逐跳释放预留。每一跳的「反向释放计数 + 通道快
   照」是一条原子 `hop_refunded` 记录，崩溃后从未释放的那一跳继续。退款不改变
   任何已结算余额，只把锁定额还原为可花费额。

> 已进入 `settling` 的支付不能再退款：前序跳可能已经凭 preimage 完成了真实结
> 算，原子性要求它只能继续结算到终点；尚未揭示 preimage 的支付则由超时路径安全
> 回滚。

## 并发与防双花

- 路由阶段只读（通道读锁下拷贝快照），不修改状态；
- 预留阶段一次性按序持有路径上的全部通道锁，`reserve` 在锁内再次校验可花费余
  额，因此并发支付竞争同一通道时，超出余额的支付预留失败，并**按反向顺序释放本
  支付已取得的全部预留**，通道状态恢复到尝试之前；
- 管理器用 `m.mu` 串行化支付元数据（状态、游标、在途计数）。通道锁与 `m.mu`
  永远不反向嵌套，避免 AB-BA 死锁。

## 限制与拒绝策略（拒绝时通道状态保持不变）

| 配置 | 超限行为 |
| --- | --- |
| `MaxHops` | 路径搜索无可达路径，返回 `ErrNoRoute` / `ErrTooManyHops` |
| `MaxTotalFee` | 路由总费用高于上限，返回 `ErrFeeExceeded`，不预留任何资金 |
| `MaxConcurrent` | 在途（非终态）支付达到上限，返回 `ErrTooManyConcurrent` |
| 余额不足 / 方向不符 | `ErrInsufficientFunds`，部分预留全部回滚 |
| preimage 与 hash 不符 | `ErrPreimageMismatch`，支付保持原状态 |
| 参数非法（金额 ≤ 0、空 ID、负余额 / 负费率等） | `ErrInvalidArgument` |

## 故障恢复（WAL）

`Recover` 执行两遍重放：

1. **通道 / 节点状态**：`node_registered`、`channel_opened`、`channel_updated`，
   以及支付复合记录（`payment_locked` 的快照集合、`hop_settled` /
   `hop_refunded` 的单通道快照）全部按 WAL 顺序安装；
2. **支付生命周期**：`payment_locked` / `payment_settling` / `hop_settled` /
   `payment_succeeded` / `hop_refunded` / `payment_refunded` / `payment_failed`
   重建支付状态与游标。

重放完成后对未决支付做处置：

- `settling`（preimage 已揭示，可能已有跳结算）→ 用持久化的 preimage
  **继续前向结算到成功**；
- `locked` 且超时，或已开始退款（`refunded_count > 0`）→ **继续反向退款到完成**；
- 未超时的 `locked` 支付保持锁定，等待 `SettlePayment` 或超时后 `RefundPayment`。

WAL 为每行一条 JSON 记录，`Append` 后立即 `fsync`。若崩溃发生在一次写盘途
中，末尾残缺行在重开时会被识别并截断，之前所有完整记录不受影响。

## 测试

```
go test -race -count=1 ./...
```

覆盖场景：

- 节点 / 通道注册、重复注册、方向余额查询；
- 两种路由策略、方向性余额约束、跳数限制、费用精确后向累积、确定性 tie-break；
- 多跳成功后的各通道余额与费用归属；
- 并发支付对同一通道的竞争（只允许一笔成功，无双花 / 超卖，无锁定泄漏）；
- 路由阶段与预留阶段失败后的全部预留释放；
- 费用上限、最大跳数、最大并发数拒绝且通道状态不变；
- 超时反向退款与幂等；
- 基于真实 `FileStore` 的重启恢复：拓扑重建、多跳成功重放；
- 结算中途崩溃后重启**恰好一次**续做完成；
- 退款中途崩溃后重启继续反向退款，余额全额恢复；
- 未过期锁定支付重启后仍可结算；错误 preimage 在重启后仍被拒绝；
- WAL 序号连续性、顺序重放、尾部残缺行截断且不影响后续追加。
