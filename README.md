# Payment Channel Router (local simulator, Go)

A self-contained simulation of a **local payment-channel network** with
multi-hop routing and **atomic HTLC settlement**. Channel balances, fees and
routing state live in memory and are durably mirrored to a local write-ahead
log (WAL) on disk. No blockchain, network service, or external process is
required — a single process plays every node.

## Packages

| Package | Responsibility |
|---|---|
| `internal/model` | Domain types: `Node`, `Channel`, `FeePolicy`, WAL event payloads |
| `internal/store` | Append-only, fsynced JSON-lines WAL; torn-tail recovery on open |
| `internal/graph` | In-memory topology, directional balances, atomic HTLC reserve primitives |
| `internal/router` | Deterministic bounded multi-hop path search with fee pricing |
| `internal/settlement` | HTLC lifecycle: lock / settle / expire, concurrency control, restart recovery |
| root `pctr` | Facade (`network.go`) wiring the above and persisting registrations |

## Data model

- A channel `(A, B)` has a fixed `Capacity` and two directional balances
  `BalanceAB` + `BalanceBA = Capacity`. Sending funds along `A -> B` moves
  amount out of `BalanceAB` into `BalanceBA`; capacity is always conserved.
- Each node advertises a forwarding policy:
  `fee(amount) = BaseFee + ceil(amount * FeeRate / 1_000_000)` (`FeeRate` in
  parts-per-million).
- Only one channel per unordered node pair is allowed.

## Routing semantics

A route `n0 -> n1 -> ... -> nk` (`n0` sender, `nk` receiver) consists of `k`
channel **segments** with amounts `a_0 .. a_(k-1)`, where the last segment
carries the payment amount `a_(k-1) = amount`.

The node that **receives** a segment and forwards it onward (`n(i+1)`)
charges its own fee:

```
a_i = a_(i+1) + fee_(i+1)(a_(i+1))      (i = 0 .. k-2)
```

The sender charges nothing, and the receiver keeps the full final amount.
The sender is debited `a_0 = amount + totalFee`.

The search is a **backward Dijkstra** from receiver to sender:

- a segment is only eligible if the sender-directional balance covers its
  required amount (current holds are reflected in the spendable balance);
- hop count is bounded by `MaxHops`;
- the total fee is bounded by `MaxFee` (checked on the chosen route);
- the ranking policy is configurable:
  - `LowestFee` (default): smaller required amount (= smaller downstream
    fees), then fewer hops, then lexicographically smallest node path;
  - `ShortestPath`: fewer hops, then smaller required amount, then path.
- neighbor expansion and all tie-breaks are ordered, so the result is
  **deterministic** for a given topology and request.

## Atomic settlement (HTLC) semantics

`Manager.Lock` reserves funds hop by hop in **forward** order. Each segment
atomically:

1. appends a `SegmentReserved` WAL record and `fsync`s;
2. moves the segment amount out of the sender-directional balance into a
   locked hold (`graph.Reserve`).

If any hop cannot be reserved (e.g. a competing payment consumed the balance
between routing and reservation), **every earlier segment is released in
reverse order before the call returns**, durable release events are written,
and channel balances are exactly as before. A failed path never leaks a
reservation and never partially debits the sender.

- **Settle** (`SettleWithPreimage`): the receiver reveals the SHA-256
  preimage of the payment hash. The reveal is persisted FIRST
  (`PreimageRevealed`), then segments settle in **reverse hop order**
  (receiver hop first). On settlement each channel's receiving side keeps
  the locked segment amount; a forwarder's profit is the difference between
  the amount it received on the previous segment and the amount it sends on
  the next one — no extra in-channel movement is required.
- **Expire** (`Expire` / `Sweep`): without a reveal before the deadline,
  segments are released in **forward order** (sender first) and every hold
  unwinds to its original owner. Per-hop deadlines may decrease downstream
  (`HopGrace`) so an upstream leg never expires before its downstream leg.

This mirrors HTLC behavior: an intermediate node never has to fund both legs
out of pocket — its outgoing leg is locked before its incoming leg settles,
and on timeout the legs unwind in order.

## Concurrency

A single manager mutex serializes payment transitions, while each graph
mutation is itself atomic under the graph lock. Concurrent payments therefore
cannot double-spend or oversell: a reservation that would push a directional
balance below zero is rejected, the whole payment rolls back, and the number
of in-flight payments is bounded by `MaxConcurrent`. Limits (`MaxFee`,
`MaxHops`, `MaxConcurrent`) and malformed requests are rejected **before any
state change**.

## Persistence and crash recovery

Every state transition is journaled before it is applied in memory. The WAL
is JSON-per-line with a monotonic sequence number and is `fsync`-ed per
append. On open, a torn final line from a crash mid-write is detected and
truncated, then the whole log is replayed.

On restart `Manager.Recover` finishes or safely reverts each payment:

- **preimage revealed / mid-settlement** → remaining segments settle in
  reverse order; the payment completes exactly once (no double settlement);
- **locked, deadline passed** → all segments release forward, payment aborts;
- **locked, within deadline** → holds are restored intact and the payment can
  still be settled or later expire;
- **orphan reserves** (crash after some `SegmentReserved` records but before
  `PaymentReserved`) → released, no funds stranded.

Settlement and release are idempotent against already-finalized holds, so
replaying or recovering repeatedly neither loses nor duplicates a settlement.

## Limits

| Config | Effect |
|---|---|
| `MaxHops` | reject routes longer than N segments |
| `MaxFee` | reject routes whose total forwarding fee exceeds the cap |
| `MaxConcurrent` | reject new locks while N payments are in flight (0 = unlimited) |
| `LockDuration` / `HopGrace` | HTLC lifetime and per-hop deadline decrement |

## Example

See [`example_test.go`](./example_test.go) for a runnable walkthrough:
topology registration, a fee-priced multi-hop payment, and restart recovery.

## Build & test

```sh
go build ./...
go test ./... -race -count=1
```
