package settlement

import (
	"strconv"
	"strings"
)

// segIndex extracts the hop index from a segment id "<payment>:seg:<i>".
func segIndex(id string) int {
	const marker = ":seg:"
	if i := strings.LastIndex(id, marker); i >= 0 {
		if n, err := strconv.Atoi(id[i+len(marker):]); err == nil {
			return n
		}
	}
	return 0
}
