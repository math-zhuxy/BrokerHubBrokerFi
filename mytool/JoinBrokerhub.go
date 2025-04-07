package mytool

import "sync"

var (
	JoinToBrokerhubRequest []string
	RequestLock            sync.Mutex
)
