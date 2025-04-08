package mytool

import "sync"

var (
	JoinToBrokerhubRequest []string
	BrokerHubJoinState     map[string]int
	RequestLock            sync.Mutex
)
