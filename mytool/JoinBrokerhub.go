package mytool

import "sync"

var (
	JoinToBrokerhubRequest []string
	BrokerHubJoinState     map[string]uint
	RequestLock            sync.Mutex
)
