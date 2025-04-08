package message

import (
	"blockEmulator/utils"
	"math/big"
)

type BrokerInfoInBrokerhub struct {
	BrokerAddr    utils.Address
	BrokerBalance *big.Int
	BrokerProfit  *big.Float
}
