package committee

import (
	"blockEmulator/broker"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/mytool"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/Broker2Earn"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CLPA committee operations
type BrokerhubCommitteeMod struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	dataTxNums   int
	batchDataNum int

	//Broker related  attributes avatar
	Broker                *broker.Broker
	brokerConfirm1Pool    map[string]*message.Mag1Confirm
	brokerConfirm2Pool    map[string]*message.Mag2Confirm
	restBrokerRawMegPool  []*message.BrokerRawMeg
	restBrokerRawMegPool2 []*message.BrokerRawMeg
	brokerTxPool          []*core.Transaction
	BrokerModuleLock      sync.Mutex
	BrokerBalanceLock     sync.Mutex

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss          *signal.StopSignal // to control the stop message sending
	IpNodeTable map[uint64]map[uint64]string

	// log balance
	Result_lockBalance   map[string][]string
	Result_brokerBalance map[string][]string
	Result_Profit        map[string][]string
	LastInvokeTime       map[string]time.Time
	LastInvokeTimeMutex  sync.Mutex
}

const brokerHubAccountId string = "40ce7569d555dbf939e58867be78fd76142df821"

func NewBrokerhubCommitteeMod(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum int) *BrokerhubCommitteeMod {
	fmt.Println("Using Brokerhub Supervisor")
	broker := new(broker.Broker)
	broker.NewBroker(nil)
	result_lockBalance := make(map[string][]string)
	result_brokerBalance := make(map[string][]string)
	result_Profit := make(map[string][]string)
	block_txs := make(map[uint64][]string)

	for _, brokeraddress := range broker.BrokerAddress {
		result_lockBalance[brokeraddress] = make([]string, 0)
		result_brokerBalance[brokeraddress] = make([]string, 0)
		result_Profit[brokeraddress] = make([]string, 0)

		a := ""
		b := ""
		title := ""
		for i := 0; i < params.ShardNum; i++ {
			title += "shard" + strconv.Itoa(i) + ","
			a += params.Init_broker_Balance.String() + ","
			b += "0,"
		}
		result_lockBalance[brokeraddress] = append(result_lockBalance[brokeraddress], title)
		result_brokerBalance[brokeraddress] = append(result_brokerBalance[brokeraddress], title)
		result_Profit[brokeraddress] = append(result_Profit[brokeraddress], title)

		result_lockBalance[brokeraddress] = append(result_lockBalance[brokeraddress], b)
		result_brokerBalance[brokeraddress] = append(result_brokerBalance[brokeraddress], a)
		result_Profit[brokeraddress] = append(result_Profit[brokeraddress], b)
	}
	for i := 0; i < params.ShardNum; i++ {
		block_txs[uint64(i)] = make([]string, 0)
		block_txs[uint64(i)] = append(block_txs[uint64(i)], "txExcuted, broker1Txs, broker2Txs, allocatedTxs")
	}
	// 初始化账户加入BrokerHub的表
	mytool.BrokerHubJoinState = make(map[string]uint)

	return &BrokerhubCommitteeMod{
		csvPath:               csvFilePath,
		dataTotalNum:          dataNum,
		batchDataNum:          batchNum,
		nowDataNum:            0,
		dataTxNums:            0,
		brokerConfirm1Pool:    make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool:    make(map[string]*message.Mag2Confirm),
		restBrokerRawMegPool:  make([]*message.BrokerRawMeg, 0),
		restBrokerRawMegPool2: make([]*message.BrokerRawMeg, 0),
		brokerTxPool:          make([]*core.Transaction, 0),
		Broker:                broker,
		IpNodeTable:           Ip_nodeTable,
		Ss:                    Ss,
		sl:                    sl,
		Result_lockBalance:    result_lockBalance,
		Result_brokerBalance:  result_brokerBalance,
		Result_Profit:         result_Profit,
		LastInvokeTime:        make(map[string]time.Time),
	}

}
func (bcm *BrokerhubCommitteeMod) HandleOtherMessage([]byte) {}
func (bcm *BrokerhubCommitteeMod) fetchModifiedMap(key string) uint64 {
	return uint64(utils.Addr2Shard(key))
}

func (bcm *BrokerhubCommitteeMod) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, bcm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			//time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := bcm.fetchModifiedMap(tx.Sender)

		//if bcm.Broker.IsBroker(tx.Sender) {
		//	sendersid = bcm.fetchModifiedMap(tx.Recipient)
		//}
		if tx.Isbrokertx2 {
			sendersid = bcm.fetchModifiedMap(tx.Recipient)
		}
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (bcm *BrokerhubCommitteeMod) JoiningToBrokerhub() {
	mytool.RequestLock.Lock()
	if len(mytool.JoinToBrokerhubRequest) != 0 {
		balance := big.NewInt(0)
		for _, val := range mytool.JoinToBrokerhubRequest {
			acc_balances, exist := bcm.Broker.BrokerBalance[val]
			if !exist {
				continue
			}
			for _, val := range acc_balances {
				balance.Add(balance, val)
			}
			bcm.sl.Slog.Printf("account %s join to brokerhub", val)
		}
		bcm.BrokerBalanceLock.Lock()
		bcm.Broker.BrokerBalance[brokerHubAccountId][0].Add(
			bcm.Broker.BrokerBalance[brokerHubAccountId][0],
			balance,
		)
		bcm.BrokerBalanceLock.Unlock()
	}

	// reset join brokerhub list
	mytool.JoinToBrokerhubRequest = mytool.JoinToBrokerhubRequest[:0]

	mytool.RequestLock.Unlock()
}

func (bcm *BrokerhubCommitteeMod) MsgSendingControl() {

	go func() {
		for {
			time.Sleep(time.Millisecond * 1000)

			//itx := bcm.dealTxByBroker(getRandomTxs(100))
			//bcm.txSending(itx)
			txs := getRandomTxs(100)
			//mytool.Mutex.Lock()
			//mytool.AutoGeneratedB2EQueue = append(mytool.AutoGeneratedB2EQueue, txs...)
			//mytool.Mutex.Unlock()

			itx := bcm.dealTxByBroker(txs)
			//bcm.BrokerModuleLock.Unlock()
			bcm.txSending(itx)

		}
	}()

	for {

		time.Sleep(time.Millisecond * 100)

		bcm.JoiningToBrokerhub()

		mytool.Mutex1.Lock()
		if len(mytool.UserRequestB2EQueue) == 0 {
			mytool.Mutex1.Unlock()
			continue
		}

		queueCopy := make([]*core.Transaction, len(mytool.UserRequestB2EQueue))
		copy(queueCopy, mytool.UserRequestB2EQueue)
		mytool.UserRequestB2EQueue = mytool.UserRequestB2EQueue[:0]

		mytool.Mutex1.Unlock()

		//bcm.BrokerModuleLock.Lock()
		itx := bcm.dealTxByBroker2(queueCopy)
		//bcm.BrokerModuleLock.Unlock()
		bcm.txSending(itx)

	}

}

func (bcm *BrokerhubCommitteeMod) HandleBlockInfo(b *message.BlockInfoMsg) {

	bcm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if b.BlockBodyLength == 0 {
		return
	}
	//fmt.Println("HandleBlockInfo.... ", b.BlockBodyLength)

	// add createConfirm
	txs := make([]*core.Transaction, 0)
	txs = append(txs, b.Broker1Txs...)
	txs = append(txs, b.Broker2Txs...)
	bcm.BrokerModuleLock.Lock()
	// when accept ctx1, update all accounts
	bcm.BrokerBalanceLock.Lock()
	//println("block length is ", len(b.ExcutedTxs))
	for _, tx := range b.Broker1Txs {
		brokeraddress, sSid, rSid := tx.Recipient, bcm.fetchModifiedMap(tx.OriginalSender), bcm.fetchModifiedMap(tx.FinalRecipient)

		if !bcm.Broker.IsBroker(brokeraddress) {
			continue
		}

		if bcm.Broker.LockBalance[brokeraddress][rSid].Cmp(tx.Value) < 0 {
			continue
		}
		bcm.Broker.LockBalance[brokeraddress][rSid].Sub(bcm.Broker.LockBalance[brokeraddress][rSid], tx.Value)
		bcm.Broker.BrokerBalance[brokeraddress][sSid].Add(bcm.Broker.BrokerBalance[brokeraddress][sSid], tx.Value)

		fee := new(big.Float).SetInt64(tx.Fee.Int64())

		fee = fee.Mul(fee, bcm.Broker.Brokerage)

		bcm.Broker.ProfitBalance[brokeraddress][sSid].Add(bcm.Broker.ProfitBalance[brokeraddress][sSid], fee)

	}
	//bcm.add_result()
	bcm.BrokerBalanceLock.Unlock()
	bcm.BrokerModuleLock.Unlock()
	bcm.createConfirm(txs)
}

func (bcm *BrokerhubCommitteeMod) createConfirm(txs []*core.Transaction) {
	confirm1s := make([]*message.Mag1Confirm, 0)
	confirm2s := make([]*message.Mag2Confirm, 0)
	bcm.BrokerModuleLock.Lock()
	for _, tx := range txs {
		if confirm1, ok := bcm.brokerConfirm1Pool[string(tx.TxHash)]; ok {
			confirm1s = append(confirm1s, confirm1)
		}
		if confirm2, ok := bcm.brokerConfirm2Pool[string(tx.TxHash)]; ok {
			confirm2s = append(confirm2s, confirm2)
		}
	}
	bcm.BrokerModuleLock.Unlock()

	if len(confirm1s) != 0 {
		bcm.handleTx1ConfirmMag(confirm1s)
	}

	if len(confirm2s) != 0 {
		bcm.handleTx2ConfirmMag(confirm2s)
	}
}

func (bcm *BrokerhubCommitteeMod) dealTxByBroker(txs []*core.Transaction) (itxs []*core.Transaction) {
	bcm.BrokerBalanceLock.Lock()
	fmt.Println("dealTxByBroker:", len(txs))
	itxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
	//copy(brokerRawMegs, bcm.restBrokerRawMegPool)
	// for _, item := range bcm.restBrokerRawMegPool {
	// 	brokerRawMegs = append(brokerRawMegs, item)
	// }
	brokerRawMegs = append(brokerRawMegs, bcm.restBrokerRawMegPool...)
	bcm.restBrokerRawMegPool = make([]*message.BrokerRawMeg, 0)

	//println("0brokerSize ", len(brokerRawMegs))
	for _, tx := range txs {

		tx.Recipient = FormatStringToLength(tx.Recipient, 40)
		if tx.Recipient == "error" {
			continue
		}

		tx.Sender = FormatStringToLength(tx.Sender, 40)
		if tx.Sender == "error" {
			continue
		}

		if tx.Recipient == tx.Sender {
			continue
		}

		rSid := bcm.fetchModifiedMap(tx.Recipient)
		sSid := bcm.fetchModifiedMap(tx.Sender)
		//if rSid != sSid && !bcm.Broker.IsBroker(tx.Recipient) && !bcm.Broker.IsBroker(tx.Sender) {
		if rSid != sSid {
			brokerRawMeg := &message.BrokerRawMeg{
				Tx:     tx,
				Broker: bcm.Broker.BrokerAddress[0],
			}
			brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
		} else {
			if bcm.Broker.IsBroker(tx.Recipient) || bcm.Broker.IsBroker(tx.Sender) {
				tx.HasBroker = true
				tx.SenderIsBroker = bcm.Broker.IsBroker(tx.Sender)
			}
			itxs = append(itxs, tx)
		}
	}

	if len(brokerRawMegs) > 1000 {

		brokerRawMegs = brokerRawMegs[:1000]
	}

	//println("1brokerSize ", len(brokerRawMegs))
	now := time.Now()
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.Broker.BrokerBalance)
	println("consume time(millsec.) ", time.Since(now).Milliseconds())
	// for _, item := range restBrokerRawMeg {
	// 	bcm.restBrokerRawMegPool = append(bcm.restBrokerRawMegPool, item)
	// }
	bcm.restBrokerRawMegPool = append(bcm.restBrokerRawMegPool, restBrokerRawMeg...)
	//println("2brokerSize ", len(alloctedBrokerRawMegs))
	//println("restBrokerRawMegSize ", len(restBrokerRawMeg))
	//println("itx size ", len(itxs))

	allocatedTxs := bcm.GenerateAllocatedTx(alloctedBrokerRawMegs)
	if len(alloctedBrokerRawMegs) != 0 {
		bcm.handleAllocatedTx(allocatedTxs)
		bcm.lockToken(alloctedBrokerRawMegs)
		bcm.BrokerBalanceLock.Unlock()
		bcm.handleBrokerRawMag(alloctedBrokerRawMegs)
	} else {
		bcm.BrokerBalanceLock.Unlock()
	}
	return itxs
}

func (bcm *BrokerhubCommitteeMod) dealTxByBroker2(txs []*core.Transaction) (itxs []*core.Transaction) {
	bcm.BrokerBalanceLock.Lock()
	fmt.Println("dealTxByBroker:", len(txs))
	itxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
	//copy(brokerRawMegs, bcm.restBrokerRawMegPool)
	// for _, item := range bcm.restBrokerRawMegPool2 {
	// 	brokerRawMegs = append(brokerRawMegs, item)
	// }
	brokerRawMegs = append(brokerRawMegs, bcm.restBrokerRawMegPool2...)
	bcm.restBrokerRawMegPool2 = make([]*message.BrokerRawMeg, 0)

	//println("0brokerSize ", len(brokerRawMegs))
	for _, tx := range txs {

		tx.Recipient = FormatStringToLength(tx.Recipient, 40)
		if tx.Recipient == "error" {
			continue
		}

		tx.Sender = FormatStringToLength(tx.Sender, 40)
		if tx.Sender == "error" {
			continue
		}

		if tx.Recipient == tx.Sender {
			continue
		}

		rSid := bcm.fetchModifiedMap(tx.Recipient)
		sSid := bcm.fetchModifiedMap(tx.Sender)
		//if rSid != sSid && !bcm.Broker.IsBroker(tx.Recipient) && !bcm.Broker.IsBroker(tx.Sender) {
		if rSid != sSid {
			brokerRawMeg := &message.BrokerRawMeg{
				Tx:     tx,
				Broker: bcm.Broker.BrokerAddress[0],
			}
			brokerRawMegs = append(brokerRawMegs, brokerRawMeg)
		} else {
			if bcm.Broker.IsBroker(tx.Recipient) || bcm.Broker.IsBroker(tx.Sender) {
				tx.HasBroker = true
				tx.SenderIsBroker = bcm.Broker.IsBroker(tx.Sender)
			}
			itxs = append(itxs, tx)
		}
	}

	//println("1brokerSize ", len(brokerRawMegs))
	now := time.Now()
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.Broker.BrokerBalance)
	println("consume time(millsec.) ", time.Since(now).Milliseconds())
	// for _, item := range restBrokerRawMeg {
	// 	bcm.restBrokerRawMegPool2 = append(bcm.restBrokerRawMegPool2, item)
	// }
	bcm.restBrokerRawMegPool2 = append(bcm.restBrokerRawMegPool2, restBrokerRawMeg...)
	//println("2brokerSize ", len(alloctedBrokerRawMegs))
	//println("restBrokerRawMegSize ", len(restBrokerRawMeg))
	//println("itx size ", len(itxs))

	allocatedTxs := bcm.GenerateAllocatedTx(alloctedBrokerRawMegs)
	if len(alloctedBrokerRawMegs) != 0 {
		bcm.handleAllocatedTx(allocatedTxs)
		bcm.lockToken(alloctedBrokerRawMegs)
		bcm.BrokerBalanceLock.Unlock()
		bcm.handleBrokerRawMag(alloctedBrokerRawMegs)
	} else {
		bcm.BrokerBalanceLock.Unlock()
	}
	return itxs
}

func (bcm *BrokerhubCommitteeMod) lockToken(alloctedBrokerRawMegs []*message.BrokerRawMeg) {
	//bcm.BrokerBalanceLock.Lock()

	for _, brokerRawMeg := range alloctedBrokerRawMegs {
		tx := brokerRawMeg.Tx
		brokerAddress := brokerRawMeg.Broker
		rSid := bcm.fetchModifiedMap(tx.Recipient)

		if !bcm.Broker.IsBroker(brokerAddress) {
			continue
		}

		bcm.Broker.LockBalance[brokerAddress][rSid].Add(bcm.Broker.LockBalance[brokerAddress][rSid], tx.Value)
		bcm.Broker.BrokerBalance[brokerAddress][rSid].Sub(bcm.Broker.BrokerBalance[brokerAddress][rSid], tx.Value)
	}

	//bcm.BrokerBalanceLock.Unlock()
}
func (bcm *BrokerhubCommitteeMod) handleAllocatedTx(alloctedTx map[uint64][]*core.Transaction) {

	//bcm.BrokerBalanceLock.Lock()

	for shardId, txs := range alloctedTx {
		for _, tx := range txs {
			if tx.IsAllocatedSender {
				bcm.Broker.BrokerBalance[tx.Sender][shardId].Sub(bcm.Broker.BrokerBalance[tx.Sender][shardId], tx.Value)
			}
			if tx.IsAllocatedRecipent {
				bcm.Broker.BrokerBalance[tx.Recipient][shardId].Add(bcm.Broker.BrokerBalance[tx.Recipient][shardId], tx.Value)
			}
		}
	}
	//bcm.BrokerBalanceLock.Unlock()

}

func (bcm *BrokerhubCommitteeMod) GenerateAllocatedTx(alloctedBrokerRawMegs []*message.BrokerRawMeg) map[uint64][]*core.Transaction {
	//bcm.Broker.BrokerBalance
	brokerNewBalance := make(map[string]map[uint64]*big.Int)
	brokerChange := make(map[string]map[uint64]*big.Int)
	brokerPeekChange := make(map[string]map[uint64]*big.Int)

	// 1. init
	alloctedTxs := make(map[uint64][]*core.Transaction)
	for i := 0; i < params.ShardNum; i++ {
		alloctedTxs[uint64(i)] = make([]*core.Transaction, 0)
	}

	//bcm.BrokerBalanceLock.Lock()
	for brokerAddress, shardMap := range bcm.Broker.BrokerBalance {
		brokerNewBalance[brokerAddress] = make(map[uint64]*big.Int)
		brokerChange[brokerAddress] = make(map[uint64]*big.Int)
		brokerPeekChange[brokerAddress] = make(map[uint64]*big.Int)
		for shardId, balance := range shardMap {
			brokerNewBalance[brokerAddress][shardId] = new(big.Int).Set(balance)
			brokerChange[brokerAddress][shardId] = big.NewInt(0)
			brokerPeekChange[brokerAddress][shardId] = new(big.Int).Set(balance)
		}

	}
	//bcm.BrokerBalanceLock.Unlock()

	for _, brokerRawMeg := range alloctedBrokerRawMegs {
		sSid := bcm.fetchModifiedMap(brokerRawMeg.Tx.Sender)
		rSid := bcm.fetchModifiedMap(brokerRawMeg.Tx.Recipient)
		brokerAddress := brokerRawMeg.Broker

		brokerNewBalance[brokerAddress][sSid].Add(brokerNewBalance[brokerAddress][sSid], brokerRawMeg.Tx.Value)
		brokerNewBalance[brokerAddress][rSid].Sub(brokerNewBalance[brokerAddress][rSid], brokerRawMeg.Tx.Value)

		brokerPeekChange[brokerAddress][rSid].Sub(brokerPeekChange[brokerAddress][rSid], brokerRawMeg.Tx.Value)
	}

	for brokerAddress, shardMap := range brokerPeekChange {
		for shardId := range shardMap {

			peekBalance := brokerPeekChange[brokerAddress][shardId]

			if peekBalance.Cmp(big.NewInt(0)) < 0 {
				// If FromShard does not have enough balance, find another shard to cover the deficit

				deficit := new(big.Int).Set(peekBalance)
				deficit.Abs(deficit)
				for id, balance := range brokerPeekChange[brokerAddress] {
					if deficit.Cmp(big.NewInt(0)) == 0 {
						break
					}
					if id != shardId && balance.Cmp(big.NewInt(0)) > 0 {
						tmpValue := new(big.Int).Set(deficit)
						if balance.Cmp(deficit) < 0 {
							tmpValue.Set(balance)
							deficit.Sub(deficit, balance)
						} else {
							deficit.SetInt64(0)
						}
						brokerNewBalance[brokerAddress][id].Sub(brokerNewBalance[brokerAddress][id], tmpValue)
						brokerNewBalance[brokerAddress][shardId].Add(brokerNewBalance[brokerAddress][shardId], tmpValue)

						brokerPeekChange[brokerAddress][id].Sub(brokerPeekChange[brokerAddress][id], tmpValue)
						brokerPeekChange[brokerAddress][shardId].Add(brokerPeekChange[brokerAddress][shardId], tmpValue)

						brokerChange[brokerAddress][id].Sub(brokerChange[brokerAddress][id], tmpValue)
						brokerChange[brokerAddress][shardId].Add(brokerChange[brokerAddress][shardId], tmpValue)
					}
				}
			}
		}

	}
	// generate allocated tx

	for brokerAddress, shardMap := range brokerChange {
		for shardId := range shardMap {

			diff := brokerChange[brokerAddress][shardId]

			if diff.Cmp(big.NewInt(0)) == 0 {
				continue
			}
			tx := core.NewTransaction(brokerAddress, brokerAddress, new(big.Int).Abs(diff), uint64(bcm.nowDataNum), big.NewInt(0))

			bcm.nowDataNum++
			if diff.Cmp(big.NewInt(0)) < 0 {
				tx.IsAllocatedSender = true
			} else {
				tx.IsAllocatedRecipent = true
			}
			alloctedTxs[shardId] = append(alloctedTxs[shardId], tx)
		}

	}

	//bcm.BrokerBalanceLock.Unlock()
	return alloctedTxs
}

func (bcm *BrokerhubCommitteeMod) handleBrokerType1Mes(brokerType1Megs []*message.BrokerType1Meg) {
	tx1s := make([]*core.Transaction, 0)
	for _, brokerType1Meg := range brokerType1Megs {
		ctx := brokerType1Meg.RawMeg.Tx
		tx1 := core.NewTransaction(ctx.Sender, brokerType1Meg.Broker, ctx.Value, ctx.Nonce, ctx.Fee)
		tx1.OriginalSender = ctx.Sender
		tx1.FinalRecipient = ctx.Recipient
		tx1.RawTxHash = make([]byte, len(ctx.TxHash))
		tx1.Isbrokertx1 = true
		tx1.Isbrokertx2 = false
		copy(tx1.RawTxHash, ctx.TxHash)
		tx1s = append(tx1s, tx1)
		confirm1 := &message.Mag1Confirm{
			RawMeg:  brokerType1Meg.RawMeg,
			Tx1Hash: tx1.TxHash,
		}
		bcm.BrokerModuleLock.Lock()
		bcm.brokerConfirm1Pool[string(tx1.TxHash)] = confirm1
		bcm.BrokerModuleLock.Unlock()
	}
	bcm.txSending(tx1s)
	fmt.Println("BrokerType1Mes received by shard,  add brokerTx1 len ", len(tx1s))
}

func (bcm *BrokerhubCommitteeMod) handleBrokerType2Mes(brokerType2Megs []*message.BrokerType2Meg) {
	tx2s := make([]*core.Transaction, 0)
	for _, mes := range brokerType2Megs {
		ctx := mes.RawMeg.Tx
		tx2 := core.NewTransaction(mes.Broker, ctx.Recipient, ctx.Value, ctx.Nonce, ctx.Fee)
		tx2.OriginalSender = ctx.Sender
		tx2.FinalRecipient = ctx.Recipient
		tx2.RawTxHash = make([]byte, len(ctx.TxHash))
		tx2.Isbrokertx2 = true
		tx2.Isbrokertx1 = false
		copy(tx2.RawTxHash, ctx.TxHash)
		tx2s = append(tx2s, tx2)

		confirm2 := &message.Mag2Confirm{
			RawMeg:  mes.RawMeg,
			Tx2Hash: tx2.TxHash,
		}
		bcm.BrokerModuleLock.Lock()
		bcm.brokerConfirm2Pool[string(tx2.TxHash)] = confirm2
		bcm.BrokerModuleLock.Unlock()
	}
	bcm.txSending(tx2s)
	//fmt.Println("Broker tx2 add to pool len ", len(tx2s))
}

// get the digest of rawMeg
func (bcm *BrokerhubCommitteeMod) getBrokerRawMagDigest(r *message.BrokerRawMeg) []byte {
	b, err := json.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	hash := sha256.Sum256(b)
	return hash[:]
}

func (bcm *BrokerhubCommitteeMod) handleBrokerRawMag(brokerRawMags []*message.BrokerRawMeg) {
	b := bcm.Broker
	brokerType1Mags := make([]*message.BrokerType1Meg, 0)
	//fmt.Println("Broker receive ctx ", len(brokerRawMags))
	bcm.BrokerModuleLock.Lock()
	for _, meg := range brokerRawMags {
		b.BrokerRawMegs[string(bcm.getBrokerRawMagDigest(meg))] = meg
		brokerType1Mag := &message.BrokerType1Meg{
			RawMeg:   meg,
			Hcurrent: 0,
			Broker:   meg.Broker,
		}
		brokerType1Mags = append(brokerType1Mags, brokerType1Mag)
	}
	bcm.BrokerModuleLock.Unlock()
	bcm.handleBrokerType1Mes(brokerType1Mags)
}

func (bcm *BrokerhubCommitteeMod) handleTx1ConfirmMag(mag1confirms []*message.Mag1Confirm) {
	brokerType2Mags := make([]*message.BrokerType2Meg, 0)
	b := bcm.Broker

	fmt.Println("receive confirm  brokerTx1 len ", len(mag1confirms))
	bcm.BrokerModuleLock.Lock()
	for _, mag1confirm := range mag1confirms {
		RawMeg := mag1confirm.RawMeg
		_, ok := b.BrokerRawMegs[string(bcm.getBrokerRawMagDigest(RawMeg))]
		if !ok {
			fmt.Println("raw message is not exited,tx1 confirms failure !")
			continue
		}
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag1confirm.Tx1Hash))
		brokerType2Mag := &message.BrokerType2Meg{
			Broker: RawMeg.Broker,
			RawMeg: RawMeg,
		}
		brokerType2Mags = append(brokerType2Mags, brokerType2Mag)
	}
	bcm.BrokerModuleLock.Unlock()
	bcm.handleBrokerType2Mes(brokerType2Mags)
}

func (bcm *BrokerhubCommitteeMod) handleTx2ConfirmMag(mag2confirms []*message.Mag2Confirm) {
	b := bcm.Broker
	fmt.Println("receive confirm  brokerTx2 len ", len(mag2confirms))
	num := 0
	bcm.BrokerModuleLock.Lock()
	for _, mag2confirm := range mag2confirms {
		RawMeg := mag2confirm.RawMeg
		b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)] = append(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)], string(mag2confirm.Tx2Hash))
		if len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]) == 2 {
			num++
		} else {
			fmt.Println(len(b.RawTx2BrokerTx[string(RawMeg.Tx.TxHash)]))
		}
	}
	bcm.BrokerModuleLock.Unlock()
	//fmt.Println("finish ctx with adding tx1 and tx2 to txpool,len", num)
}

func (bcm *BrokerhubCommitteeMod) Result_save() {

	// write to .csv file
	dirpath := params.DataWrite_path + "brokerRsult/"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	for brokerAddress := range bcm.Broker.BrokerBalance {
		targetPath0 := dirpath + brokerAddress + "_lockBalance.csv"
		targetPath1 := dirpath + brokerAddress + "_brokerBalance.csv"
		targetPath2 := dirpath + brokerAddress + "_Profit.csv"
		bcm.Wirte_result(targetPath0, bcm.Result_lockBalance[brokerAddress])
		bcm.Wirte_result(targetPath1, bcm.Result_brokerBalance[brokerAddress])
		bcm.Wirte_result(targetPath2, bcm.Result_Profit[brokerAddress])
	}
}
func (bcm *BrokerhubCommitteeMod) Wirte_result(targetPath string, resultStr []string) {

	f, err := os.Open(targetPath)
	if err != nil && os.IsNotExist(err) {
		file, er := os.Create(targetPath)
		if er != nil {
			panic(er)
		}
		defer file.Close()

		w := csv.NewWriter(file)
		w.Flush()
		for _, str := range resultStr {
			str_arry := strings.Split(str, ",")
			w.Write(str_arry[0 : len(str_arry)-1])
			w.Flush()
		}
	} else {
		file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_RDWR, 0666)

		if err != nil {
			log.Panic(err)
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		err = writer.Write(resultStr)
		if err != nil {
			log.Panic()
		}
		writer.Flush()
	}
	f.Close()
}
