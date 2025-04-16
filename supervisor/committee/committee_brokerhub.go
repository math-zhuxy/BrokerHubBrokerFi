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
	"slices"
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

	// Broker infomation in BrokerHub
	BrokerInfoListInBrokerHub map[string][]*message.BrokerInfoInBrokerhub

	// BorkerHub List
	BrokerHubAccountList []utils.Address

	// Broker加入Brokerhub的状态
	BrokerJoinBrokerHubState map[string]string

	// Broker 最近一次加入B2E的收益
	brokerEpochProfitInB2E map[string]*big.Float

	// BrokerHub 这一轮的收益
	brokerhubEpochProfit map[string]*big.Float

	isInitedBrokerHub bool
}

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

	broker_info_list_in_hub := make(map[string][]*message.BrokerInfoInBrokerhub)

	broker_info_list_in_hub["40ce7569d555dbf939e58867be78fd76142df821"] = make([]*message.BrokerInfoInBrokerhub, 0)
	broker_info_list_in_hub["a8f769b88d6d74fb2bd3912f6793f75625228baf"] = make([]*message.BrokerInfoInBrokerhub, 0)

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

		BrokerInfoListInBrokerHub: broker_info_list_in_hub,
		BrokerHubAccountList: []string{
			"40ce7569d555dbf939e58867be78fd76142df821",
			"a8f769b88d6d74fb2bd3912f6793f75625228baf",
		},
		BrokerJoinBrokerHubState: make(map[string]string),
		brokerEpochProfitInB2E:   make(map[string]*big.Float),
		brokerhubEpochProfit:     make(map[string]*big.Float),
		isInitedBrokerHub:        false,
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

		if tx.Isbrokertx2 {
			sendersid = bcm.fetchModifiedMap(tx.Recipient)
		}
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (bcm *BrokerhubCommitteeMod) calculateTotalBalance(addr string) *big.Int {
	BrokerBalance := big.NewInt(0)
	for _, balance := range bcm.Broker.BrokerBalance[addr] {
		BrokerBalance.Add(BrokerBalance, balance)
	}
	return BrokerBalance
}

func (bcm *BrokerhubCommitteeMod) init_brokerhub() {
	for _, brokerhub_id := range bcm.BrokerHubAccountList {
		bcm.Broker.BrokerAddress = append([]string{brokerhub_id}, bcm.Broker.BrokerAddress...)

		bcm.Broker.BrokerBalance[brokerhub_id] = make(map[uint64]*big.Int)
		for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
			bcm.Broker.BrokerBalance[brokerhub_id][sid] = new(big.Int).Set(params.Init_broker_Balance)
		}
		bcm.Broker.LockBalance[brokerhub_id] = make(map[uint64]*big.Int)
		for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
			bcm.Broker.LockBalance[brokerhub_id][sid] = new(big.Int).Set(big.NewInt(0))
		}

		bcm.Broker.ProfitBalance[brokerhub_id] = make(map[uint64]*big.Float)
		for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
			bcm.Broker.ProfitBalance[brokerhub_id][sid] = new(big.Float).Set(big.NewFloat(0))
		}
		bcm.brokerhubEpochProfit[brokerhub_id] = big.NewFloat(0)
	}
}

func (bcm *BrokerhubCommitteeMod) judgeBrokerhubInfo(broker_id string, brokerhub_id string) (string, bool) {
	if !slices.Contains(bcm.BrokerHubAccountList, brokerhub_id) {
		return "hub not exist", false
	}
	if _, exist := bcm.Broker.BrokerBalance[brokerhub_id]; !exist {
		return "hub not init", false
	}
	if _, exist := bcm.Broker.BrokerBalance[broker_id]; !exist {
		return "not broker", false
	}
	return "", true
}

func (bcm *BrokerhubCommitteeMod) JoiningToBrokerhub(broker_id string, brokerhub_id string) string {
	if res, ok := bcm.judgeBrokerhubInfo(broker_id, brokerhub_id); !ok {
		return res
	}

	if _, exist := bcm.BrokerJoinBrokerHubState[broker_id]; exist {
		return "already in"
	}

	// 计算账户余额
	broker_balance := bcm.calculateTotalBalance(broker_id)
	bcm.Broker.BrokerBalance[brokerhub_id][0].Add(
		bcm.Broker.BrokerBalance[brokerhub_id][0],
		broker_balance,
	)

	// 更新账户信息
	brokerinfo := new(message.BrokerInfoInBrokerhub)
	brokerinfo.BrokerAddr = broker_id
	brokerinfo.BrokerBalance = new(big.Int).Set(broker_balance)
	brokerinfo.BrokerProfit = big.NewFloat(0)
	bcm.BrokerInfoListInBrokerHub[brokerhub_id] = append(
		bcm.BrokerInfoListInBrokerHub[brokerhub_id],
		brokerinfo,
	)

	bcm.BrokerJoinBrokerHubState[broker_id] = brokerhub_id
	return "done"
}

func (bcm *BrokerhubCommitteeMod) ExitingBrokerHub(broker_id string, brokerhub_id string) string {
	if res, ok := bcm.judgeBrokerhubInfo(broker_id, brokerhub_id); !ok {
		return res
	}
	{
		hub_id, exist := bcm.BrokerJoinBrokerHubState[broker_id]
		if !exist {
			return "not in hub"
		}
		if hub_id != brokerhub_id {
			return "hub id error"
		}
	}
	bcm.BrokerInfoListInBrokerHub[brokerhub_id] = slices.DeleteFunc(
		bcm.BrokerInfoListInBrokerHub[brokerhub_id],
		func(x *message.BrokerInfoInBrokerhub) bool {
			return x.BrokerAddr == broker_id
		},
	)
	delete(bcm.BrokerJoinBrokerHubState, broker_id)
	return "done"
}

func (bcm *BrokerhubCommitteeMod) getManagementExpanseRatio() float64 {
	return 0.5
}
func (bcm *BrokerhubCommitteeMod) getBrokerHubTotalBalance(brokerhub_id string) *big.Int {
	BrokerTotalBalanceInHub := big.NewInt(0)
	for _, brokerinfo := range bcm.BrokerInfoListInBrokerHub[brokerhub_id] {
		BrokerTotalBalanceInHub.Add(BrokerTotalBalanceInHub, brokerinfo.BrokerBalance)
	}
	BrokerTotalBalanceInHub.Add(BrokerTotalBalanceInHub, bcm.calculateTotalBalance(brokerhub_id))
	return BrokerTotalBalanceInHub
}

func (bcm *BrokerhubCommitteeMod) allocateBrokerhubRevenue(addr string, ssid uint64, fee *big.Float) {
	// 如果账户不是BrokerHub，直接按照正常流程的增加余额流程
	if !slices.Contains(bcm.BrokerHubAccountList, addr) {
		bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], fee)
		// 本轮 B2E 收益增加
		if bcm.brokerEpochProfitInB2E[addr] == nil {
			bcm.brokerEpochProfitInB2E[addr] = big.NewFloat(0)
		}
		bcm.brokerEpochProfitInB2E[addr].Add(bcm.brokerEpochProfitInB2E[addr], fee)
		// bcm.sl.Slog.Printf("broker %s get renenue", addr)
		return
	}

	// BrokerHub获取的收益
	BrokerHubRevenue := new(big.Float)
	BrokerHubRevenue.Mul(fee, big.NewFloat(bcm.getManagementExpanseRatio()))
	bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], BrokerHubRevenue)
	// brokerhub收益增加
	bcm.brokerhubEpochProfit[addr].Add(bcm.brokerhubEpochProfit[addr], BrokerHubRevenue)
	// bcm.sl.Slog.Printf("brokerhub %s get renenue", addr)

	// 计算每个Broker的收益
	if len(bcm.BrokerInfoListInBrokerHub[addr]) == 0 {
		return
	}
	BrokersRevenue := new(big.Float).Sub(fee, BrokerHubRevenue)
	BrokerTotalBalanceInHub := bcm.getBrokerHubTotalBalance(addr)
	for _, brokerinfo := range bcm.BrokerInfoListInBrokerHub[addr] {
		broker_revenue := big.NewFloat(0).Mul(BrokersRevenue, new(big.Float).SetInt(brokerinfo.BrokerBalance))
		broker_revenue.Quo(broker_revenue, big.NewFloat(0).SetInt(BrokerTotalBalanceInHub))
		brokerinfo.BrokerProfit.Add(brokerinfo.BrokerProfit, broker_revenue)
	}
}

func (bcm *BrokerhubCommitteeMod) GetBrokerInfomationInHub(broker_id string) (uint64, float64, string) {
	brokerhub_id, exist := bcm.BrokerJoinBrokerHubState[broker_id]
	if !exist {
		return 0, 0, ""
	}

	for _, brokerinfo := range bcm.BrokerInfoListInBrokerHub[brokerhub_id] {
		if brokerinfo.BrokerAddr == broker_id {
			fund := brokerinfo.BrokerBalance.Uint64()
			earn, _ := brokerinfo.BrokerProfit.Float64()
			return fund, earn, brokerhub_id
		}
	}
	return 0, 0, ""
}

func (bcm *BrokerhubCommitteeMod) MsgSendingControl() {

	go func() {
		for {
			time.Sleep(time.Second)

			txs := getRandomTxs(1000)

			itx := bcm.dealTxByBroker(txs)
			//bcm.BrokerModuleLock.Unlock()
			bcm.txSending(itx)

			time.Sleep(time.Second)

			bcm.broker_behaviour_simulator()

		}
	}()

	for {

		time.Sleep(time.Millisecond * 100)

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

		bcm.allocateBrokerhubRevenue(brokeraddress, sSid, fee)

	}
	//bcm.add_result()
	bcm.BrokerBalanceLock.Unlock()
	bcm.BrokerModuleLock.Unlock()
	bcm.createConfirm(txs)
}

func (bcm *BrokerhubCommitteeMod) handleBrokerInBrokerHub() (temp_map map[string]map[uint64]*big.Int) {
	temp_map = make(map[string]map[uint64]*big.Int)
	for key, val := range bcm.Broker.BrokerBalance {
		IsInHub := false
		for broker_id := range bcm.BrokerJoinBrokerHubState {
			if broker_id == key {
				IsInHub = true
			}
		}
		if !IsInHub {
			temp_map[key] = val
		}
	}
	return temp_map
}

func (bcm *BrokerhubCommitteeMod) init_broker_revenue_in_epoch() {
	for _, broker_id := range bcm.Broker.BrokerAddress {
		if _, is_in_hub := bcm.BrokerJoinBrokerHubState[broker_id]; !is_in_hub {
			bcm.brokerEpochProfitInB2E[broker_id] = big.NewFloat(0)
		}
	}
	for _, brokerhub_id := range bcm.BrokerHubAccountList {
		bcm.brokerhubEpochProfit[brokerhub_id] = big.NewFloat(0)
	}
}

func (bcm *BrokerhubCommitteeMod) broker_behaviour_simulator() {
	bcm.sl.Slog.Printf("total broker num is: %d",
		len(bcm.Broker.BrokerAddress)-len(bcm.BrokerHubAccountList),
	)
	for key, val := range bcm.BrokerInfoListInBrokerHub {
		bcm.sl.Slog.Printf("brokerhub % s has %d brokers", key[:5], len(val))
	}
	for _, broker_id := range bcm.Broker.BrokerAddress {
		if slices.Contains(bcm.BrokerHubAccountList, broker_id) {
			continue
		}
		if bcm.brokerEpochProfitInB2E[broker_id] == nil {
			bcm.sl.Slog.Printf("broker %s is not in b2e", broker_id)
			bcm.brokerEpochProfitInB2E[broker_id] = big.NewFloat(0)
			continue
		}
		b2e_revenue := big.NewFloat(0).Set(bcm.brokerEpochProfitInB2E[broker_id])
		// b2e_revenue = b2e_revenue.Quo(b2e_revenue, big.NewFloat(0).SetInt(bcm.calculateTotalBalance(broker_id)))
		max_brokerhub_id := bcm.BrokerHubAccountList[0]
		max_hub_revenue := big.NewFloat(0)
		for _, brokerhub_id := range bcm.BrokerHubAccountList {
			hub_revenue := big.NewFloat(0).Set(bcm.brokerhubEpochProfit[brokerhub_id])
			// hub_revenue = hub_revenue.Quo(hub_revenue, big.NewFloat(0).SetInt(bcm.getBrokerHubTotalBalance(brokerhub_id)))
			EARN_ratio := big.NewFloat(1).Sub(big.NewFloat(1), big.NewFloat(0).SetFloat64(bcm.getManagementExpanseRatio()))
			hub_revenue = hub_revenue.Mul(hub_revenue, EARN_ratio)
			bcm.sl.Slog.Printf("hub %s revenue: %f", brokerhub_id[:5], hub_revenue)
			if hub_revenue.Cmp(max_hub_revenue) == 1 {
				max_brokerhub_id = brokerhub_id
				max_hub_revenue = hub_revenue
			}
		}
		broker_joined_hub_id, broker_is_in_hub := bcm.BrokerJoinBrokerHubState[broker_id]
		bcm.sl.Slog.Printf("b2e: %f", b2e_revenue)
		if b2e_revenue.Cmp(max_hub_revenue) == 1 && broker_is_in_hub {
			bcm.ExitingBrokerHub(broker_id, broker_joined_hub_id)
			bcm.sl.Slog.Printf("broker %s exit brokerhub %s", broker_id[:5], max_brokerhub_id[:5])
		} else if max_hub_revenue.Cmp(b2e_revenue) == 1 && !broker_is_in_hub {
			bcm.JoiningToBrokerhub(broker_id, max_brokerhub_id)
			bcm.sl.Slog.Printf("broker %s join brokerhub %s", broker_id[:5], max_brokerhub_id[:5])
		} else if max_hub_revenue.Cmp(b2e_revenue) == 1 && broker_is_in_hub && broker_joined_hub_id != max_brokerhub_id {
			bcm.ExitingBrokerHub(broker_id, broker_joined_hub_id)
			bcm.JoiningToBrokerhub(broker_id, max_brokerhub_id)
			bcm.sl.Slog.Printf("broker %s jump to brokerhub %s", broker_id[:5], max_brokerhub_id[:5])
		}
	}
	bcm.init_broker_revenue_in_epoch()
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
	if !bcm.isInitedBrokerHub {
		bcm.init_brokerhub()
		bcm.isInitedBrokerHub = true
	}
	itxs = make([]*core.Transaction, 0)
	brokerRawMegs := make([]*message.BrokerRawMeg, 0)
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
	now := time.Now()
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.handleBrokerInBrokerHub())
	println("b2e consume time(millsec.) ", time.Since(now).Milliseconds())
	bcm.restBrokerRawMegPool = append(bcm.restBrokerRawMegPool, restBrokerRawMeg...)

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

	now := time.Now()
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.handleBrokerInBrokerHub())
	println("b2e consume time(millsec.) ", time.Since(now).Milliseconds())
	bcm.restBrokerRawMegPool2 = append(bcm.restBrokerRawMegPool2, restBrokerRawMeg...)

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
