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
	"math"
	"math/big"
	rand2 "math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type optimizer struct {
	init_tax_rate  float64
	learning_ratio float64
	max_tax_rate   float64
	min_tax_rate   float64
	tax_rate       float64
}

type simulation_param struct {
	hubHigherRate int64
	endedEpoch    int
	txsPerEpoch   int
	currentEpoch  int
}

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

	// transaction revenue list
	transaction_fee_list []*big.Int

	// transaction value list
	transaction_value_list []*big.Int

	// log balance
	Result_lockBalance   map[string][]string
	Result_brokerBalance map[string][]string
	Result_Profit        map[string][]string
	LastInvokeTime       map[string]time.Time
	LastInvokeTimeMutex  sync.Mutex

	// Broker infomation in BrokerHub
	brokerInfoListInBrokerHub map[string][]*message.BrokerInfoInBrokerhub

	// Broker history infomation in Brokerhub
	global_broker_info_list_hub []*message.BrokerInfoInBrokerhub

	// BorkerHub List
	BrokerHubAccountList []utils.Address

	// Broker加入Brokerhub的状态
	brokerJoinBrokerHubState map[string]string

	// Broker 最近一次加入B2E的收益
	brokerEpochProfitInB2E map[string]*big.Float

	// BrokerHub 这一轮的收益
	brokerhubEpochProfit map[string]*big.Float

	taxOptimizer map[string]*optimizer

	hubParams simulation_param
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

	brokerhub_account_list := []string{
		"d15e634876c991990542b8d75a3e94eaacdf840e",
		"c00eb36ed0dac15d7fb4c0ff92580be24074a14d",
	}

	broker_info_list_in_hub := make(map[string][]*message.BrokerInfoInBrokerhub)
	global_info_list_hub := make([]*message.BrokerInfoInBrokerhub, 0)

	for _, val := range brokerhub_account_list {
		broker_info_list_in_hub[val] = make([]*message.BrokerInfoInBrokerhub, 0)
	}

	var simulation_parameters simulation_param
	simulation_parameters.hubHigherRate = 10
	simulation_parameters.txsPerEpoch = 1000
	simulation_parameters.endedEpoch = 86
	simulation_parameters.currentEpoch = 0

	fee_list := make([]*big.Int, 0)
	value_list := make([]*big.Int, 0)
	// hub_higher_ratio := int64(10)
	// num_of_special_tx := 0
	for i := 0; i < simulation_parameters.txsPerEpoch; i++ {
		fee := new(big.Int).SetUint64(uint64(10 + rand2.Intn(10)))
		broker_num_bios := 4 + len(brokerhub_account_list) + params.BrokerNum
		min_balance := int(params.Init_broker_Balance.Int64()) * broker_num_bios * params.ShardNum * 2 / simulation_parameters.txsPerEpoch
		value := new(big.Int).SetUint64(uint64(min_balance + rand2.Intn(min_balance)))
		// if i%(simulation_parameters.txsPerEpoch/2) == 0 {
		// 	fee.SetUint64(7500)
		// 	value.SetUint64(500)
		// }
		// if num_of_special_tx < 3 {
		// 	fee.SetUint64(7500)
		// 	value.SetUint64(100)
		// 	num_of_special_tx++
		// }
		fee_list = append(fee_list, fee)
		value_list = append(value_list, value)
	}

	return &BrokerhubCommitteeMod{
		csvPath:                     csvFilePath,
		dataTotalNum:                dataNum,
		batchDataNum:                batchNum,
		nowDataNum:                  0,
		dataTxNums:                  0,
		brokerConfirm1Pool:          make(map[string]*message.Mag1Confirm),
		brokerConfirm2Pool:          make(map[string]*message.Mag2Confirm),
		restBrokerRawMegPool:        make([]*message.BrokerRawMeg, 0),
		restBrokerRawMegPool2:       make([]*message.BrokerRawMeg, 0),
		brokerTxPool:                make([]*core.Transaction, 0),
		Broker:                      broker,
		IpNodeTable:                 Ip_nodeTable,
		Ss:                          Ss,
		sl:                          sl,
		Result_lockBalance:          result_lockBalance,
		Result_brokerBalance:        result_brokerBalance,
		Result_Profit:               result_Profit,
		LastInvokeTime:              make(map[string]time.Time),
		transaction_fee_list:        fee_list,
		transaction_value_list:      value_list,
		brokerInfoListInBrokerHub:   broker_info_list_in_hub,
		global_broker_info_list_hub: global_info_list_hub,
		BrokerHubAccountList:        brokerhub_account_list,
		brokerJoinBrokerHubState:    make(map[string]string),
		brokerEpochProfitInB2E:      make(map[string]*big.Float),
		brokerhubEpochProfit:        make(map[string]*big.Float),
		taxOptimizer:                make(map[string]*optimizer),
		hubParams:                   simulation_parameters,
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
	BrokerHubInitialBalance := new(big.Int).Set(params.Init_broker_Balance)
	for _, brokerhub_id := range bcm.BrokerHubAccountList {
		bcm.Broker.BrokerAddress = append(bcm.Broker.BrokerAddress, brokerhub_id)

		bcm.Broker.BrokerBalance[brokerhub_id] = make(map[uint64]*big.Int)
		for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
			bcm.Broker.BrokerBalance[brokerhub_id][sid] = new(big.Int).Set(BrokerHubInitialBalance)
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

		bcm.taxOptimizer[brokerhub_id] = &optimizer{
			init_tax_rate:  0.2,
			learning_ratio: 0.08,
			max_tax_rate:   0.5,
			min_tax_rate:   0.05,
			tax_rate:       0.2,
		}
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

func (bcm *BrokerhubCommitteeMod) JoiningToBrokerhubOrstackMore(broker_id string, brokerhub_id string, token *big.Int) string {
	bcm.BrokerBalanceLock.Lock()
	defer bcm.BrokerBalanceLock.Unlock()
	if !slices.Contains(bcm.BrokerHubAccountList, brokerhub_id) {
		return "hub not exist"
	}
	if _, exist := bcm.Broker.BrokerBalance[brokerhub_id]; !exist {
		return "hub not init"
	}
	if _, exist := bcm.brokerJoinBrokerHubState[broker_id]; exist {
		for _, broker_info := range bcm.brokerInfoListInBrokerHub[brokerhub_id] {
			if broker_info.BrokerAddr == broker_id {
				broker_info.BrokerBalance.Add(broker_info.BrokerBalance, token)
				bcm.Broker.BrokerBalance[brokerhub_id][0].Add(
					bcm.Broker.BrokerBalance[brokerhub_id][0],
					token,
				)
				return "satck more done"
			}
		}
		return "none info"
	}
	if bcm.Broker.IsBroker(broker_id) {
		return "is broker"
	}

	bcm.Broker.BrokerBalance[brokerhub_id][0].Add(
		bcm.Broker.BrokerBalance[brokerhub_id][0],
		token,
	)
	brokerinfo := new(message.BrokerInfoInBrokerhub)
	brokerinfo.BrokerAddr = broker_id
	brokerinfo.BrokerBalance = new(big.Int).Set(token)
	brokerinfo.BrokerProfit = big.NewFloat(0)
	bcm.brokerInfoListInBrokerHub[brokerhub_id] = append(
		bcm.brokerInfoListInBrokerHub[brokerhub_id],
		brokerinfo,
	)
	bcm.global_broker_info_list_hub = append(
		bcm.global_broker_info_list_hub,
		brokerinfo,
	)
	bcm.brokerJoinBrokerHubState[broker_id] = brokerhub_id

	return "done"
}

func (bcm *BrokerhubCommitteeMod) WithdrawBrokerhubDirectly(broker_id string, brokerhub_id string) (string, float64) {
	bcm.BrokerBalanceLock.Lock()
	defer bcm.BrokerBalanceLock.Unlock()
	if !slices.Contains(bcm.BrokerHubAccountList, brokerhub_id) {
		return "hub not exist", 0
	}
	if _, exist := bcm.Broker.BrokerBalance[brokerhub_id]; !exist {
		return "hub not init", 0
	}
	{
		hub_id, exist := bcm.brokerJoinBrokerHubState[broker_id]
		if !exist {
			return "not in hub", 0
		}
		if hub_id != brokerhub_id {
			return "hub id error", 0
		}
	}

	for _, brokerinfo := range bcm.global_broker_info_list_hub {
		if brokerinfo.BrokerAddr == broker_id {
			profit := new(big.Int)
			brokerinfo.BrokerProfit.Int(profit)
			brokerinfo.BrokerBalance.Add(
				brokerinfo.BrokerBalance,
				profit,
			)
			break
		}
	}

	for _, brokerinfo := range bcm.brokerInfoListInBrokerHub[brokerhub_id] {
		if brokerinfo.BrokerAddr == broker_id {
			bcm.brokerInfoListInBrokerHub[brokerhub_id] = slices.DeleteFunc(
				bcm.brokerInfoListInBrokerHub[brokerhub_id],
				func(x *message.BrokerInfoInBrokerhub) bool {
					return x.BrokerAddr == broker_id
				},
			)
			delete(bcm.brokerJoinBrokerHubState, broker_id)
			profit_in_hub, _ := brokerinfo.BrokerProfit.Float64()
			return "done", profit_in_hub
		}
	}
	return "not in hub", 0
}

func (bcm *BrokerhubCommitteeMod) JoiningToBrokerhub(broker_id string, brokerhub_id string) string {
	bcm.BrokerBalanceLock.Lock()
	defer bcm.BrokerBalanceLock.Unlock()
	if res, ok := bcm.judgeBrokerhubInfo(broker_id, brokerhub_id); !ok {
		return res
	}

	if _, exist := bcm.brokerJoinBrokerHubState[broker_id]; exist {
		return "already in"
	}

	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		bcm.Broker.BrokerBalance[brokerhub_id][i].Add(
			bcm.Broker.BrokerBalance[brokerhub_id][i],
			bcm.Broker.BrokerBalance[broker_id][i],
		)
	}

	// 更新账户信息
	brokerinfo := new(message.BrokerInfoInBrokerhub)
	brokerinfo.BrokerAddr = broker_id
	brokerinfo.BrokerBalance = new(big.Int).Set(bcm.calculateTotalBalance(broker_id))
	brokerinfo.BrokerProfit = big.NewFloat(0)
	bcm.brokerInfoListInBrokerHub[brokerhub_id] = append(
		bcm.brokerInfoListInBrokerHub[brokerhub_id],
		brokerinfo,
	)

	bcm.brokerJoinBrokerHubState[broker_id] = brokerhub_id
	return "done"
}

func (bcm *BrokerhubCommitteeMod) ExitingBrokerHub(broker_id string, brokerhub_id string) string {
	bcm.BrokerBalanceLock.Lock()
	defer bcm.BrokerBalanceLock.Unlock()
	if res, ok := bcm.judgeBrokerhubInfo(broker_id, brokerhub_id); !ok {
		return res
	}
	{
		hub_id, exist := bcm.brokerJoinBrokerHubState[broker_id]
		if !exist {
			return "not in hub"
		}
		if hub_id != brokerhub_id {
			return "hub id error"
		}
	}
	for _, brokerinfo := range bcm.brokerInfoListInBrokerHub[brokerhub_id] {
		if brokerinfo.BrokerAddr == broker_id {
			remained_balance := new(big.Int).Set(brokerinfo.BrokerBalance)
			profit_in_hub := new(big.Float).Set(brokerinfo.BrokerProfit)
			if bcm.calculateTotalBalance(brokerhub_id).Cmp(remained_balance) == -1 {
				return "fund lock"
			}
			for i := uint64(0); i < uint64(params.ShardNum); i++ {
				if bcm.Broker.BrokerBalance[brokerhub_id][i].Cmp(remained_balance) == 1 {
					bcm.Broker.BrokerBalance[brokerhub_id][i].Sub(
						bcm.Broker.BrokerBalance[brokerhub_id][i],
						remained_balance,
					)
					remained_balance = new(big.Int).SetInt64(0)
					break
				} else {
					remained_balance.Sub(remained_balance, bcm.Broker.BrokerBalance[brokerhub_id][i])
					bcm.Broker.BrokerBalance[brokerhub_id][i] = new(big.Int).SetInt64(0)
				}
			}
			if remained_balance.Cmp(new(big.Int).SetInt64(0)) == 1 {
				log.Panic()
			}
			for _, val := range bcm.Broker.ProfitBalance[broker_id] {
				val.Add(
					val,
					new(big.Float).Quo(profit_in_hub, new(big.Float).SetFloat64(float64(params.ShardNum))),
				)
			}
			break
		}
	}
	bcm.brokerInfoListInBrokerHub[brokerhub_id] = slices.DeleteFunc(
		bcm.brokerInfoListInBrokerHub[brokerhub_id],
		func(x *message.BrokerInfoInBrokerhub) bool {
			return x.BrokerAddr == broker_id
		},
	)

	delete(bcm.brokerJoinBrokerHubState, broker_id)
	return "done"
}

func (bcm *BrokerhubCommitteeMod) calManagementExpanseRatio() {
	for _, brokerhub_id := range bcm.BrokerHubAccountList {
		if bcm.hubParams.currentEpoch > bcm.hubParams.endedEpoch {
			continue
		}
		result := bcm.taxOptimizer[brokerhub_id].tax_rate
		learning_ratio := bcm.taxOptimizer[brokerhub_id].learning_ratio
		my_hub_length := len(bcm.brokerInfoListInBrokerHub[brokerhub_id])
		comp_hub_length := 0
		for key, val := range bcm.brokerInfoListInBrokerHub {
			if key != brokerhub_id {
				comp_hub_length = len(val)
				break
			}
		}
		if my_hub_length > comp_hub_length {
			if comp_hub_length == 0 {
				result += learning_ratio * 2
			} else {
				result += learning_ratio * (0.99 + rand2.Float64()*0.2)
			}
		} else {
			result -= learning_ratio
			if my_hub_length == 0 {
				result -= learning_ratio * 1.5
			} else {
				result -= learning_ratio * (0.99 + rand2.Float64()*0.2)
			}
		}
		if result > bcm.taxOptimizer[brokerhub_id].max_tax_rate {
			result = bcm.taxOptimizer[brokerhub_id].max_tax_rate
		}
		if result < bcm.taxOptimizer[brokerhub_id].min_tax_rate {
			result = bcm.taxOptimizer[brokerhub_id].min_tax_rate
		}
		bcm.taxOptimizer[brokerhub_id].tax_rate = result
	}
}

func (bcm *BrokerhubCommitteeMod) allocateBrokerhubRevenue(addr string, ssid uint64, fee *big.Float) {
	// if slices.Contains(bcm.BrokerHubAccountList, addr) {
	// 	brokerhub_bios := make(map[string]int)
	// 	total_bios := 0
	// 	for _, hub_id := range bcm.BrokerHubAccountList {
	// 		brokerhub_bios[hub_id] = 1 + len(bcm.brokerInfoListInBrokerHub)
	// 		total_bios += 1 + len(bcm.brokerInfoListInBrokerHub)
	// 	}
	// 	bios := float64(brokerhub_bios[addr]) / float64(total_bios)
	// 	earn := new(big.Float).Mul(fee, new(big.Float).SetFloat64(bios))
	// 	bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], earn)
	// 	bcm.brokerhubEpochProfit[addr].Add(bcm.brokerhubEpochProfit[addr], earn)
	// 	return
	// }
	// bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], fee)

	// 如果账户不是BrokerHub，直接按照正常流程的增加余额流程
	if !slices.Contains(bcm.BrokerHubAccountList, addr) {
		bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], fee)
		// 本轮 B2E 收益增加
		if bcm.brokerEpochProfitInB2E[addr] == nil {
			bcm.brokerEpochProfitInB2E[addr] = big.NewFloat(0)
		}
		// bcm.brokerEpochProfitInB2E[addr].Add(bcm.brokerEpochProfitInB2E[addr], fee)
		return
	}

	// BrokerHub获取的收益
	bcm.Broker.ProfitBalance[addr][ssid].Add(bcm.Broker.ProfitBalance[addr][ssid], fee)
	// brokerhub本轮epoch收益增加
	// bcm.brokerhubEpochProfit[addr].Add(bcm.brokerhubEpochProfit[addr], fee)

	// 计算每个Broker的收益
	if len(bcm.brokerInfoListInBrokerHub[addr]) == 0 {
		return
	}
	BrokersRevenue := new(big.Float).Set(fee).Mul(fee, new(big.Float).SetFloat64(1-bcm.taxOptimizer[addr].tax_rate))
	for _, brokerinfo := range bcm.brokerInfoListInBrokerHub[addr] {
		broker_revenue := new(big.Float).Mul(BrokersRevenue, new(big.Float).SetInt(brokerinfo.BrokerBalance))
		broker_revenue.Quo(broker_revenue, new(big.Float).SetInt(bcm.calculateTotalBalance(addr)))
		brokerinfo.BrokerProfit.Add(brokerinfo.BrokerProfit, broker_revenue)
	}
}

func (bcm *BrokerhubCommitteeMod) GetBrokerInfomationInHub(broker_id string) (uint64, float64, string) {
	brokerhub_id, exist := bcm.brokerJoinBrokerHubState[broker_id]
	if !exist {
		for _, broker_info := range bcm.global_broker_info_list_hub {
			if broker_info.BrokerAddr == broker_id {
				fund := broker_info.BrokerBalance.Uint64()
				earn, _ := broker_info.BrokerProfit.Float64()
				return fund, earn, "exit"
			}
		}
		return 0, 0, ""
	}

	for _, brokerinfo := range bcm.brokerInfoListInBrokerHub[brokerhub_id] {
		if brokerinfo.BrokerAddr == broker_id {
			fund := brokerinfo.BrokerBalance.Uint64()
			earn, _ := brokerinfo.BrokerProfit.Float64()
			return fund, earn, brokerhub_id
		}
	}
	return 0, 0, ""
}

func (bcm *BrokerhubCommitteeMod) generateRandomTxs() []*core.Transaction {
	if len(bcm.transaction_fee_list) != len(bcm.transaction_value_list) {
		log.Panic()
	}
	size := len(bcm.transaction_fee_list)
	if len(AddressSet) == 0 {
		mu.Lock()
		if len(AddressSet) == 0 {
			AddressSet = make([]string, 20000)
			for i := 0; i < 20000; i++ {
				randomString, err := generateRandomHexString(40)
				if err != nil {
					fmt.Println("Error generating random string:", err)
				}
				AddressSet[i] = randomString
			}
		}
		mu.Unlock()
	}
	txs := make([]*core.Transaction, 0)
	for i := 0; i < size; i++ {
		sender := AddressSet[rand2.Intn(20000)]
		recever := AddressSet[rand2.Intn(20000)]

		sid := utils.Addr2Shard(sender)
		UUID := strconv.Itoa(sid) + "-" + uuid.New().String()

		tx := core.NewTransaction(
			sender,
			recever,
			bcm.transaction_value_list[i],
			uint64(123),
			bcm.transaction_fee_list[i],
		)
		tx.UUID = UUID
		txs = append(txs, tx)
	}
	return txs
}

func (bcm *BrokerhubCommitteeMod) MsgSendingControl() {
	bcm.init_brokerhub()
	// bcm.writeDataToCsv(true)

	go func() {
		for {

			bcm.hubParams.currentEpoch++
			bcm.sl.Slog.Printf("epoch: %d", bcm.hubParams.currentEpoch)

			txs := bcm.generateRandomTxs()

			itx := bcm.dealTxByBroker(txs)

			bcm.txSending(itx)

			time.Sleep(time.Second * 2)

			// bcm.broker_behaviour_simulator(true)

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

	// bcm.sl.Slog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
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

func (bcm *BrokerhubCommitteeMod) calculateBrokerhubRank(brokerhub_id string, mer float64) int {
	broker_num := len(bcm.brokerInfoListInBrokerHub[brokerhub_id])
	if broker_num == 20 {
		return 1
	}
	if broker_num == 0 && math.Abs(mer-bcm.taxOptimizer[brokerhub_id].min_tax_rate) < 0.01 {
		return 2
	}
	if broker_num > 10 {
		return 1
	}
	if broker_num <= 10 && broker_num > 6 {
		return 8 - broker_num/2
	}
	if broker_num <= 6 {
		return 13 - broker_num
	}
	return 2
}

func (bcm *BrokerhubCommitteeMod) writeDataToCsv(is_first bool) {
	for index, hub_id := range bcm.BrokerHubAccountList {
		file, err := os.OpenFile("./hubres/hub"+strconv.Itoa(index)+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Panic()
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		if is_first {
			err = writer.Write([]string{"epoch", "revenue", "broker_num", "mer", "fund", "Rank"})
		} else {
			revenue, _ := bcm.brokerhubEpochProfit[hub_id].Float64()
			err = writer.Write([]string{
				strconv.Itoa(bcm.hubParams.currentEpoch),
				strconv.FormatFloat(revenue, 'f', 6, 64),
				strconv.Itoa(len(bcm.brokerInfoListInBrokerHub[hub_id])),
				strconv.FormatFloat(bcm.taxOptimizer[hub_id].tax_rate, 'f', 6, 64),
				strconv.FormatUint(bcm.calculateTotalBalance(hub_id).Uint64(), 10),
				strconv.Itoa(bcm.calculateBrokerhubRank(hub_id, bcm.taxOptimizer[hub_id].tax_rate)),
			})
		}
		if err != nil {
			log.Panic()
		}
		writer.Flush()
	}
}

func (bcm *BrokerhubCommitteeMod) handleBrokerB2EBalance() (temp_map map[string]map[uint64]*big.Int) {
	temp_map = make(map[string]map[uint64]*big.Int)
	for key, val := range bcm.Broker.BrokerBalance {
		if _, exist := bcm.brokerJoinBrokerHubState[key]; !exist {
			temp_map[key] = val
		}
	}
	for _, val := range bcm.BrokerHubAccountList {
		temp_map[val] = bcm.Broker.BrokerBalance[val]
	}
	return temp_map
}

func (bcm *BrokerhubCommitteeMod) init_broker_revenue_in_epoch() {
	for _, broker_id := range bcm.Broker.BrokerAddress {
		if slices.Contains(bcm.BrokerHubAccountList, broker_id) {
			continue
		}
		if _, is_in_hub := bcm.brokerJoinBrokerHubState[broker_id]; !is_in_hub {
			bcm.brokerEpochProfitInB2E[broker_id] = big.NewFloat(0)
		}
	}
	for _, brokerhub_id := range bcm.BrokerHubAccountList {
		bcm.brokerhubEpochProfit[brokerhub_id] = big.NewFloat(0)
	}
}

func (bcm *BrokerhubCommitteeMod) broker_behaviour_simulator(should_simulate bool) {
	bcm.BrokerBalanceLock.Lock()
	defer bcm.BrokerBalanceLock.Unlock()
	if !should_simulate {
		bcm.writeDataToCsv(false)
		bcm.init_broker_revenue_in_epoch()
		return
	}
	bcm.writeDataToCsv(false)
	for key, val := range bcm.brokerInfoListInBrokerHub {
		bcm.sl.Slog.Printf("hub %s has % d brokers", key[:5], len(val))
	}
	bcm.calManagementExpanseRatio()
	broker_decision_map := make(map[string]string)
	decided_join_hub_length := make(map[string]int)
	for _, hub_id := range bcm.BrokerHubAccountList {
		decided_join_hub_length[hub_id] = 0
	}
	should_print := true
	for _, broker_id := range bcm.Broker.BrokerAddress {
		if slices.Contains(bcm.BrokerHubAccountList, broker_id) {
			continue
		}
		if bcm.brokerEpochProfitInB2E[broker_id] == nil {
			bcm.brokerEpochProfitInB2E[broker_id] = big.NewFloat(0)
			continue
		}
		b2e_revenue := big.NewFloat(0).Set(bcm.brokerEpochProfitInB2E[broker_id])
		max_brokerhub_id := bcm.BrokerHubAccountList[0]
		max_hub_revenue := big.NewFloat(0)
		for _, brokerhub_id := range bcm.BrokerHubAccountList {
			hub_revenue := big.NewFloat(0).Set(bcm.brokerhubEpochProfit[brokerhub_id])
			earn_ratio := (1 - bcm.taxOptimizer[brokerhub_id].tax_rate)
			if bcm.hubParams.currentEpoch < bcm.hubParams.endedEpoch {
				length := (len(bcm.brokerInfoListInBrokerHub[brokerhub_id]) + decided_join_hub_length[brokerhub_id]) / 3
				if length > 0 {
					earn_ratio = earn_ratio / float64(length)
				}
				hub_revenue.Mul(hub_revenue, new(big.Float).SetFloat64(earn_ratio))
			}
			if should_print {
				bcm.sl.Slog.Printf("hub %s revenue is: %f, earn ratio is: %f", brokerhub_id[:5], hub_revenue, earn_ratio)
			}
			if hub_revenue.Cmp(max_hub_revenue) == 1 {
				max_brokerhub_id = brokerhub_id
				max_hub_revenue = hub_revenue
			}
		}
		if b2e_revenue.Cmp(max_hub_revenue) == 1 {
			broker_decision_map[broker_id] = "b2e"
		} else {
			broker_decision_map[broker_id] = max_brokerhub_id
			decided_join_hub_length[max_brokerhub_id]++
		}
		should_print = false
	}
	for broker_id, decision_hub_id := range broker_decision_map {
		broker_joined_hub_id, broker_is_in_hub := bcm.brokerJoinBrokerHubState[broker_id]
		if decision_hub_id == "b2e" && broker_is_in_hub {
			res := bcm.ExitingBrokerHub(broker_id, broker_joined_hub_id)
			if res != "done" && res != "fund lock" {
				log.Panic()
			}
			if res == "done" {
				bcm.sl.Slog.Printf("broker %s exit brokerhub %s", broker_id[:5], broker_joined_hub_id[:5])
			}
		} else if decision_hub_id != "b2e" && !broker_is_in_hub {
			if bcm.JoiningToBrokerhub(broker_id, decision_hub_id) != "done" {
				log.Panic()
			}
			bcm.sl.Slog.Printf("broker %s join brokerhub %s", broker_id[:5], decision_hub_id[:5])
		} else if decision_hub_id != "b2e" && broker_is_in_hub && broker_joined_hub_id != decision_hub_id {
			res := bcm.ExitingBrokerHub(broker_id, broker_joined_hub_id)
			if res != "done" && res != "fund lock" {
				log.Panic()
			}
			if res == "done" {
				if bcm.JoiningToBrokerhub(broker_id, decision_hub_id) != "done" {
					log.Panic()
				}
				bcm.sl.Slog.Printf("broker %s jump to brokerhub %s", broker_id[:5], broker_joined_hub_id[:5])
			}
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
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.handleBrokerB2EBalance())
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
	alloctedBrokerRawMegs, restBrokerRawMeg := Broker2Earn.B2E(brokerRawMegs, bcm.handleBrokerB2EBalance())
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
