// Definition of transaction

package core

import (
	"blockEmulator/utils"
	"blockEmulator/vm/state"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"log"
	"math/big"
	"time"
)

type Transaction struct {
	Sender    utils.Address
	Recipient utils.Address
	Nonce     uint64
	Signature []byte // not implemented now.
	Value     *big.Int
	Fee       *big.Int
	TxHash    []byte

	Time time.Time // the time adding in pool

	// used in transaction relaying
	Relayed bool
	// used in broker, if the tx is not a broker1 or broker2 tx, these values should be empty.
	HasBroker           bool
	SenderIsBroker      bool
	IsAllocatedSender   bool
	IsAllocatedRecipent bool
	OriginalSender      utils.Address
	FinalRecipient      utils.Address
	RawTxHash           []byte
	Isbrokertx1         bool
	Isbrokertx2         bool

	//Isimportant bool

	Gas      uint64
	GasPrice *big.Int
	To       common.Address
	From     common.Address
	Data     []byte

	IsContract       bool
	IsBrokerContract bool

	Log []state.Wrapper
	UUID string

}

func (tx *Transaction) PrintTx() string {
	vals := []interface{}{
		tx.Sender[:],
		tx.Recipient[:],
		tx.Value,
		string(tx.TxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// Encode transaction for storing
func (tx *Transaction) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Decode transaction
func DecodeTx(to_decode []byte) *Transaction {
	var tx Transaction

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

// new a transaction
func NewTransaction(sender, recipient string, value *big.Int, nonce uint64, fee *big.Int) *Transaction {
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Fee:       fee,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.IsAllocatedSender = false
	tx.IsAllocatedRecipent = false
	return tx
}

func NewTransactionContractBroker(sender, recipient string, value *big.Int, nonce uint64, bytes []byte) *Transaction {
	s1, _ := hex.DecodeString(sender)
	s2, _ := hex.DecodeString(recipient)
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Fee:       big.NewInt(0),
		From:      common.Address(s1),
		Time:      time.Now(),
	}

	var s3 [20]byte
	if recipient == "" {
		s3 = stringToFixedByteArray("")
		tx.To = s3
	} else {
		tx.To = common.Address(s2)
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.IsAllocatedSender = false
	tx.IsAllocatedRecipent = false
	tx.Data = bytes
	tx.IsContract = true
	tx.IsBrokerContract = true
	return tx
}

func NewTransactionContract(sender, recipient string, value *big.Int, nonce uint64, bytes []byte) *Transaction {
	s1, _ := hex.DecodeString(sender)
	s2, _ := hex.DecodeString(recipient)
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Fee:       big.NewInt(0),
		From:      common.Address(s1),
		Time:      time.Now(),
	}

	var s3 [20]byte
	if recipient == "" {
		s3 = stringToFixedByteArray("")
		tx.To = s3
	} else {
		tx.To = common.Address(s2)
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	tx.IsAllocatedSender = false
	tx.IsAllocatedRecipent = false
	tx.Data = bytes
	tx.IsContract = true
	return tx
}

func stringToFixedByteArray(s string) [20]byte {
	var b [20]byte
	copy(b[:], []byte(s)) // 将s的字节复制到b的前len(s)个位置
	// 如果需要，可以在这里手动填充剩余的字节
	// 例如，填充为0
	for i := len(s); i < 20; i++ {
		b[i] = 0
	}
	return b
}
