package chord

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	f, _ := os.Create("dht-test.log")
	logrus.SetOutput(f)
}

const hashLength = 160

func ConsistentHash(s string) *big.Int {
	h := sha1.New()
	h.Write([]byte(s))
	return (&big.Int{}).SetBytes(h.Sum(nil))
}

func Mod(x *big.Int) *big.Int {
	one := big.NewInt(1)
	mod := new(big.Int).Sub(new(big.Int).Lsh(one, hashLength), one)
	return new(big.Int).And(x, mod)
}

func HashFinger(x *big.Int, y uint) *big.Int {
	finger := new(big.Int).Add(x, new(big.Int).Lsh(big.NewInt(1), y))
	return Mod(finger)
}

const successorListSize = 10
const maintainInterval = 100 * time.Millisecond

type NodeInformation struct {
	address string
	ID      *big.Int
}

type Node struct {
	NodeInformation
	online        bool
	listener      net.Listener
	server        *rpc.Server
	predecessor   NodeInformation
	successorList [successorListSize]NodeInformation
	finger        [hashLength]NodeInformation
	curFinger     uint
	data          map[string]string
	backup        map[string]string

	onlineLock      sync.RWMutex
	predecessorLock sync.RWMutex
	successorLock   sync.RWMutex
	fingerLock      sync.RWMutex
	dataLock        sync.RWMutex
	backupLock      sync.RWMutex
}

type Pair struct {
	Key   string
	Value string
}

func (node *Node) ResetMap() {
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

func (node *Node) ResetBackup() {
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
}

func (node *Node) SetOnline(online bool) {
	node.onlineLock.Lock()
	node.online = online
	node.onlineLock.Unlock()
}

func (node *Node) Online() bool {
	node.onlineLock.RLock()
	defer node.onlineLock.RUnlock()
	return node.online
}

func (node *Node) Init(address string) {
	node.address = address
	node.ID = ConsistentHash(address)
	node.SetOnline(false)
	node.server = rpc.NewServer()
	node.server.Register(node)
	node.ResetMap()
}

func (node *Node) RunRPCServer() {
	logrus.Infof("[Serve] %s start serving\n", node.address)
	for node.Online() {
		conn, err := node.listener.Accept()
		if !node.Online() {
			logrus.Warnf("[Serve] %s offline, stop serving\n", node.address)
			break
		}
		if err != nil {
			logrus.Errorf("[Serve] %s, listener.accept error: %v\n", node.address, err)
			return
		}
		go node.server.ServeConn(conn)
	}
	logrus.Warnf("[Serve] %s offline, stop serving\n", node.address)
}

func (node *Node) StopRPCServer() {
	node.SetOnline(false)
	node.listener.Close()
}

func GetClient(address string) (*rpc.Client, error) {
	var (
		conn net.Conn
		err  error
	)
	conn, err = net.DialTimeout("tcp", address, 800*time.Millisecond)
	if err == nil {
		logrus.Infof("[GetClient] success!\n")
		return rpc.NewClient(conn), err
	}
	logrus.Errorf("[GetClient] Dial %s, error: %v\n", address, err)
	return nil, err
}

func RemoteCall(address, method string, args, reply interface{}) error {
	client, err := GetClient(address)
	if err != nil {
		return err
	}
	if client != nil {
		defer client.Close()
	}
	err = client.Call(method, args, reply)
	if err == nil {
		logrus.Infof("[RemoteCall] success!\n")
	}
	return err
}

func (node *Node) Run() {
	node.SetOnline(true)
	var err error
	node.listener, err = net.Listen("tcp", node.address)
	if err != nil {
		logrus.Errorf("[Run] %s listen error: %v\n", node.address, err)
	}
	go node.RunRPCServer()
}

func (node *Node) Create() {
	logrus.Infof("[Create] %s Created!\n", node.address)
	node.SetOnline(true)
	node.successorList[0] = NodeInformation{node.address, node.ID}
	for i := 0; i < hashLength; i++ {
		node.finger[i] = NodeInformation{node.address, node.ID}
	}
	node.Maintain()
}

func (node *Node) GetSuccessorList(_ string, ret *[successorListSize]NodeInformation) error {
	node.successorLock.RLock()
	*ret = node.successorList
	node.successorLock.RUnlock()
	return nil
}

func (node *Node) getPredecessor() NodeInformation {
	node.predecessorLock.RLock()
	defer node.predecessorLock.RUnlock()
	return node.predecessor
}

func (node *Node) GetPredecessor(_ string, address *string) error {
	*address = node.getPredecessor().address
	return nil
}

func (node *Node) SetPredecessor(predecessor string) {
	node.predecessorLock.Lock()
	node.predecessor = NodeInformation{predecessor, ConsistentHash(predecessor)}
	node.predecessorLock.Unlock()
}

func (node *Node) GetData(_ string, ret *map[string]string) error {
	node.dataLock.RLock()
	*ret = node.data
	node.dataLock.RUnlock()
	return nil
}

func (node *Node) DataToBackup(data map[string]string, _ *string) error {
	node.backupLock.Lock()
	for key, value := range data {
		node.backup[key] = value
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) BackupToData() {
	node.backupLock.RLock()
	tmp := node.backup
	node.backupLock.RUnlock()
	node.dataLock.Lock()
	for k, v := range tmp {
		node.data[k] = v
	}
	node.dataLock.Unlock()
	node.ResetBackup()
}

func (node *Node) BackupToSuccessor() {
	successor := node.FindOnlineSuc()
	RemoteCall(successor.address, "Node.DataToBackup", node.data, nil)
}

func (node *Node) CheckPredecessor() {
	predecessor := node.getPredecessor()
	if predecessor.address == "" {
		logrus.Warnf("[CheckPredecessor] predecessor of %s is empty...\n", node.address)
		return
	}
	if !node.Ping(predecessor.address) {
		logrus.Warnf("[CheckPredecessor] %s ping predecessor %s failed.\n", node.address, predecessor.address)
		node.SetPredecessor("")
		node.BackupToData()
		node.BackupToSuccessor()
	}
}

func (node *Node) checkPredecessor(_ string, _ *string) error {
	node.CheckPredecessor()
	return nil
}

func Between(mid, from, to *big.Int) bool {
	if from.Cmp(to) < 0 {
		return from.Cmp(mid) < 0 && mid.Cmp(to) < 0
	}
	return from.Cmp(mid) < 0 || mid.Cmp(to) < 0
}

func (node *Node) FindOnlineSuc() NodeInformation {
	for i := 0; i < successorListSize; i++ {
		node.successorLock.RLock()
		curSuccessor := node.successorList[i]
		node.successorLock.RUnlock()
		if curSuccessor.address == "" {
			continue
		}
		if !node.Ping(curSuccessor.address) {
			continue
		}
		logrus.Infof("[FindOnlineSuc] success!\n")
		return curSuccessor
	}
	logrus.Errorf("[FindOnlineSuc] %s failed...\n", node.address)
	return NodeInformation{}
}

func (node *Node) FindFingerBefore(ID *big.Int) (address string) {
	for i := hashLength - 1; i >= 0; i-- {
		node.fingerLock.RLock()
		finger := node.finger[i]
		node.fingerLock.RUnlock()
		if finger.address == "" {
			continue
		}
		if !node.Ping(finger.address) {
			node.fingerLock.Lock()
			node.finger[i] = NodeInformation{"", nil}
			node.fingerLock.Unlock()
			continue
		}
		if Between(finger.ID, node.ID, ID) {
			return finger.address
		}
	}
	return node.FindOnlineSuc().address
}

func (node *Node) PassData(predecessorAddress string, preData *map[string]string) error {
	preID := ConsistentHash(predecessorAddress)
	node.dataLock.Lock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for address, data := range node.data {
		hash := ConsistentHash(address)
		if hash.Cmp(node.ID) != 0 && !Between(hash, preID, node.ID) {
			node.backup[address] = data
			(*preData)[address] = data
			delete(node.data, address)
		}
	}
	node.dataLock.Unlock()
	node.backupLock.Unlock()
	successor := node.FindOnlineSuc()
	err := RemoteCall(successor.address, "Node.RemoveBackup", *preData, nil)
	if err != nil {
		logrus.Errorf("[PassData] %s pre %s error: %v\n:", node.address, predecessorAddress, err)
		return err
	}
	node.SetPredecessor(predecessorAddress)
	return nil
}

func (node *Node) RemoveBackup(remove map[string]string, _ *string) error {
	node.backupLock.Lock()
	for i := range remove {
		delete(node.backup, i)
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) FindSuccessor(id *big.Int, ret *string) error {
	if !node.Online() {
		return fmt.Errorf("[FindSuccessor] %s offline...\n", node.address)
	}
	successor := node.FindOnlineSuc()
	if successor.address == "" {
		return fmt.Errorf("[FindSuccessor] %s FindOnlineSuc failed...\n", node.address)
	}
	logrus.Infof("[FindSuccessor] %s, online successor: %s\n", node.address, successor.address)
	ID := id
	if ID.Cmp(successor.ID) == 0 || Between(ID, node.ID, successor.ID) {
		logrus.Infof("[FindSuccessor] %s, find successor: %s before %s\n", node.address, ID, successor.ID)
		*ret = successor.address
		return nil
	}
	return RemoteCall(node.FindFingerBefore(ID), "Node.FindSuccessor", ID, ret)
}

func (node *Node) FixFinger() {
	curFinger := HashFinger(node.ID, node.curFinger)
	var address string
	err := node.FindSuccessor(curFinger, &address)
	if err != nil {
		logrus.Errorf("[FixFinger] %s FindSuccessor of %v failed, error: %v\n", node.address, curFinger, err)
		return
	}
	node.fingerLock.Lock()
	if node.finger[node.curFinger].address != address {
		node.finger[node.curFinger].address = address
		node.finger[node.curFinger].ID = ConsistentHash(address)
	}
	node.fingerLock.Unlock()
	node.curFinger = (node.curFinger + 1) % hashLength
}

func (node *Node) Notify(newPredecessor string, _ *string) error {
	logrus.Infof("[Notify] %s newPredecessor %s\n", node.address, newPredecessor)
	predecessor := node.getPredecessor()
	if node.Ping(newPredecessor) && (predecessor.address == "" || Between(ConsistentHash(newPredecessor), predecessor.ID, node.ID)) {
		logrus.Infof("[Notify] %s predecessor from %s to %s\n", node.address, predecessor.address, newPredecessor)
		node.SetPredecessor(newPredecessor)
		newPreData := make(map[string]string)
		err := RemoteCall(newPredecessor, "Node.GetData", "", &newPreData)
		if err != nil {
			logrus.Errorf("[Notify] %s get data from %s, error: %v\n", node.address, newPredecessor, err)
			return err
		}
		node.backupLock.Lock()
		node.backup = newPreData
		node.backupLock.Unlock()
	}
	return nil
}

func (node *Node) Stabilize() {
	successor := node.FindOnlineSuc()
	var newSuccessor NodeInformation
	RemoteCall(successor.address, "Node.GetPredecessor", "", &newSuccessor.address)
	newSuccessor.ID = ConsistentHash(newSuccessor.address)
	if newSuccessor.address != "" && Between(newSuccessor.ID, node.ID, successor.ID) {
		successor = newSuccessor
	}
	var successorList [successorListSize]NodeInformation
	err := RemoteCall(successor.address, "Node.GetSuccessorList", "", &successorList)
	if err != nil {
		logrus.Errorf("[Stabilize] %s GetSuccessorList of %s, error: %v\n", node.address, successor.address, err)
		return
	}
	node.fingerLock.Lock()
	node.finger[0] = successor
	node.fingerLock.Unlock()
	node.successorLock.Lock()
	node.successorList[0] = successor
	for i := 1; i < successorListSize; i++ {
		node.successorList[i] = successorList[i-1]
	}
	node.successorLock.Unlock()
	err = RemoteCall(successor.address, "Node.Notify", node.address, nil)
	if err != nil {
		logrus.Errorf("[stabilize] %s notify %s, error: %v\n", node.address, successor.address, err)
	}
}

func (node *Node) RemoteStabilize(_ string, _ *string) error {
	node.Stabilize()
	return nil
}

func (node *Node) Maintain() {
	go func() {
		for node.Online() {
			node.FixFinger()
			time.Sleep(maintainInterval)
		}
	}()
	go func() {
		for node.Online() {
			node.Stabilize()
			time.Sleep(maintainInterval)
		}
	}()
	go func() {
		for node.Online() {
			node.CheckPredecessor()
			time.Sleep(maintainInterval)
		}
	}()
}

func (node *Node) Join(address string) bool {
	logrus.Infof("[Join] %s joins %s\n", node.address, address)
	var successor NodeInformation
	err := RemoteCall(address, "Node.FindSuccessor", node.ID, &successor.address)
	if err != nil {
		logrus.Errorf("[Join] %s call %s FindSuccessor error: %s\n", node.address, address, err)
		return false
	}
	successor.ID = ConsistentHash(successor.address)
	var successorList [successorListSize]NodeInformation
	RemoteCall(successor.address, "Node.GetSuccessorList", nil, &successorList)
	node.fingerLock.Lock()
	node.finger[0] = successor
	node.fingerLock.Unlock()
	node.successorLock.Lock()
	node.successorList[0] = successor
	for i := 1; i < successorListSize; i++ {
		node.successorList[i] = successorList[i-1]
	}
	node.successorLock.Unlock()
	node.dataLock.Lock()
	err = RemoteCall(successor.address, "Node.PassData", node.address, &node.data)
	node.dataLock.Unlock()
	if err != nil {
		logrus.Errorf("[Join] %s PassData to %s, error: %v\n", successor.address, node.address, err)
		return false
	}
	node.SetOnline(true)
	node.Maintain()
	return true
}

func (node *Node) Quit() {
	if !node.Online() {
		logrus.Warnf("[Quit] %s already offline...\n", node.address)
		return
	}
	logrus.Infof("[Quit] %s quit successfully.\n", node.address)
	node.StopRPCServer()
	successor := node.FindOnlineSuc()
	err := RemoteCall(successor.address, "Node.checkPredecessor", "", nil)
	if err != nil {
		logrus.Errorf("[Quit] %s calls %s CheckPredecessor, error: %v\n", node.address, successor.address, err)
	}
	predecessor := node.getPredecessor()
	err = RemoteCall(predecessor.address, "Node.RemoteStabilize", "", nil)
	if err != nil {
		logrus.Errorf("[Quit] %s calls %s Stabilize, error: %v\n", node.address, predecessor.address, err)
	}
	node.ResetMap()
}

func (node *Node) ForceQuit() {
	if !node.Online() {
		logrus.Warnf("[ForceQuit] %s already offline...\n", node.address)
		return
	}
	logrus.Infof("[ForceQuit] %s quit successfully.\n", node.address)
	node.StopRPCServer()
	node.ResetMap()
}

func (node *Node) Ping(address string) bool {
	if address == node.address {
		return true
	}
	if address == "" {
		logrus.Warnf("[Ping] %s pings %s error: empty address\n", node.address, address)
		return false
	}
	conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		return true
	}
	return false
}

func (node *Node) PutBackup(pair Pair, _ *string) error {
	node.backupLock.Lock()
	node.backup[pair.Key] = pair.Value
	node.backupLock.Unlock()
	return nil
}

func (node *Node) PutPair(pair Pair, _ *string) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	successor := node.FindOnlineSuc()
	err := RemoteCall(successor.address, "Node.PutBackup", pair, nil)
	if err != nil {
		logrus.Errorf("[PutPair] %s put %v in the backup of %s, error: %v\n", node.address, pair, successor.address, err)
		return err
	}
	return nil
}

func (node *Node) Put(key string, value string) bool {
	logrus.Infof("[Put] key: %s, value: %s\n", key, value)
	if !node.Online() {
		logrus.Errorf("[Put] %s offline.\n", node.address)
		return false
	}
	var successor string
	err := node.FindSuccessor(ConsistentHash(key), &successor)
	if err != nil {
		logrus.Errorf("[Put] %s find successor of key %v, error: %v\n", node.address, key, err)
		return false
	}
	err = RemoteCall(successor, "Node.PutPair", Pair{key, value}, nil)
	if err != nil {
		logrus.Errorf("[Put] PutPair %v to %s, error: %v\n", Pair{key, value}, successor, err)
		return false
	}
	return true
}

func (node *Node) GetValue(key string, value *string) error {
	var ok bool
	node.dataLock.RLock()
	*value, ok = node.data[key]
	node.dataLock.RUnlock()
	if !ok {
		return fmt.Errorf("key %v not found in %p", key, node)
	}
	return nil
}

func (node *Node) Get(key string) (bool, string) {
	if !node.Online() {
		logrus.Errorf("[Get] %s is offline.\n", node.address)
		return false, ""
	}
	logrus.Infof("[Get] key: %s\n", key)
	var successor, value string
	err := node.FindSuccessor(ConsistentHash(key), &successor)
	if err != nil {
		logrus.Errorf("[Get] %s find successor of key %s, error: %v\n", node.address, key, err)
		return false, ""
	}
	err = RemoteCall(successor, "Node.GetValue", key, &value)
	if err != nil {
		logrus.Errorf("[Get] %s GetValue failed, error: %v\n", successor, err)
		return false, ""
	}
	return true, value
}

func (node *Node) DeleteBackup(key string, _ *string) error {
	node.backupLock.Lock()
	_, ok := node.backup[key]
	delete(node.backup, key)
	node.backupLock.Unlock()
	if ok {
		return nil
	}
	return fmt.Errorf("deleteBackup %s in %s, not found.\n", key, node.address)
}

func (node *Node) DeletePair(key string, _ *string) error {
	node.dataLock.Lock()
	_, ok := node.data[key]
	delete(node.data, key)
	node.dataLock.Unlock()
	if !ok {
		return fmt.Errorf("delete %s in %s, not found.\n", key, node.address)
	}
	successor := node.FindOnlineSuc()
	err := RemoteCall(successor.address, "Node.DeleteBackup", key, nil)
	if err != nil {
		logrus.Errorf("[DeletePair] %s delete %s, error: %v\n", node.address, key, err)
		return err
	}
	return nil
}

func (node *Node) Delete(key string) bool {
	if !node.Online() {
		logrus.Errorf("[Delete] %s offline.\n", node.address)
		return false
	}
	var successor string
	err := node.FindSuccessor(ConsistentHash(key), &successor)
	if err != nil {
		logrus.Errorf("[Delete] %s FindSuccessor of key: %s failed, error: %v\n", node.address, key, err)
		return false
	}
	err = RemoteCall(successor, "Node.DeletePair", key, nil)
	if err != nil {
		logrus.Errorf("[Delete] Delete %s in %s failed, error: %v\n", key, successor, err)
		return false
	}
	return true
}
