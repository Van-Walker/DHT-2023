package kademila

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

type Network struct {
	rpcNode  *RPCNode
	server   *rpc.Server
	listener net.Listener
	online   bool
}

func (network *Network) Init(address string, node *Node) error {
	network.server = rpc.NewServer()
	network.rpcNode = new(RPCNode)
	network.rpcNode.node = node
	network.online = true
	err := network.server.Register(network.rpcNode)
	if err != nil {
		logrus.Errorf("[Initialize] register < %v > rpc service failed, error: < %v >", node, err)
		return err
	}
	network.listener, err = net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("[Initialize] tcp listen for address < %s > failed, error: < %v >", address, err)
		return err
	}
	go func() {
		for node.network.online {
			conn, _err := network.listener.Accept()
			if _err != nil {
				return
			}
			go network.server.ServeConn(conn)
		}
	}()
	return nil
}

func GetClient(address string) (*rpc.Client, error) {
	if address == "" {
		err := fmt.Errorf("GetClient empty address")
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", address, 800*time.Millisecond)
	if err == nil {
		return rpc.NewClient(conn), err
	}
	return nil, err
}

func (node *Node) Ping(address string) bool {
	client, _ := GetClient(address)
	if client != nil {
		client.Close()
		return true
	}
	return false
}

func (network *Network) ShutDown() {
	if !network.online {
		return
	}
	network.online = false
	network.listener.Close()
}

const (
	M                 = 16
	K                 = 40
	alpha             = 3
	ValidTime         = 500 * time.Second
	RepublishTime     = 120 * time.Second
	RepublishInterval = 100 * time.Second
)

var Mod = big.NewInt(0).Exp(big.NewInt(2), big.NewInt(int64(M)), nil)

func ConsistentHash(s string) big.Int {
	var ret big.Int
	h := sha1.New()
	h.Write([]byte(s))
	ret.SetBytes(h.Sum(nil))
	ret.Mod(&ret, Mod)
	return ret
}

type Pair struct {
	Key   string
	Value string
}

type Address struct {
	IP string
	ID big.Int
}

type Data struct {
	hashMap       map[string]string
	republishTime map[string]time.Time
	validTime     map[string]time.Time
	lock          sync.RWMutex
}

type Node struct {
	address  Address
	data     Data
	online   bool
	network  *Network
	kBuckets [M]KBucket

	lock sync.RWMutex
}

func (node *Node) Init(ip string) {
	node.address.IP = ip
	node.address.ID = ConsistentHash(ip)
	node.reset()
}

func (node *Node) reset() {
	node.online = false
	node.data.hashMap = make(map[string]string)
	node.data.republishTime = make(map[string]time.Time)
	node.data.validTime = make(map[string]time.Time)
}

func (node *Node) Run() {
	node.network = new(Network)
	err := node.network.Init(node.address.IP, node)
	if err != nil {
		logrus.Errorf("[Run] < %s > initialization failed, error: < %v >", node.address.IP, err)
		return
	}
	logrus.Infof("[Run] < %s > in service.", node.address.IP)
	node.online = true
	go node.Republish()
}

func (node *Node) UpdateKBucket(address Address) {
	node.lock.Lock()
	defer node.lock.Unlock()
	if address.IP == "" || address.IP == node.address.IP {
		return
	}
	node.kBuckets[KBucketIndex(&address.ID, &node.address.ID)].Update(address)
}

func (node *Node) Join(ip string) bool {
	address := Address{ip, ConsistentHash(ip)}
	node.UpdateKBucket(address)
	client, err := GetClient(ip)
	if err == nil {
		var kClosest KClosest
		_ = client.Call("RPCNode.FindNode", &FindNodeInput{node.address.ID, node.address}, &kClosest)
		for i := 0; i < kClosest.Size; i++ {
			node.UpdateKBucket(kClosest.Element[i])
		}
		client.Close()
	}
	kClosest := node.NodeLookUp(&node.address.ID)
	for i := 0; i < kClosest.Size; i++ {
		node.UpdateKBucket(kClosest.Element[i])
		client, err = GetClient(kClosest.Element[i].IP)
		if err != nil {
			logrus.Errorf("[Join] GetClient < %s > failed, error: < %v >", kClosest.Element[i].IP, err)
		} else {
			var tmpClosest KClosest
			err = client.Call("RPCNode.FindNode", &FindNodeInput{node.address.ID, node.address}, &tmpClosest)
			if err != nil {
				logrus.Errorf("[Join] FindNode of < %s > failed, error: < %v >", node.address.IP, err)
			}
			for j := 0; j < tmpClosest.Size; j++ {
				node.UpdateKBucket(tmpClosest.Element[j])
			}
			client.Close()
		}
	}
	return true
}

func (node *Node) Republish() {
	for node.online {
		for i := 0; i < M; i++ {
			node.kBuckets[i].Check()
		}
		node.lock.Lock()
		copy := node.data.MakeCopy()
		republishList := node.data.GetRepublishList()
		node.lock.Unlock()
		for _, key := range republishList {
			node.Put(key, copy[key])
		}
		node.data.DeleteExpiredData()
		time.Sleep(RepublishInterval)
	}
}

func (node *Node) Put(key, value string) bool {
	id := ConsistentHash(key)
	kClosest := node.NodeLookUp(&id)
	kClosest.Insert(node.address)
	for i := 0; i < kClosest.Size; i++ {
		client, err := GetClient(kClosest.Element[i].IP)
		if err == nil {
			var ret string
			err = client.Call("RPCNode.AddPair", &AddPairInput{key, value, node.address}, &ret)
			if err != nil {
				logrus.Errorf("[Put] AddPair failed, error: < %v >", err)
			}
			client.Close()
		}
	}
	return true
}

func (node *Node) Get(key string) (bool, string) {
	id := ConsistentHash(key)
	findValue := node.FindValue(key, &id)
	if findValue.Value != "" {
		return true, findValue.Value
	}
	kClosest := findValue.KClosest_
	getClient := make(map[string]bool)
	outDated := true
	for outDated {
		outDated = false
		var tmpClosest KClosest
		var deleteList []Address
		for i := 0; i < kClosest.Size; i++ {
			if getClient[kClosest.Element[i].IP] {
				continue
			}
			client, err := GetClient(kClosest.Element[i].IP)
			getClient[kClosest.Element[i].IP] = true
			if err != nil {
				deleteList = append(deleteList, kClosest.Element[i])
			} else {
				var findValue_ FindValueOutput
				_ = client.Call("RPCNode.FindValue", &FindValueInput{Key: key, Addr: node.address}, &findValue_)
				if findValue_.Value != "" {
					return true, findValue_.Value
				} else {
					for j := 0; j < findValue_.KClosest_.Size; j++ {
						tmpClosest.Insert(findValue_.KClosest_.Element[j])
					}
				}
				client.Close()
			}
		}
		for _, key := range deleteList {
			kClosest.Delete(key)
		}
		for i := 0; i < tmpClosest.Size; i++ {
			outDated = outDated || kClosest.Insert(tmpClosest.Element[i])
		}
	}
	nodeLookUp := node.NodeLookUp(&id)
	for _, address := range nodeLookUp.Element {
		client, err := GetClient(address.IP)
		if address.IP == "" {
			continue
		}
		if err != nil {
			logrus.Warnf("[Get] GetClient IP: < %s > failed, error: < %v >", address.IP, err)
		}
		var findValueOutput FindValueOutput
		client.Call("RPCNode.FindValue", &FindValueInput{Key: key, Addr: node.address}, &findValueOutput)
		defer client.Close()
		if findValueOutput.Value != "" {
			return true, findValueOutput.Value
		}
	}
	return false, ""
}

func (node *Node) FindNode(id *big.Int) (kClosest KClosest) {
	node.lock.Lock()
	kClosest.From = *id
	for i := 0; i < M; i++ {
		for j := 0; j < node.kBuckets[i].size; j++ {
			if Ping(node.kBuckets[i].element[j].IP) == nil {
				kClosest.Insert(node.kBuckets[i].element[j])
			}
		}
	}
	node.lock.Unlock()
	return kClosest
}

func (node *Node) FindValue(key string, hash *big.Int) FindValueOutput {
	node.lock.RLock()
	defer node.lock.RUnlock()
	value, ok := node.data.GetValue(key)
	if ok {
		return FindValueOutput{KClosest{}, value}
	}
	kClosest := KClosest{From: *hash}
	for i := 0; i < M; i++ {
		for j := 0; j < node.kBuckets[i].size; j++ {
			if Ping(node.kBuckets[i].element[j].IP) == nil {
				kClosest.Insert(node.kBuckets[i].element[j])
			}
		}
	}
	return FindValueOutput{kClosest, ""}
}

func (node *Node) NodeLookUp(id *big.Int) (kClosest KClosest) {
	kClosest = node.FindNode(id)
	kClosest.Insert(node.address)
	outDated := true
	getClient := make(map[string]bool)
	for outDated {
		outDated = false
		var tmpClosest KClosest
		var deleteList []Address
		for i := 0; i < kClosest.Size; i++ {
			if getClient[kClosest.Element[i].IP] {
				continue
			}
			node.UpdateKBucket(kClosest.Element[i])
			client, err := GetClient(kClosest.Element[i].IP)
			getClient[kClosest.Element[i].IP] = true
			if err != nil {
				deleteList = append(deleteList, kClosest.Element[i])
			} else {
				var findnode KClosest
				err = client.Call("RPCNode.FindNode", &FindNodeInput{*id, node.address}, &findnode)
				for j := 0; j < findnode.Size; j++ {
					tmpClosest.Insert(findnode.Element[j])
				}
				client.Close()
			}
		}
		for _, key := range deleteList {
			kClosest.Delete(key)
		}
		for i := 0; i < tmpClosest.Size; i++ {
			outDated = outDated || kClosest.Insert(tmpClosest.Element[i])
		}
	}
	return
}

type RPCNode struct {
	node *Node
}

type FindNodeInput struct {
	ID   big.Int
	Addr Address
}

func (rpcNode *RPCNode) FindNode(input *FindNodeInput, kClosest *KClosest) error {
	*kClosest = rpcNode.node.FindNode(&input.ID)
	rpcNode.node.UpdateKBucket(input.Addr)
	return nil
}

type FindValueInput struct {
	Key  string
	Hash big.Int
	Addr Address
}

type FindValueOutput struct {
	KClosest_ KClosest
	Value     string
}

func (rpcNode *RPCNode) FindValue(input *FindValueInput, output *FindValueOutput) error {
	*output = rpcNode.node.FindValue(input.Key, &input.Hash)
	rpcNode.node.UpdateKBucket(input.Addr)
	return nil
}

type AddPairInput struct {
	Key   string
	Value string
	Addr  Address
}

func (rpcNode *RPCNode) AddPair(input *AddPairInput, _ *string) error {
	rpcNode.node.data.AddPair(input.Key, input.Value)
	rpcNode.node.UpdateKBucket(input.Addr)
	return nil
}

func (node *Node) Create() {}

func (node *Node) Quit() {
	node.ForceQuit()
}

func (node *Node) ForceQuit() {
	logrus.Infof("[Quit] < %s >.", node.address.IP)
	node.network.ShutDown()
	node.reset()
}

func (node *Node) Delete(key string) bool {
	node.data.DeletePair(key)
	return true
}

func distance(x *big.Int, y *big.Int) big.Int {
	var ret big.Int
	ret.Xor(x, y)
	return ret
}

func KBucketIndex(x *big.Int, y *big.Int) int {
	dist := distance(x, y)
	return dist.BitLen() - 1
}

func Ping(address string) error {
	if address == "" {
		return fmt.Errorf("[Ping] empty address.")
	}
	client, err := rpc.Dial("tcp", address)
	if err == nil {
		client.Close()
	}
	return err
}

type KBucket struct {
	lock    sync.RWMutex
	size    int
	element [K]Address
}

// modified: return
func (kBucket *KBucket) Check() {
	kBucket.lock.Lock()
	defer kBucket.lock.Unlock()
	for i := 0; i < kBucket.size; i++ {
		if Ping(kBucket.element[i].IP) != nil {
			for j := i + 1; j < kBucket.size; j++ {
				kBucket.element[j-1] = kBucket.element[j]
			}
			kBucket.size--
			return
		}
	}
}

func (kBucket *KBucket) Update(address Address) {
	if address.IP == "" {
		return
	}
	kBucket.lock.Lock()
	defer kBucket.lock.Unlock()
	pos := -1
	for i := 0; i < kBucket.size; i++ {
		if address.IP == kBucket.element[i].IP {
			pos = i
			break
		}
	}
	if pos != -1 {
		for i := pos + 1; i < kBucket.size; i++ {
			kBucket.element[i-1] = kBucket.element[i]
		}
		kBucket.element[kBucket.size-1] = address
	} else {
		if kBucket.size < K {
			kBucket.element[kBucket.size] = address
			kBucket.size++
			return
		}
		if Ping(kBucket.element[0].IP) != nil {
			for i := 1; i < kBucket.size; i++ {
				kBucket.element[i-1] = kBucket.element[i]
			}
			kBucket.element[kBucket.size-1] = address
			return
		}
		tmp := kBucket.element[0]
		for i := 1; i < kBucket.size; i++ {
			kBucket.element[i-1] = kBucket.element[i]
		}
		kBucket.element[kBucket.size-1] = tmp
		return
	}
}

type KClosest struct {
	Size    int
	Element [K]Address
	From    big.Int
}

func (kClosest *KClosest) Insert(address Address) bool {
	if Ping(address.IP) != nil {
		return false
	}
	for i := 0; i < kClosest.Size; i++ {
		if kClosest.Element[i].IP == address.IP {
			return false
		}
	}
	newDist := distance(&kClosest.From, &address.ID)
	if kClosest.Size < K {
		for i := 0; i < kClosest.Size; i++ {
			iDist := distance(&kClosest.From, &kClosest.Element[i].ID)
			if newDist.Cmp(&iDist) < 0 {
				for j := kClosest.Size; j > i; j-- {
					kClosest.Element[j] = kClosest.Element[j-1]
				}
				kClosest.Element[i] = address
				kClosest.Size++
				return true
			}
		}
		kClosest.Element[kClosest.Size] = address
		kClosest.Size++
		return true
	}
	for i := 0; i < kClosest.Size; i++ {
		iDist := distance(&kClosest.From, &kClosest.Element[i].ID)
		if newDist.Cmp(&iDist) < 0 {
			for j := kClosest.Size - 1; j > i; j-- {
				kClosest.Element[j] = kClosest.Element[j-1]
			}
			kClosest.Element[i] = address
			return true
		}
	}
	return false
}

func (kClosest *KClosest) Delete(address Address) bool {
	for i := 0; i < kClosest.Size; i++ {
		if address.IP == kClosest.Element[i].IP {
			kClosest.Size--
			for j := i; j < kClosest.Size; j++ {
				kClosest.Element[j] = kClosest.Element[j+1]
			}
			return true
		}
	}
	return false
}

func (data *Data) GetRepublishList() (republishList []string) {
	data.lock.RLock()
	for key, tim := range data.republishTime {
		if time.Now().After(tim) {
			republishList = append(republishList, key)
		}
	}
	data.lock.RUnlock()
	return republishList
}

func (data *Data) DeleteExpiredData() {
	data.lock.Lock()
	for key, tim := range data.validTime {
		if time.Now().After(tim) {
			delete(data.hashMap, key)
			delete(data.validTime, key)
			delete(data.republishTime, key)
		}
	}
	data.lock.Unlock()
}

func (data *Data) AddPair(key, value string) {
	data.lock.Lock()
	data.hashMap[key] = value
	data.validTime[key] = time.Now().Add(ValidTime)
	data.republishTime[key] = time.Now().Add(RepublishTime)
	data.lock.Unlock()
}

func (data *Data) DeletePair(key string) bool {
	data.lock.Lock()
	_, ok := data.hashMap[key]
	if ok {
		delete(data.hashMap, key)
		delete(data.validTime, key)
		delete(data.republishTime, key)
	}
	data.lock.Unlock()
	return ok
}

func (data *Data) MakeCopy() map[string]string {
	ret := make(map[string]string)
	data.lock.RLock()
	for key, value := range data.hashMap {
		ret[key] = value
	}
	data.lock.RUnlock()
	return ret
}

func (data *Data) GetValue(key string) (string, bool) {
	data.lock.RLock()
	value, ok := data.hashMap[key]
	data.lock.RUnlock()
	return value, ok
}
