package naive

import (
	//"os"
	//"fmt"
	"context"
	"errors"
	"net"
	"sync"
	"time"

	//"strings"
	"net/rpc"
	//"math/big"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// client and server for rpc/tcp
type Connection struct{}

func (*Connection) Connect(struct{}, *struct{}) error { return nil }

type Server struct {
	shutDown   atomic.Value
	server     *rpc.Server
	listener   net.Listener
	lock       sync.Mutex
	connection *Connection
	available  map[net.Conn]struct{}
}

func (s *Server) Init(service interface{}) {
	s.connection = &Connection{}
	s.server = rpc.NewServer()
	s.server.Register(s.connection)
	s.server.Register(service)
	s.available = make(map[net.Conn]struct{})
}

func (s *Server) Status(flag bool) { s.shutDown.Store(flag) }

func (s *Server) IsShutDown() bool { return s.shutDown.Load().(bool) }

func (s *Server) Serve() {
	for {
		i, err := s.listener.Accept()
		if err != nil {
			if s.IsShutDown() {
				logrus.Infof("[Serve] Shutdown\n")
			} else {
				logrus.Errorf("[Serve] Error: %v\n", err)
			}
			return
		}
		s.lock.Lock()
		s.available[i] = struct{}{}
		s.lock.Unlock()
		go func() {
			s.server.ServeConn(i)
			s.lock.Lock()
			delete(s.available, i)
			s.lock.Unlock()
		}()
	}
}

func (s *Server) Start(network, address string) error {
	s.Status(false)
	var err error
	s.listener, err = net.Listen(network, address)
	if err != nil {
		return err
	}
	go s.Serve()
	return nil
}

func (s *Server) Close() {
	s.Status(true)
	s.lock.Lock()
	s.listener.Close()
	for i := range s.available {
		i.Close()
		delete(s.available, i)
	}
	s.lock.Unlock()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.Status(true)
	err := s.listener.Close()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
outer:
	for {
		if len(s.available) == 0 {
			return err
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break outer
		case <-ticker.C:
		}
	}
	s.lock.Lock()
	for i := range s.available {
		i.Close()
		delete(s.available, i)
	}
	s.lock.Unlock()
	return err
}

var (
	clientPools sync.Map

	DialTimeout           = 800 * time.Millisecond
	SleepAfterDialTimeout = 1500 * time.Millisecond
	CheckInterval         = 10 * time.Second
)

type Client struct {
	client *rpc.Client
	time   time.Time
}

type ClientPool struct{ clientpool chan Client }

// create a new pool
func NewPool(size int) *ClientPool { return &ClientPool{make(chan Client, size)} }

// create a new client
func NewClient(address string) (Client, error) {
	logrus.Infof("[NewClient] Address: %s\n", address)
	var (
		conn net.Conn
		err  error
	)
	for i := 0; i < 3; i++ {
		conn, err = net.DialTimeout("tcp", address, DialTimeout)
		if err != nil {
			logrus.Errorf("[NewClient] Dial: %s Error: %v\n", address, err)
		} else {
			client := rpc.NewClient(conn)
			return Client{client, time.Now()}, err
		}
	}
	return Client{}, err
}

// todo
func (pool *ClientPool) Get(address string) (Client, error) {
	for {
		select { // random ?
		case c := <-pool.clientpool:
			if time.Since(c.time) > CheckInterval { // 10s ?
				err := c.client.Call("Connection.Connect", struct{}{}, nil)
				if err != nil {
					logrus.Warnf("[Get of pool] %p connection fail, closed\n", c.client)
					c.client.Close()
					continue
				} else {
					c.time = time.Now()
				}
			}
			return c, nil
		default:
			return NewClient(address)
		}
	}
}

// todo: err?
func Get(address string) (Client, error) {
	pool, ok := clientPools.Load(address)
	if ok {
		return pool.(*ClientPool).Get(address)
	} else {
		logrus.Infof("[GetClient] New pool [%s]\n", address)
		pool = NewPool(5) // size = 5, why?
		clientPools.Store(address, pool)
		return pool.(*ClientPool).Get(address)
	}
}

// todo
func (pool *ClientPool) Put(c Client) error {
	if c.client == nil {
		return errors.New("Client is nil")
	}
	if pool.clientpool == nil {
		return c.client.Close()
	} // rpc.ErrShutdown
	select {
	case pool.clientpool <- c:
		return nil
	default:
		return c.client.Close()
	}
}

// todo
func Put(address string, c Client) error {
	pool, ok := clientPools.Load(address)
	if ok {
		return pool.(*ClientPool).Put(c)
	} else {
		pool = NewPool(5)
		clientPools.Store(address, pool)
		return pool.(*ClientPool).Put(c)
	}
}

// todo: almost no
func Ping(address string) bool {
	c, err := Get(address)
	if err != nil {
		logrus.Warnf("[Ping] GetClient: %s, error: %v\n", address, err)
		return false
	}
	err = c.client.Call("Connection.Connect", struct{}{}, nil)
	if err != nil {
		logrus.Warnf("[Ping] %p connection fail\n", c.client)
		c.client.Close()
		return false
	}
	Put(address, c)
	return true
}

// todo: understand
func RemoteCall(address, method string, args, reply interface{}) error {
	c, err := Get(address)
	if err != nil {
		logrus.Warnf("[RemoteCall] GetClient from address %s failed\n", address)
		return err
	}
	err = c.client.Call(method, args, reply)
	if err != nil {
		logrus.Errorf("[RemoteCall] %s error: %v\n", method, err)
		c.client.Close()
	} else {
		Put(address, c)
	}
	return err
}
