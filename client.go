package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	EXPERIMENT_DURATION = 10 * time.Second
	VALUE_SIZE          = 100
)

type Client struct {
	udpConn *net.UDPConn
	peers   map[int]string
	peerIDs []int
	debug   bool

	pending map[uint64]chan *Message
	mu      sync.RWMutex
	nextSeq uint64

	workload int // write ratio (50=YCSB-A, 5=YCSB-B, 0=YCSB-C)
	workers  int
	numKeys  int
}

type WorkerResult struct {
	count    int
	duration time.Duration
}

func NewClient(confPath string, workload int, workers int, numKeys int, debug bool) *Client {
	peers := parseConfig(confPath)
	ids := sortedIDs(peers)

	clientAddr := parseClientAddr(confPath)
	localAddr, _ := net.ResolveUDPAddr("udp", clientAddr)
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		panic(err)
	}

	return &Client{
		udpConn:  conn,
		peers:    peers,
		peerIDs:  ids,
		debug:    debug,
		pending:  make(map[uint64]chan *Message),
		workload: workload,
		workers:  workers,
		numKeys:  numKeys,
	}
}

// coordinatorFor returns the coordinator address for a given key.
// Hermes determines the coordinator by hashing the key modulo the number of nodes.
func (c *Client) coordinatorFor(key string) *net.UDPAddr {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := int(h.Sum32()) % len(c.peerIDs)
	addr, _ := net.ResolveUDPAddr("udp", c.peers[c.peerIDs[idx]])
	return addr
}

// anyNode returns a random node address for read requests.
// In Hermes all nodes serve reads (when the key is in StateValid).
func (c *Client) anyNode() *net.UDPAddr {
	idx := rand.Intn(len(c.peerIDs))
	addr, _ := net.ResolveUDPAddr("udp", c.peers[c.peerIDs[idx]])
	return addr
}

func (c *Client) Run() {
	fmt.Printf("[Client] Listening on %s\n", c.udpConn.LocalAddr())
	fmt.Printf("[Client] Peers: %v\n", c.peers)
	fmt.Printf("[Client] Workload: %d%% writes, Workers: %d, Keys: %d\n", c.workload, c.workers, c.numKeys)

	go c.receiveLoop()

	// Wait for servers to be ready
	time.Sleep(2 * time.Second)

	c.runBenchmark()
}

func (c *Client) receiveLoop() {
	buf := make([]byte, 65535)
	for {
		n, _, err := c.udpConn.ReadFromUDP(buf)
		if err != nil {
			c.log("Failed to read UDP: %v", err)
			continue
		}

		msg, err := DecodeMessage(buf[:n])
		if err != nil {
			c.log("Failed to decode message: %v", err)
			continue
		}

		c.mu.RLock()
		ch, ok := c.pending[msg.Seq]
		c.mu.RUnlock()

		if ok {
			ch <- msg
		}
	}
}

func (c *Client) Put(key, value string) bool {
	seq := atomic.AddUint64(&c.nextSeq, 1)
	msg := &Message{
		Type:  MsgTypeWrite,
		Seq:   seq,
		Key:   key,
		Value: value,
	}

	respCh := make(chan *Message, 1)
	c.mu.Lock()
	c.pending[seq] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
	}()

	c.udpConn.WriteToUDP(msg.Encode(), c.coordinatorFor(key))

	select {
	case resp := <-respCh:
		return resp.Type == MsgTypeResponse
	case <-time.After(5 * time.Second):
		return false
	}
}

func (c *Client) Get(key string) (string, bool) {
	seq := atomic.AddUint64(&c.nextSeq, 1)
	msg := &Message{
		Type: MsgTypeRead,
		Seq:  seq,
		Key:  key,
	}

	respCh := make(chan *Message, 1)
	c.mu.Lock()
	c.pending[seq] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, seq)
		c.mu.Unlock()
	}()

	c.udpConn.WriteToUDP(msg.Encode(), c.anyNode())

	select {
	case resp := <-respCh:
		return resp.Value, resp.Type == MsgTypeResponse
	case <-time.After(5 * time.Second):
		return "", false
	}
}

func (c *Client) runBenchmark() {
	ctx, cancel := context.WithTimeout(context.Background(), EXPERIMENT_DURATION)
	defer cancel()

	resultCh := make(chan WorkerResult, c.workers)
	var wg sync.WaitGroup

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultCh <- c.worker(ctx)
		}()
	}

	wg.Wait()
	close(resultCh)

	var totalCount int
	var totalDuration time.Duration
	for res := range resultCh {
		totalCount += res.count
		totalDuration += res.duration
	}

	throughput := float64(totalCount) / EXPERIMENT_DURATION.Seconds()
	avgLatency := float64(0)
	if totalCount > 0 {
		avgLatency = float64(totalDuration.Milliseconds()) / float64(totalCount)
	}

	workloadName := "unknown"
	switch c.workload {
	case 50:
		workloadName = "ycsb-a"
	case 5:
		workloadName = "ycsb-b"
	case 0:
		workloadName = "ycsb-c"
	}

	fmt.Printf("Benchmark completed\n")
	fmt.Printf("Total ops: %d\n", totalCount)
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Avg latency: %.2f ms\n", avgLatency)
	fmt.Printf("RESULT:%s,%d,%d,%.2f,%.2f\n", workloadName, c.workers, c.numKeys, throughput, avgLatency)
}

func (c *Client) worker(ctx context.Context) WorkerResult {
	res := WorkerResult{}

	for {
		select {
		case <-ctx.Done():
			return res
		default:
		}

		key := fmt.Sprintf("k%d", rand.Intn(c.numKeys))
		value := randomValue(VALUE_SIZE)

		start := time.Now()
		var ok bool

		if rand.Intn(100) < c.workload {
			ok = c.Put(key, value)
		} else {
			_, ok = c.Get(key)
		}

		if ok {
			res.count++
			res.duration += time.Since(start)
		}
	}
}

func randomValue(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func (c *Client) log(format string, args ...interface{}) {
	if c.debug {
		msg := fmt.Sprintf(format, args...)
		fmt.Printf("[Client] %s\n", msg)
	}
}
