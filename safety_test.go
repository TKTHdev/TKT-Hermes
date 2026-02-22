package main

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- test infrastructure ----

// udpListen returns a new UDP socket bound to a free loopback port.
// The socket is closed automatically when the test ends.
func udpListen(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("udpListen: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// startNode creates a HermesNode backed by the given pre-bound connection and
// starts its receive loop. Using a pre-bound conn avoids the port-reuse race
// that would occur if we let listen() open its own socket.
func startNode(t *testing.T, id int, peers map[int]string, conn *net.UDPConn) *HermesNode {
	t.Helper()
	h := &HermesNode{
		me:           id,
		peers:        peers,
		store:        make(map[string]string),
		kstate:       make(map[string]KeyState),
		kseq:         make(map[string]uint64),
		pendingWrite: make(map[string]*writeRecord),
	}
	h.readCond = sync.NewCond(&h.mu)
	h.udpConn = conn
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return // conn closed at end of test
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			go h.handleMessage(data, from)
		}
	}()
	return h
}

// testClient is a minimal UDP peer used in tests to send requests and wait for
// responses.
type testClient struct {
	conn    *net.UDPConn
	mu      sync.Mutex
	pending map[uint64]chan *Message
	nextSeq uint64
}

func newTestClient(t *testing.T) *testClient {
	t.Helper()
	tc := &testClient{
		conn:    udpListen(t),
		pending: make(map[uint64]chan *Message),
	}
	go tc.recvLoop()
	return tc
}

func (tc *testClient) recvLoop() {
	buf := make([]byte, 65535)
	for {
		n, _, err := tc.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		msg, err := DecodeMessage(buf[:n])
		if err != nil {
			continue
		}
		tc.mu.Lock()
		ch, ok := tc.pending[msg.Seq]
		tc.mu.Unlock()
		if ok {
			ch <- msg
		}
	}
}

// send sends msg to addr (assigning a fresh Seq) and waits up to timeout for
// a matching response. Returns nil on timeout.
func (tc *testClient) send(addr *net.UDPAddr, msg *Message, timeout time.Duration) *Message {
	seq := atomic.AddUint64(&tc.nextSeq, 1)
	msg.Seq = seq

	ch := make(chan *Message, 1)
	tc.mu.Lock()
	tc.pending[seq] = ch
	tc.mu.Unlock()
	defer func() {
		tc.mu.Lock()
		delete(tc.pending, seq)
		tc.mu.Unlock()
	}()

	tc.conn.WriteToUDP(msg.Encode(), addr)

	select {
	case resp := <-ch:
		return resp
	case <-time.After(timeout):
		return nil
	}
}

// sendRaw sends msg to addr without waiting for a response (fire-and-forget).
func (tc *testClient) sendRaw(addr *net.UDPAddr, msg *Message) {
	tc.conn.WriteToUDP(msg.Encode(), addr)
}

func mustResolve(t *testing.T, addr string) *net.UDPAddr {
	t.Helper()
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatalf("resolve %q: %v", addr, err)
	}
	return a
}

// ---- safety tests ----

// TestReadBlockedWhileStateInvalid verifies the core Hermes safety invariant:
// a node that has received an INV (StateInvalid) must never serve a read with
// the stale value — it must block until the coordinator delivers VAL.
func TestReadBlockedWhileStateInvalid(t *testing.T) {
	t.Parallel()

	conn0, conn1 := udpListen(t), udpListen(t)
	peers := map[int]string{
		0: conn0.LocalAddr().String(),
		1: conn1.LocalAddr().String(),
	}
	startNode(t, 0, peers, conn0)
	node1 := startNode(t, 1, peers, conn1)

	// Place node1 directly into StateInvalid for key "x", as if it had
	// received an INV from the coordinator.
	node1.mu.Lock()
	node1.store["x"] = "stale"
	node1.kstate["x"] = StateInvalid
	node1.mu.Unlock()

	tc := newTestClient(t)
	node1Addr := mustResolve(t, peers[1])

	// Issue a read on node1 from a background goroutine.
	readDone := make(chan *Message, 1)
	go func() {
		resp := tc.send(node1Addr, &Message{Type: MsgTypeRead, Key: "x"}, 3*time.Second)
		readDone <- resp
	}()

	// Give the read goroutine time to send the request and enter the wait loop.
	time.Sleep(50 * time.Millisecond)

	// The read must NOT return while the key is still invalid.
	select {
	case resp := <-readDone:
		t.Fatalf("safety violation: read returned %q while key was StateInvalid (stale read)", resp.Value)
	case <-time.After(150 * time.Millisecond):
		// correct: read is stalled
	}

	// Deliver VAL — simulates the coordinator committing the write.
	tc.sendRaw(node1Addr, &Message{Type: MsgTypeVAL, Key: "x", Value: "fresh", Seq: 999})

	// The read must now unblock and return the committed value.
	select {
	case resp := <-readDone:
		if resp == nil {
			t.Fatal("read timed out after VAL was delivered")
		}
		if resp.Value != "fresh" {
			t.Fatalf("safety violation: returned stale %q instead of committed %q", resp.Value, "fresh")
		}
	case <-time.After(time.Second):
		t.Fatal("read did not unblock within 1 s of VAL delivery")
	}
}

// TestReadBlockedWhileStateTrans verifies that the coordinator itself also
// stalls reads on a key that is mid-write (StateTrans), preventing any client
// from observing an intermediate state.
func TestReadBlockedWhileStateTrans(t *testing.T) {
	t.Parallel()

	conn := udpListen(t)
	peers := map[int]string{0: conn.LocalAddr().String()}
	node := startNode(t, 0, peers, conn)

	// Simulate the coordinator having started a write (StateTrans).
	node.mu.Lock()
	node.store["y"] = "stale"
	node.kstate["y"] = StateTrans
	node.mu.Unlock()

	tc := newTestClient(t)
	nodeAddr := mustResolve(t, peers[0])

	readDone := make(chan *Message, 1)
	go func() {
		resp := tc.send(nodeAddr, &Message{Type: MsgTypeRead, Key: "y"}, 3*time.Second)
		readDone <- resp
	}()

	time.Sleep(50 * time.Millisecond)

	select {
	case resp := <-readDone:
		t.Fatalf("safety violation: read returned %q while key was StateTrans", resp.Value)
	case <-time.After(150 * time.Millisecond):
		// correct: read is stalled
	}

	// Simulate the write completing on this node (VAL applied).
	node.mu.Lock()
	node.store["y"] = "committed"
	node.kstate["y"] = StateValid
	node.mu.Unlock()
	node.readCond.Broadcast()

	select {
	case resp := <-readDone:
		if resp == nil {
			t.Fatal("read timed out after state became Valid")
		}
		if resp.Value != "committed" {
			t.Fatalf("safety violation: returned %q, want %q", resp.Value, "committed")
		}
	case <-time.After(time.Second):
		t.Fatal("read did not unblock after write completed")
	}
}

// TestWriteVisibleOnAllNodesAfterAck verifies that once a write is
// acknowledged to the client, every node in the cluster returns the committed
// value — not an older one.
func TestWriteVisibleOnAllNodesAfterAck(t *testing.T) {
	t.Parallel()

	conn0, conn1 := udpListen(t), udpListen(t)
	peers := map[int]string{
		0: conn0.LocalAddr().String(),
		1: conn1.LocalAddr().String(),
	}
	startNode(t, 0, peers, conn0)
	startNode(t, 1, peers, conn1)

	tc := newTestClient(t)
	node0Addr := mustResolve(t, peers[0])
	node1Addr := mustResolve(t, peers[1])

	// Write "z"="committed" via node0 (coordinator).
	resp := tc.send(node0Addr, &Message{Type: MsgTypeWrite, Key: "z", Value: "committed"}, 2*time.Second)
	if resp == nil {
		t.Fatal("write timed out")
	}

	// After the write ACK, both nodes must return the committed value.
	// (A read on node1 may briefly stall if VAL is still in flight, but it
	// must never return the old value.)
	for _, addr := range []*net.UDPAddr{node0Addr, node1Addr} {
		r := tc.send(addr, &Message{Type: MsgTypeRead, Key: "z"}, 2*time.Second)
		if r == nil {
			t.Fatalf("read from %s timed out after write ack", addr)
		}
		if r.Value != "committed" {
			t.Fatalf("safety violation: %s returned %q after write ack, want %q",
				addr, r.Value, "committed")
		}
	}
}

// TestMonotonicWritesAcrossNodes issues a sequence of writes and after each
// ACK confirms that every node in a 3-node cluster returns the expected value.
// This catches any scenario where a node could serve an older version of a key
// after the write that superseded it has already been acknowledged.
func TestMonotonicWritesAcrossNodes(t *testing.T) {
	t.Parallel()

	conn0, conn1, conn2 := udpListen(t), udpListen(t), udpListen(t)
	peers := map[int]string{
		0: conn0.LocalAddr().String(),
		1: conn1.LocalAddr().String(),
		2: conn2.LocalAddr().String(),
	}
	startNode(t, 0, peers, conn0)
	startNode(t, 1, peers, conn1)
	startNode(t, 2, peers, conn2)

	tc := newTestClient(t)
	node0Addr := mustResolve(t, peers[0])
	allAddrs := []*net.UDPAddr{
		mustResolve(t, peers[0]),
		mustResolve(t, peers[1]),
		mustResolve(t, peers[2]),
	}

	for i, val := range []string{"alpha", "beta", "gamma"} {
		resp := tc.send(node0Addr, &Message{Type: MsgTypeWrite, Key: "w", Value: val}, 2*time.Second)
		if resp == nil {
			t.Fatalf("write #%d (%q) timed out", i, val)
		}

		// After ACK, all nodes must reflect val — not an earlier version.
		for j, addr := range allAddrs {
			r := tc.send(addr, &Message{Type: MsgTypeRead, Key: "w"}, 2*time.Second)
			if r == nil {
				t.Fatalf("write #%d: read from node%d timed out", i, j)
			}
			if r.Value != val {
				t.Fatalf("safety violation: write #%d acked %q but node%d returned %q",
					i, val, j, r.Value)
			}
		}
	}
}

// TestConcurrentWritesDifferentKeys fires parallel writes to distinct keys and
// verifies that each committed value is visible on every node. This tests that
// the protocol correctly handles concurrent operations on independent keys.
func TestConcurrentWritesDifferentKeys(t *testing.T) {
	t.Parallel()

	conn0, conn1, conn2 := udpListen(t), udpListen(t), udpListen(t)
	peers := map[int]string{
		0: conn0.LocalAddr().String(),
		1: conn1.LocalAddr().String(),
		2: conn2.LocalAddr().String(),
	}
	startNode(t, 0, peers, conn0)
	startNode(t, 1, peers, conn1)
	startNode(t, 2, peers, conn2)

	node0Addr := mustResolve(t, peers[0])
	allAddrs := []*net.UDPAddr{
		mustResolve(t, peers[0]),
		mustResolve(t, peers[1]),
		mustResolve(t, peers[2]),
	}

	const numKeys = 8
	// Track which writes timed out.
	timedOut := make([]int32, numKeys)

	var wg sync.WaitGroup
	for i := 0; i < numKeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each goroutine owns its own client so seq numbers don't collide.
			tc := newTestClient(t)
			key := fmt.Sprintf("pk%d", i)
			val := fmt.Sprintf("pv%d", i)
			resp := tc.send(node0Addr, &Message{Type: MsgTypeWrite, Key: key, Value: val}, 2*time.Second)
			if resp == nil {
				atomic.StoreInt32(&timedOut[i], 1)
				t.Errorf("write key=%s timed out", key)
			}
		}(i)
	}
	wg.Wait()

	// For each successfully acked write, verify all nodes return the correct value.
	tc := newTestClient(t)
	for i := 0; i < numKeys; i++ {
		if atomic.LoadInt32(&timedOut[i]) != 0 {
			continue
		}
		key := fmt.Sprintf("pk%d", i)
		want := fmt.Sprintf("pv%d", i)
		for j, addr := range allAddrs {
			r := tc.send(addr, &Message{Type: MsgTypeRead, Key: key}, 2*time.Second)
			if r == nil {
				t.Errorf("read key=%s from node%d timed out", key, j)
				continue
			}
			if r.Value != want {
				t.Errorf("safety violation: key=%s node%d returned %q, want %q",
					key, j, r.Value, want)
			}
		}
	}
}

// TestConcurrentWritesSameKey fires parallel writes to a single key and
// verifies the agreement property: after all activity settles, every node in
// the cluster returns the same value (no split-brain). Some writers may not
// receive an ack when their pendingWrite record is displaced by a concurrent
// write, but the cluster must always converge to one consistent value.
func TestConcurrentWritesSameKey(t *testing.T) {
	t.Parallel()

	conn0, conn1, conn2 := udpListen(t), udpListen(t), udpListen(t)
	peers := map[int]string{
		0: conn0.LocalAddr().String(),
		1: conn1.LocalAddr().String(),
		2: conn2.LocalAddr().String(),
	}
	startNode(t, 0, peers, conn0)
	startNode(t, 1, peers, conn1)
	startNode(t, 2, peers, conn2)

	node0Addr := mustResolve(t, peers[0])
	allAddrs := []*net.UDPAddr{
		mustResolve(t, peers[0]),
		mustResolve(t, peers[1]),
		mustResolve(t, peers[2]),
	}

	const numWriters = 5
	writtenValues := make([]string, numWriters)
	for i := range writtenValues {
		writtenValues[i] = fmt.Sprintf("writer%d", i)
	}

	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tc := newTestClient(t)
			// Ignore ack timeout: concurrent same-key writes may not all be
			// individually acknowledged (the "winner" is whichever write's
			// pendingWrite record survives the race). The invariant we test is
			// agreement across nodes, not that every write is acked.
			tc.send(node0Addr, &Message{Type: MsgTypeWrite, Key: "shared", Value: writtenValues[i]}, 2*time.Second)
		}(i)
	}
	wg.Wait()

	// After all goroutines finish, read from every node.
	// Reads may briefly stall if a VAL is still propagating — that is fine.
	tc := newTestClient(t)
	agreed := make([]string, len(allAddrs))
	for j, addr := range allAddrs {
		r := tc.send(addr, &Message{Type: MsgTypeRead, Key: "shared"}, 2*time.Second)
		if r == nil {
			t.Fatalf("read from node%d timed out after concurrent writes", j)
		}
		agreed[j] = r.Value
	}

	// Agreement: all nodes must return the same value (no split-brain).
	for j := 1; j < len(agreed); j++ {
		if agreed[j] != agreed[0] {
			t.Fatalf("agreement violation: node0=%q node%d=%q — split-brain detected",
				agreed[0], j, agreed[j])
		}
	}

	// Validity: the agreed value must be one of the values that was written.
	for _, v := range writtenValues {
		if agreed[0] == v {
			return
		}
	}
	t.Fatalf("validity violation: agreed value %q was not written by any writer", agreed[0])
}
