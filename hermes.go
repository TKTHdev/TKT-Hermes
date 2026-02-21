package main

import (
	"log"
	"net"
	"sync"
)

// KeyState represents the per-key replication state in the Hermes protocol.
type KeyState int

const (
	StateValid   KeyState = iota // key is up-to-date; reads allowed
	StateInvalid                 // invalidation received; reads must stall
	StateTrans                   // write in progress (coordinator only)
)

// writeRecord tracks an in-flight write on the coordinator side.
type writeRecord struct {
	value      string
	acksNeeded int
	clientAddr string
	seq        uint64
}

type HermesNode struct {
	me    int
	peers map[int]string // id -> "ip:port"

	udpConn *net.UDPConn

	// Key-value state machine
	mu       sync.Mutex
	readCond *sync.Cond
	store    map[string]string
	kstate   map[string]KeyState // per-key protocol state (default: StateValid)

	// In-flight writes on the coordinator: key -> pending write
	writeMu      sync.Mutex
	pendingWrite map[string]*writeRecord

	debug bool
}

func NewHermesNode(id int, confPath string, debug bool) *HermesNode {
	peers := parseConfig(confPath)

	h := &HermesNode{
		me:           id,
		peers:        peers,
		store:        make(map[string]string),
		kstate:       make(map[string]KeyState),
		pendingWrite: make(map[string]*writeRecord),
		debug:        debug,
	}
	h.readCond = sync.NewCond(&h.mu)
	return h
}

func (h *HermesNode) Run() {
	log.Printf("[Node %d] Starting Hermes node on %s", h.me, h.peers[h.me])
	go h.listen()
	select {}
}
