package main

import (
	"fmt"
	"log"
	"net"
)

func (h *HermesNode) listen() {
	addr, err := net.ResolveUDPAddr("udp", h.peers[h.me])
	if err != nil {
		log.Fatalf("[Node %d] Failed to resolve address: %v", h.me, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("[Node %d] Failed to listen UDP: %v", h.me, err)
	}
	h.udpConn = conn
	h.log("Listening on %s (UDP)", h.peers[h.me])

	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			h.log("Failed to read UDP: %v", err)
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		go h.handleMessage(data, remoteAddr)
	}
}

func (h *HermesNode) handleMessage(data []byte, from *net.UDPAddr) {
	if len(data) == 0 {
		return
	}

	msg, err := DecodeMessage(data)
	if err != nil {
		h.log("Failed to decode message: %v", err)
		return
	}

	switch msg.Type {
	case MsgTypeWrite:
		h.handleWrite(msg, from)
	case MsgTypeINV:
		h.handleINV(msg, from)
	case MsgTypeACK:
		h.handleACK(msg, from)
	case MsgTypeVAL:
		h.handleVAL(msg, from)
	case MsgTypeRead:
		h.handleRead(msg, from)
	}
}

// handleWrite is called on the coordinator for a key when a client issues a write.
//  1. Mark key as StateTrans
//  2. Broadcast INV to all peers; store pendingWrite for ACK counting
//  3. On receiving all ACKs (handleACK), broadcast VAL and reply to client
func (h *HermesNode) handleWrite(msg *Message, from *net.UDPAddr) {
	h.log("WRITE key=%s seq=%d", msg.Key, msg.Seq)

	h.mu.Lock()
	h.kstate[msg.Key] = StateTrans
	h.mu.Unlock()

	// Collect peers excluding self.
	peers := make([]int, 0, len(h.peers)-1)
	for id := range h.peers {
		if id != h.me {
			peers = append(peers, id)
		}
	}

	// Single-node cluster: commit immediately without INV/ACK round-trip.
	if len(peers) == 0 {
		h.mu.Lock()
		h.store[msg.Key] = msg.Value
		h.kstate[msg.Key] = StateValid
		h.mu.Unlock()
		h.readCond.Broadcast()
		resp := &Message{Type: MsgTypeResponse, Seq: msg.Seq, Key: msg.Key}
		h.sendToAddr(from, resp.Encode())
		return
	}

	h.writeMu.Lock()
	h.pendingWrite[msg.Key] = &writeRecord{
		value:      msg.Value,
		acksNeeded: len(peers),
		clientAddr: from.String(),
		seq:        msg.Seq,
	}
	h.writeMu.Unlock()

	inv := &Message{Type: MsgTypeINV, Seq: msg.Seq, Key: msg.Key, Value: msg.Value}
	data := inv.Encode()
	for _, id := range peers {
		h.sendTo(id, data)
	}
}

// handleINV is called on non-coordinator nodes when they receive an invalidation.
//  1. Mark key as StateInvalid
//  2. Reply with ACK to coordinator
func (h *HermesNode) handleINV(msg *Message, from *net.UDPAddr) {
	h.log("INV key=%s seq=%d", msg.Key, msg.Seq)

	h.mu.Lock()
	h.kstate[msg.Key] = StateInvalid
	h.mu.Unlock()

	ack := &Message{Type: MsgTypeACK, Seq: msg.Seq, Key: msg.Key}
	h.sendToAddr(from, ack.Encode())
}

// handleACK is called on the coordinator when a peer acknowledges an INV.
//  1. Count ACKs; when all peers have acked, broadcast VAL and reply to client
func (h *HermesNode) handleACK(msg *Message, from *net.UDPAddr) {
	h.log("ACK key=%s seq=%d", msg.Key, msg.Seq)

	h.writeMu.Lock()
	rec := h.pendingWrite[msg.Key]
	if rec == nil {
		h.writeMu.Unlock()
		return
	}
	rec.acksNeeded--
	done := rec.acksNeeded == 0
	h.writeMu.Unlock()

	if !done {
		return
	}

	// All ACKs received: broadcast VAL to peers.
	val := &Message{Type: MsgTypeVAL, Seq: rec.seq, Key: msg.Key, Value: rec.value}
	data := val.Encode()
	for id := range h.peers {
		if id != h.me {
			h.sendTo(id, data)
		}
	}

	// Apply locally and reply to client.
	h.mu.Lock()
	h.store[msg.Key] = rec.value
	h.kstate[msg.Key] = StateValid
	h.mu.Unlock()
	h.readCond.Broadcast()

	h.writeMu.Lock()
	delete(h.pendingWrite, msg.Key)
	h.writeMu.Unlock()

	clientAddr, _ := net.ResolveUDPAddr("udp", rec.clientAddr)
	resp := &Message{Type: MsgTypeResponse, Seq: rec.seq, Key: msg.Key}
	h.sendToAddr(clientAddr, resp.Encode())
}

// handleVAL is called on non-coordinator nodes when the coordinator commits a write.
//  1. Apply value to store, mark key as StateValid, wake stalled readers
func (h *HermesNode) handleVAL(msg *Message, from *net.UDPAddr) {
	h.log("VAL key=%s seq=%d", msg.Key, msg.Seq)

	h.mu.Lock()
	h.store[msg.Key] = msg.Value
	h.kstate[msg.Key] = StateValid
	h.mu.Unlock()
	h.readCond.Broadcast()
}

func (h *HermesNode) handleRead(msg *Message, from *net.UDPAddr) {
	h.mu.Lock()
	for h.kstate[msg.Key] != StateValid {
		h.readCond.Wait()
	}
	value := h.store[msg.Key]
	h.mu.Unlock()

	h.log("READ key=%s seq=%d -> %q", msg.Key, msg.Seq, value)
	resp := &Message{Type: MsgTypeResponse, Seq: msg.Seq, Key: msg.Key, Value: value}
	h.sendToAddr(from, resp.Encode())
}

func (h *HermesNode) sendTo(peerID int, data []byte) error {
	addr, err := net.ResolveUDPAddr("udp", h.peers[peerID])
	if err != nil {
		return err
	}
	_, err = h.udpConn.WriteToUDP(data, addr)
	return err
}

func (h *HermesNode) sendToAddr(addr *net.UDPAddr, data []byte) error {
	_, err := h.udpConn.WriteToUDP(data, addr)
	return err
}

func (h *HermesNode) log(format string, args ...interface{}) {
	if h.debug {
		msg := fmt.Sprintf(format, args...)
		log.Printf("[Node %d] %s", h.me, msg)
	}
}
