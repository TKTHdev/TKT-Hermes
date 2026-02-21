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
// TODO: implement Hermes write protocol
//  1. Mark key as StateTrans
//  2. Broadcast INV to all peers
//  3. On receiving all ACKs, broadcast VAL and reply to client
func (h *HermesNode) handleWrite(msg *Message, from *net.UDPAddr) {
	h.log("WRITE key=%s seq=%d (TODO)", msg.Key, msg.Seq)
}

// handleINV is called on non-coordinator nodes when they receive an invalidation.
// TODO: implement Hermes INV handling
//  1. Mark key as StateInvalid, store pending value
//  2. Reply with ACK to coordinator
func (h *HermesNode) handleINV(msg *Message, from *net.UDPAddr) {
	h.log("INV key=%s seq=%d (TODO)", msg.Key, msg.Seq)
}

// handleACK is called on the coordinator when a peer acknowledges an INV.
// TODO: implement Hermes ACK handling
//  1. Count ACKs; when all peers have acked, broadcast VAL
func (h *HermesNode) handleACK(msg *Message, from *net.UDPAddr) {
	h.log("ACK key=%s seq=%d (TODO)", msg.Key, msg.Seq)
}

// handleVAL is called on non-coordinator nodes when the coordinator commits a write.
// TODO: implement Hermes VAL handling
//  1. Apply value to store, mark key as StateValid
func (h *HermesNode) handleVAL(msg *Message, from *net.UDPAddr) {
	h.log("VAL key=%s seq=%d (TODO)", msg.Key, msg.Seq)
}

// handleRead is called on any node when a client issues a read.
//  1. If key is StateValid, reply immediately.
//  2. If key is StateInvalid/StateTrans, stall until VAL arrives.
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
