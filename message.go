package main

import (
	"bytes"
	"encoding/binary"
)

// Hermes protocol message types.
const (
	MsgTypeWrite    = iota // client -> coordinator: write request
	MsgTypeINV             // coordinator -> all peers: invalidate key
	MsgTypeACK             // peer -> coordinator: ack INV
	MsgTypeVAL             // coordinator -> all peers: validate (commit) write
	MsgTypeRead            // client -> any node: read request
	MsgTypeResponse        // node -> client: read/write response
)

type Message struct {
	Type       uint8
	Seq        uint64
	Key        string
	Value      string
	ClientAddr string // originating client address (for coordinator to reply)
}

func (m *Message) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, m.Type)
	binary.Write(buf, binary.BigEndian, m.Seq)

	keyBytes := []byte(m.Key)
	binary.Write(buf, binary.BigEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)

	valBytes := []byte(m.Value)
	binary.Write(buf, binary.BigEndian, uint16(len(valBytes)))
	buf.Write(valBytes)

	addrBytes := []byte(m.ClientAddr)
	binary.Write(buf, binary.BigEndian, uint16(len(addrBytes)))
	buf.Write(addrBytes)

	return buf.Bytes()
}

func DecodeMessage(data []byte) (*Message, error) {
	buf := bytes.NewReader(data)
	m := &Message{}

	binary.Read(buf, binary.BigEndian, &m.Type)
	binary.Read(buf, binary.BigEndian, &m.Seq)

	var keyLen uint16
	binary.Read(buf, binary.BigEndian, &keyLen)
	keyBytes := make([]byte, keyLen)
	buf.Read(keyBytes)
	m.Key = string(keyBytes)

	var valLen uint16
	binary.Read(buf, binary.BigEndian, &valLen)
	valBytes := make([]byte, valLen)
	buf.Read(valBytes)
	m.Value = string(valBytes)

	var addrLen uint16
	binary.Read(buf, binary.BigEndian, &addrLen)
	addrBytes := make([]byte, addrLen)
	buf.Read(addrBytes)
	m.ClientAddr = string(addrBytes)

	return m, nil
}
