package util

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

type MsgType string

const (
	MsgTypeNewMachine   MsgType = "newMachine"
	MsgTypeNewMachineOK MsgType = "newMachineOK"

	MsgTypeRegister   MsgType = "register"
	MsgTypeDeregister MsgType = "deregister"

	MsgTypeReqScrapeConn MsgType = "reqScrapeConn"
	MsgTypeNewScrapeConn MsgType = "newScrapeConn"
)

const maxMsgLength = 1 << 32

var (
	ErrMsgType      = errors.New("message type error")
	ErrMaxMsgLength = errors.New("message length exceed the limit")
	ErrMsgLength    = errors.New("message length error")
	ErrMsgFormat    = errors.New("message format error")
)

var msgTypes map[byte]MsgType
var msgTypeBytes map[MsgType]byte

func init() {
	msgTypes = map[byte]MsgType{
		'm': MsgTypeNewMachine,
		'o': MsgTypeNewMachineOK,
		'r': MsgTypeRegister,
		'd': MsgTypeDeregister,
		's': MsgTypeReqScrapeConn,
		'c': MsgTypeNewScrapeConn,
	}
	msgTypeBytes = map[MsgType]byte{
		MsgTypeNewMachine:    'm',
		MsgTypeNewMachineOK:  'o',
		MsgTypeRegister:      'r',
		MsgTypeDeregister:    'd',
		MsgTypeReqScrapeConn: 's',
		MsgTypeNewScrapeConn: 'c',
	}
}

func ReadMsg(r io.Reader) (typ MsgType, buffer []byte, err error) {
	// read type
	buffer = make([]byte, 1)
	_, err = r.Read(buffer)
	if err != nil {
		return "", nil, err
	}
	if typ = msgTypes[buffer[0]]; typ == "" {
		return "", nil, ErrMsgType
	}

	// read length
	var length int64
	err = binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return
	}
	if length > maxMsgLength {
		err = ErrMaxMsgLength
		return
	} else if length < 0 {
		err = ErrMsgLength
		return
	}

	// read msg
	buffer = make([]byte, length)
	n, err := io.ReadFull(r, buffer)
	if err != nil {
		return
	}
	if int64(n) != length {
		err = ErrMsgFormat
	}
	return
}

func WriteMsg(w io.Writer, typ MsgType, content []byte) error {
	typeByte, ok := msgTypeBytes[typ]
	if !ok {
		return ErrMsgType
	}
	buffer := bytes.NewBuffer(nil)
	buffer.Write([]byte{typeByte})
	binary.Write(buffer, binary.BigEndian, int64(len(content)))
	buffer.Write(content)
	_, err := w.Write(buffer.Bytes())
	return err
}

type NewClientMessage struct {
	Fqdn      string `json:"fqdn"`
	Timestamp int64  `json:"timestamp"`
	Auth      string `json:"auth"`
}

func (m *NewClientMessage) Marshal() ([]byte, error) {
	return json.Marshal(&m)
}

func UnmarshalIntoNewClientMessage(data []byte) (*NewClientMessage, error) {
	var m NewClientMessage
	err := json.Unmarshal(data, &m)
	return &m, err
}
