package base

import (
	"fraisedb/store"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/raft"
	"log"
)

var Channel chan []byte

var NodeRaft *raft.Raft
var NodeDB store.DB

var LogHandler *log.Logger

var config ConfigModel

type ConnInfo struct {
	ConnId    string
	Conn      *websocket.Conn
	Namespace string
	KeyPrefix string
}

var ConnMap = make(map[string]ConnInfo, 1000)
