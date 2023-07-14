package base

import (
	"fraisedb/store"
	"github.com/gorilla/websocket"
	"github.com/hashicorp/raft"
	"log"
	"sync"
)

var Channel chan []byte

var Node *raft.Raft
var NodeDB store.DB

var LogHandler *log.Logger

var config ConfigModel

type ConnInfo struct {
	ConnId    string
	Conn      *websocket.Conn
	KeyPrefix string
}

var ConnLock sync.Mutex
var ConnMap = make(map[string]ConnInfo, 1000)
