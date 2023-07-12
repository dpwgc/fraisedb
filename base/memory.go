package base

import (
	"fraisedb/store"
	"github.com/hashicorp/raft"
	"log"
)

var Channel chan []byte

var Node *raft.Raft
var NodeDB store.DB

var LogHandler *log.Logger

var config ConfigModel
