package base

import (
	"FraiseDB/store"
	"github.com/hashicorp/raft"
	"log"
)

var Node *raft.Raft
var NodeDB store.DB

var LogHandler *log.Logger

var config ConfigModel
