package http

import (
	"fraisedb/base"
	"fraisedb/cluster"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
	"sync"
)

var upGrader = websocket.Upgrader{
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
}

type connInfo struct {
	ConnId    string
	Conn      *websocket.Conn
	KeyPrefix string
}

var connLock sync.Mutex
var connMap = make(map[string]connInfo, 1000)

func subscribe(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

	keyPrefix := p.ByName("keyPrefix")
	connId := p.ByName("connId")

	upGrader.CheckOrigin = func(r *http.Request) bool { return true }

	conn, err := upGrader.Upgrade(w, r, nil)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return
	}

	ci := connInfo{
		ConnId:    connId,
		Conn:      conn,
		KeyPrefix: keyPrefix,
	}

	go listener(ci)
}

func listener(ci connInfo) {

	connLock.Lock()
	connMap[ci.ConnId] = ci
	connLock.Unlock()

	defer func() {
		err := ci.Conn.Close()
		if err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
		connLock.Lock()
		delete(connMap, ci.ConnId)
		connLock.Unlock()
	}()
	for {
		// 接收数据
		messageType, _, err := ci.Conn.ReadMessage()
		if err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
			return
		}

		// 回写请求
		if err = ci.Conn.WriteMessage(messageType, []byte("1")); err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
			return
		}
	}
}

func initEventConsumer() {
	go func() {
		for {
			msg := <-base.Channel
			broadcast(msg)
		}
	}()
}

func broadcast(msg []byte) {
	al := cluster.ApplyLogModel{}
	err := yaml.Unmarshal(msg, &al)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return
	}
	for _, ci := range connMap {
		go push(ci, msg, al)
	}
}

func push(ci connInfo, msg []byte, al cluster.ApplyLogModel) {
	err := error(nil)
	defer func() {
		if err != nil {
			err = ci.Conn.Close()
			if err != nil {
				base.LogHandler.Println(base.LogErrorTag, err)
			}
			connLock.Lock()
			delete(connMap, ci.ConnId)
			connLock.Unlock()
		}
	}()
	if len(ci.KeyPrefix) == 0 {
		// 回写请求
		if err = ci.Conn.WriteMessage(1, msg); err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
		return
	}
	if strings.HasPrefix(al.Key, ci.KeyPrefix) {
		// 回写请求
		if err = ci.Conn.WriteMessage(1, msg); err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
	}
}
