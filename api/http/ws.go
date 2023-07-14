package http

import (
	"fraisedb/base"
	"fraisedb/cluster"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"gopkg.in/yaml.v3"
	"net/http"
	"strings"
)

var upGrader = websocket.Upgrader{
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
}

func subscribe(w http.ResponseWriter, r *http.Request, p httprouter.Params) {

	keyPrefix := p.ByName("keyPrefix")
	connId := base.ID()

	upGrader.CheckOrigin = func(r *http.Request) bool { return true }

	conn, err := upGrader.Upgrade(w, r, nil)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return
	}

	ci := base.ConnInfo{
		ConnId:    connId,
		Conn:      conn,
		KeyPrefix: keyPrefix,
	}

	go listener(ci)
}

func listener(ci base.ConnInfo) {

	base.ConnLock.Lock()
	base.ConnMap[ci.ConnId] = ci
	base.ConnLock.Unlock()

	defer func() {
		err := ci.Conn.Close()
		if err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
		base.ConnLock.Lock()
		delete(base.ConnMap, ci.ConnId)
		base.ConnLock.Unlock()
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

func initConsumer() {
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
	for _, ci := range base.ConnMap {
		go push(ci, al)
	}
}

func push(ci base.ConnInfo, al cluster.ApplyLogModel) {
	err := error(nil)
	defer func() {
		if err != nil {
			err = ci.Conn.Close()
			if err != nil {
				base.LogHandler.Println(base.LogErrorTag, err)
			}
			base.ConnLock.Lock()
			delete(base.ConnMap, ci.ConnId)
			base.ConnLock.Unlock()
		}
	}()
	if len(ci.KeyPrefix) == 0 {
		// 回写请求
		if err = ci.Conn.WriteJSON(al); err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
		return
	}
	if strings.HasPrefix(al.Key, ci.KeyPrefix) {
		// 回写请求
		if err = ci.Conn.WriteJSON(al); err != nil {
			base.LogHandler.Println(base.LogErrorTag, err)
		}
	}
}
