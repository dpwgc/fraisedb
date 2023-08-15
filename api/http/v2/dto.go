package http_v2

import (
	"encoding/json"
	"fraisedb/base"
	"io"
	"net/http"
)

type nodeCommand struct {
	Addr     string `json:"addr"`
	TcpPort  int    `json:"tcpPort"`
	HttpPort int    `json:"httpPort"`
}

type kvCommand struct {
	SaveType int    `json:"type"`
	Value    string `json:"value"`
	Incr     int64  `json:"incr"`
	TTL      int64  `json:"ttl"`
}

func result(w http.ResponseWriter, code int, data any, errorMsg error) {
	var res = make(map[string]any, 3)
	res["code"] = code
	if code != base.SuccessCode {
		res["error"] = errorMsg.Error()
		base.LogHandler.Println(base.LogErrorTag, errorMsg)
	}
	if data != nil {
		res["data"] = data
	}
	marshal, _ := json.Marshal(res)
	_, err := w.Write(marshal)
	if err != nil {
		base.LogHandler.Println(base.LogErrorTag, err)
		return
	}
}

func readKVCommand(r *http.Request) (kvCommand, error) {
	command := kvCommand{}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return command, err
	}
	err = json.Unmarshal(body, &command)
	return command, err
}

func readNodeCommand(r *http.Request) (nodeCommand, error) {
	command := nodeCommand{}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return command, err
	}
	err = json.Unmarshal(body, &command)
	return command, err
}
