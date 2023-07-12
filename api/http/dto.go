package http

import (
	"FraiseDB/base"
	"encoding/json"
	"io"
	"net/http"
)

type resultDTO struct {
	Ok    bool   `json:"ok"`
	Data  any    `json:"data"`
	Error string `json:"error"`
}

type nodeCommand struct {
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

type kvCommand struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   int64  `json:"ttl"`
}

func result(w http.ResponseWriter, ok bool, data any, errorMsg string) {
	if len(errorMsg) > 0 {
		base.LogHandler.Println(base.LogErrorTag, errorMsg)
	}
	marshal, _ := json.Marshal(resultDTO{
		Ok:    ok,
		Data:  data,
		Error: errorMsg,
	})
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
