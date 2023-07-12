package base

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func InitLog() {
	logFile, err := os.OpenFile(fmt.Sprintf("%s/%s.log", Config().Log.Path, strings.Split(time.Now().String(), " ")[0]), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("open log file failed, err:", err)
		return
	}
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Llongfile)
	LogHandler = log.Default()
}
