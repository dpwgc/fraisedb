package base

import (
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// CreatePath 自动创建目录
func CreatePath(path string) error {
	return os.MkdirAll(path, 0766)
}

func ID() string {
	return fmt.Sprintf("%v%s", time.Now().UnixMilli(), strings.Replace(uuid.New().String(), "-", "", -1))
}

func HttpGet(url string) ([]byte, error) {
	client := http.Client{
		Timeout: ConnectTimeout3 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			LogHandler.Println(LogErrorTag, err)
		}
	}(resp.Body)

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
