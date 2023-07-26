package base

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"
)

// CreatePath 自动创建目录
func CreatePath(path string) error {
	return os.MkdirAll(path, 0766)
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

func HttpForward(writer http.ResponseWriter, request *http.Request, forwardUrl string) error {
	u, err := url.Parse(forwardUrl)
	if nil != err {
		return err
	}
	proxy := httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL = u
		},
	}
	proxy.ServeHTTP(writer, request)
	return nil
}
