package base

import (
	"fmt"
	"github.com/google/uuid"
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
