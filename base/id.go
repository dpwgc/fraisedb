package base

import (
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

func ID() string {
	return fmt.Sprintf("%v%s", time.Now().UnixMilli(), strings.Replace(uuid.New().String(), "-", "", -1))
}
