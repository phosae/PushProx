package util

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

func SignAuth(token string, timestamp int64) (key string) {
	token = token + fmt.Sprintf("%d", timestamp)
	hash := md5.New()
	hash.Write([]byte(token))
	data := hash.Sum(nil)
	return hex.EncodeToString(data)
}
