package util

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCrypto(t *testing.T) {
	cases := []struct {
		key, text string
	}{
		{
			key:  "",
			text: "你好",
		}, {
			key:  "",
			text: "",
		}, {
			key:  "pwd",
			text: "你好",
		}, {
			key:  "pwd",
			text: "",
		},
	}

	for i := range cases {
		key, text := cases[i].key, cases[i].text
		var rwBuf io.ReadWriter = bytes.NewBuffer(nil)
		w, err := NewCryptoWriter(rwBuf, []byte(key))
		assert.NoError(t, err)
		r := NewCryptoReader(rwBuf, []byte(key))

		w.Write([]byte(text))
		plain := bytes.NewBuffer(nil)
		io.Copy(plain, r)
		assert.Equal(t, text, string(plain.Bytes()))
	}
}
