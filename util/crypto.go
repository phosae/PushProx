package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"io"
	"net"

	"golang.org/x/crypto/pbkdf2"
)

var DefaultSalt = "pushprox"

type cryptoConn struct {
	r io.Reader
	w io.Writer
	net.Conn
}

func (cc *cryptoConn) Read(b []byte) (n int, err error) {
	return cc.r.Read(b)
}

func (cc *cryptoConn) Write(b []byte) (n int, err error) {
	return cc.w.Write(b)
}

func WrapAsCryptoConn(c net.Conn, key []byte) (net.Conn, error) {
	w, err := NewCryptoWriter(c, key)
	if err != nil {
		return nil, err
	}
	return &cryptoConn{
		r:    NewCryptoReader(c, key),
		w:    w,
		Conn: c,
	}, nil
}

// NewCryptoReader returns a new Reader that decrypts bytes from r
func NewCryptoReader(r io.Reader, key []byte) io.Reader {
	key = pbkdf2.Key(key, []byte(DefaultSalt), 64, aes.BlockSize, sha1.New)

	return &reader{
		r:   r,
		key: key,
	}
}

// reader is an io.Reader that can read encrypted bytes.
// Now it only supports aes-128-cfb.
type reader struct {
	r   io.Reader
	dec *cipher.StreamReader
	key []byte
	iv  []byte
	err error
}

// Read satisfies the io.Reader interface.
func (r *reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	if r.dec == nil {
		iv := make([]byte, aes.BlockSize)
		if _, err = io.ReadFull(r.r, iv); err != nil {
			return
		}
		r.iv = iv

		block, err := aes.NewCipher(r.key)
		if err != nil {
			return 0, err
		}
		r.dec = &cipher.StreamReader{
			S: cipher.NewCFBDecrypter(block, iv),
			R: r.r,
		}
	}

	n, err = r.dec.Read(p)
	if err != nil {
		r.err = err
	}
	return
}

// NewCryptoWriter returns a new Writer that encrypts bytes to w.
func NewCryptoWriter(w io.Writer, key []byte) (io.Writer, error) {
	key = pbkdf2.Key(key, []byte(DefaultSalt), 64, aes.BlockSize, sha1.New)

	// random iv
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	return &writer{
		w: w,
		enc: &cipher.StreamWriter{
			S: cipher.NewCFBEncrypter(block, iv),
			W: w,
		},
		key: key,
		iv:  iv,
	}, nil
}

// Writer is an io.Writer that can write encrypted bytes.
// Now it only support aes-128-cfb.
type writer struct {
	w      io.Writer
	enc    *cipher.StreamWriter
	key    []byte
	iv     []byte
	ivSend bool
	err    error
}

// Write satisfies the io.Writer interface.
func (w *writer) Write(p []byte) (nRet int, errRet error) {
	if w.err != nil {
		return 0, w.err
	}

	// When write is first called, iv will be written to w.w
	if !w.ivSend {
		w.ivSend = true
		_, errRet = w.w.Write(w.iv)
		if errRet != nil {
			w.err = errRet
			return
		}
	}

	nRet, errRet = w.enc.Write(p)
	if errRet != nil {
		w.err = errRet
	}
	return
}
