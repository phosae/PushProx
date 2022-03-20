package proxy

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/go-kit/log"
)

func TestServer(t *testing.T) {
	l, err := net.Listen("tcp", ":7080")
	if err != nil {
		panic(err)
	}
	s := &server{
		l:       l,
		lg:      log.NewLogfmtLogger(os.Stdout),
		remotes: map[string]*Coordinator{},
		tokens:  []string{""},
	}
	s.lg.Log("msg", fmt.Sprintf("handle proxyc request on %s", ":7080"))
	ha := newHttpHandler(s, log.NewLogfmtLogger(os.Stdout))
	go func() {
		s.lg.Log("msg", fmt.Sprintf("handle prometheus request on %s", ":8080"))
		http.ListenAndServe(":8080", ha)
	}()
	s.StartServe()
}
