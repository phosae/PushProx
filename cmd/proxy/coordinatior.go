package proxy

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus-community/pushprox/util"
)

type Coordinator struct {
	lg   log.Logger
	fqdn string

	mu      sync.Mutex // guard stopped, known
	stopped bool
	known   map[string]time.Time

	ctlConn      net.Conn
	scrapeConnCh chan net.Conn
}

func (c *Coordinator) addScrapeTarget(fqdn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.known[fqdn] = time.Now()
	knownTargets.Set(float64(len(c.known)))
}

func (c *Coordinator) delScrapeTarget(fqdn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.known, fqdn)
	knownTargets.Set(float64(len(c.known)))
}

// KnownTargets returns a list of available targets
func (c *Coordinator) KnownTargets() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	known := make([]string, 0, len(c.known))
	for k := range c.known {
		known = append(known, k)
	}
	return known
}

func (c *Coordinator) getScrapeConn() (net.Conn, error) {
	select {
	case conn, ok := <-c.scrapeConnCh:
		if ok {
			return conn, nil
		}
		return nil, fmt.Errorf("err scrapeConn channel closed")
	default:
		level.Debug(c.lg).Log("msg", "send "+util.MsgTypeReqScrapeConn+" to proxyc for new connection")
		err := util.WriteMsg(c.ctlConn, util.MsgTypeReqScrapeConn, []byte{})
		if err != nil {
			return nil, fmt.Errorf("err control connection closed")
		}
	}

	select {
	case conn := <-c.scrapeConnCh:
		return conn, nil
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("err timeout getScrapeConn")
	}
}

func (c *Coordinator) registerScrapeConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			level.Error(c.lg).Log("panic error", err)
		}
	}()
	c.scrapeConnCh <- conn
}

func (c *Coordinator) start() {
	for {
		msgType, msg, err := util.ReadMsg(c.ctlConn)
		if err != nil {
			if err == io.EOF {
				level.Debug(c.lg).Log("msg", "control connection closed")
				c.stop()
				return
			}
			level.Debug(c.lg).Log("err read message", err)
			c.stop()
			return
		}

		conn := c.ctlConn
		switch msgType {
		case util.MsgTypeRegister:
			process := string(msg)
			c.addScrapeTarget(fmt.Sprintf("%s:80/%s", c.fqdn, process))
		case util.MsgTypeDeregister:
			process := string(msg)
			c.delScrapeTarget(fmt.Sprintf("%s:80/%s", c.fqdn, process))
		default:
			level.Warn(c.lg).Log("msg", "Error message type from conn"+conn.RemoteAddr().String())
			conn.Close()
		}
	}
}

func (c *Coordinator) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.known = nil
	close(c.scrapeConnCh)
	c.ctlConn.Close()
	c.stopped = true
}
