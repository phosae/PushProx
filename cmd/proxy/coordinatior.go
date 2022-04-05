package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
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

func (c *Coordinator) getScrapeConn(timeout time.Duration) (net.Conn, error) {
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
	case <-time.After(timeout):
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

func (c *Coordinator) handleScrape(w http.ResponseWriter, r *http.Request) {
	if r.Header == nil {
		r.Header = map[string][]string{}
	}
	util.EnsureHeaderTimeout(maxScrapeTimeout, defaultScrapeTimeout, r.Header)
	timeout, _ := util.GetHeaderTimeout(r.Header)
	rwc, err := c.getScrapeConn(timeout)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer func() {
		go func() {
			if len(c.scrapeConnCh) == cap(c.scrapeConnCh) {
				rwc.Close()
			} else {
				c.registerScrapeConn(rwc)
			}
		}()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := r.Write(rwc)
		if err != nil {
			level.Error(c.lg).Log("msg", "failed to write connection", "err", err)
			rwc.Close()
		}
	}()
	go func() {
		defer wg.Done()
		resp, err := http.ReadResponse(bufio.NewReader(rwc), nil)
		if err != nil {
			rwc.Close()
			return
		}
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}()
	wg.Wait()
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
			c.addScrapeTarget(fmt.Sprintf("%s.%s:80", process, c.fqdn))
		case util.MsgTypeDeregister:
			process := string(msg)
			c.delScrapeTarget(fmt.Sprintf("%s.%s:80", process, c.fqdn))
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
