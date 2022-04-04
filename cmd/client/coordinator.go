package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/yamux"
	"github.com/pkg/errors"
	"github.com/prometheus-community/pushprox/util"
)

type tunnel struct {
	token   string
	conn    net.Conn
	session *yamux.Session
}

func connectServer(pxyAddr, token string) (*tunnel, error) {
	conn, err := net.Dial("tcp", pxyAddr)
	if err != nil {
		return nil, err
	}
	cfg := yamux.DefaultConfig()
	session, err := yamux.Client(conn, cfg)
	if err != nil {
		return nil, err
	}

	return &tunnel{
		token:   token,
		conn:    conn,
		session: session,
	}, nil
}

func (t *tunnel) OpenStream(plain bool) (c net.Conn, err error) {
	c, err = t.session.Open()
	if plain {
		return
	} else {
		return util.WrapAsCryptoConn(c, []byte(t.token))
	}
}

func (t *tunnel) Close() error {
	return t.session.Close()
}

type Coordinator struct {
	lg             log.Logger
	proxyAddr      string
	token          string
	tunnel         *tunnel
	ctlConn        net.Conn
	fqdn           string
	processes      map[string]*url.URL
	transport      http.RoundTripper
	modifyResponse func(*http.Response) error

	mu sync.Mutex // guard processes update
}

func makeEndpointUrls(eps []Endpoint) (map[string]*url.URL, error) {
	var processes = map[string]*url.URL{}
	for i := range eps {
		if _, ok := processes[eps[i].Name]; ok {
			return nil, fmt.Errorf("duplicate Endpoint, name: %s", eps[i].Name)
		}
		URL, err := eps[i].Url()
		if err != nil {
			return nil, fmt.Errorf("invalid Endpoint[%d]{%v}: %v", i, eps[i], err)
		}
		processes[eps[i].Name] = URL
	}
	return processes, nil
}

func NewCoordinator(c *Config) (*Coordinator, error) {
	processes, err := makeEndpointUrls(c.Eps)
	if err != nil {
		return nil, err
	}
	ts := c.transport
	if ts == nil {
		ts = http.DefaultTransport
	}

	return &Coordinator{
		lg:             c.logger,
		proxyAddr:      c.ProxyAddr,
		token:          c.Token,
		fqdn:           c.FQDN,
		processes:      processes,
		transport:      ts,
		modifyResponse: c.rspModifier,
	}, nil
}

func (c *Coordinator) Start() {
	var err = c.prepare()
	if err != nil {
		level.Error(c.lg).Log("msg", "prepare Coordinator", "err", err)
		return
	}

	for name := range c.processes {
		err = util.WriteMsg(c.ctlConn, util.MsgTypeRegister, []byte(name))
		if err != nil {
			level.Error(c.lg).Log("msg", "send MsgTypeRegister", "err", err)
			return
		}
	}

	for {
		msgType, _, err := util.ReadMsg(c.ctlConn)
		if err != nil {
			level.Error(c.lg).Log("msg", "ReadMsg", "err", err)
			return
		}
		switch msgType {
		case util.MsgTypeReqScrapeConn:
			sconn, err := c.tunnel.OpenStream(false)
			if err != nil {
				level.Error(c.lg).Log("msg", "open scrape stream", "err", err)
				continue
			}
			err = util.WriteMsg(sconn, util.MsgTypeNewScrapeConn, []byte(c.fqdn))
			if err != nil {
				level.Error(c.lg).Log("msg", "Write MsgTypeNewScrapeConn", "err", err)
				return
			}
			go c.handleScrape(sconn)
		default:
			level.Error(c.lg).Log("msg", "unexpect msgType"+msgType)
			return
		}
	}
}

func (c *Coordinator) prepare() error {
	var err error
	c.tunnel, err = connectServer(c.proxyAddr, c.token)
	if err != nil {
		return fmt.Errorf("err connectServe: %v", err)
	}

	ctlConn, err := c.tunnel.OpenStream(true)
	if err != nil {
		return fmt.Errorf("err open control stream: %v", err)
	}
	ts := time.Now().Unix()
	newClientMsg, err := (&util.NewClientMessage{Fqdn: c.fqdn, Timestamp: ts, Auth: util.SignAuth(c.token, ts)}).Marshal()
	if err != nil {
		ctlConn.Close()
		return fmt.Errorf("err Marshal NewClientMessage: %v", err)
	}
	err = util.WriteMsg(ctlConn, util.MsgTypeNewMachine, newClientMsg)
	if err != nil {
		ctlConn.Close()
		return fmt.Errorf("err send MsgTypeNewMachine: %v", err)
	}

	ctlConn, err = util.WrapAsCryptoConn(ctlConn, []byte(c.token))
	if err != nil {
		ctlConn.Close()
		return fmt.Errorf("err wrap conn as CryptoConn: %v", err)
	}
	ctlConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	msgType, _, err := util.ReadMsg(ctlConn)
	if err != nil || msgType != util.MsgTypeNewMachineOK {
		panic("err wait MsgTypeNewMachineOK, client auth failed, invalid token")
	}
	ctlConn.SetReadDeadline(time.Time{})
	c.ctlConn = ctlConn
	return nil
}

func (c *Coordinator) handleScrape(scon net.Conn) {
	for {
		request, err := http.ReadRequest(bufio.NewReader(scon))
		if err != nil {
			level.Error(c.lg).Log("msg", "read scrape request", "err", err)
			scon.Close()
			return
		}

		request.RequestURI = ""
		if request.Host != c.fqdn {
			c.handleErr(scon, request, errors.New("scrape target doesn't match client fqdn"))
			continue
		}

		var process string
		paths := strings.Split(request.URL.Path, "/")
		if !strings.HasPrefix(request.URL.Path, "/") && len(paths) > 0 {
			process = paths[0]
		}
		if strings.HasPrefix(request.URL.Path, "/") && len(paths) > 1 {
			process = paths[1]
		}
		target, exist := c.processes[process]
		if !exist {
			c.handleErr(scon, request, errors.New("scrape target doesn't match client process name"))
			continue
		}

		timeout, err := util.GetHeaderTimeout(request.Header)
		if err != nil {
			c.handleErr(scon, request, err)
			return
		}

		func() {
			ctx, cancel := context.WithTimeout(request.Context(), timeout)
			defer cancel()
			request = request.WithContext(ctx)
			request.URL = target
			scrapeResp, err := c.transport.RoundTrip(request)
			if err != nil {
				msg := fmt.Sprintf("failed to scrape %s", request.URL.String())
				c.handleErr(scon, request, errors.Wrap(err, msg))
				return
			}
			if c.modifyResponse != nil {
				err = c.modifyResponse(scrapeResp)
				if err != nil {
					msg := fmt.Sprintf("failed to mutate scraped response, process: %s", process)
					c.handleErr(scon, request, errors.Wrap(err, msg))
					return
				}
			}
			err = scrapeResp.Write(scon)
			if err != nil {
				level.Error(c.lg).Log("msg", "write scrape result", "err", err)
				scon.Close()
				return
			}
		}()
	}
}

func (c *Coordinator) handleErr(scon net.Conn, request *http.Request, err error) {
	rsp := http.Response{}
	rsp.StatusCode = http.StatusInternalServerError
	errMsg := err.Error()
	rsp.Body = ioutil.NopCloser(strings.NewReader(errMsg))
	rsp.ContentLength = int64(len([]byte(errMsg)))
	rsp.Write(scon)
}

func (c *Coordinator) Update(eps []Endpoint) error {
	processes, err := makeEndpointUrls(eps)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// deregister old processes
	for name := range c.processes {
		err = util.WriteMsg(c.tunnel.conn, util.MsgTypeDeregister, []byte(name))
		if err != nil {
			level.Error(c.lg).Log("msg", "send MsgTypeDeregister", "err", err)
			return err
		}
	}
	// register updated processes
	c.processes = processes
	for name := range c.processes {
		err = util.WriteMsg(c.tunnel.conn, util.MsgTypeRegister, []byte(name))
		if err != nil {
			level.Error(c.lg).Log("msg", "send MsgTypeRegister", "err", err)
			return err
		}
	}
	return nil
}

func (c *Coordinator) Stop() {
	c.tunnel.Close()
}
