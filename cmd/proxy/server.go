package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/hashicorp/yamux"
	"github.com/prometheus-community/pushprox/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	listenPxyAddress     = kingpin.Flag("web.proxy-address", "Address to listen on for proxy requests.").Default(":8080").String()
	listenServerAddress  = kingpin.Flag("web.server-address", "Address to listen on for client requests.").Default(":7080").String()
	maxScrapeTimeout     = kingpin.Flag("scrape.max-timeout", "Any scrape with a timeout higher than this will have to be clamped to this.").Default("5m").Duration()
	defaultScrapeTimeout = kingpin.Flag("scrape.default-timeout", "If a scrape lacks a timeout, use this value.").Default("15s").Duration()

	authTokens    = kingpin.Flag("auth.tokens", "String contains comma split tokens, i.e pwd-a,token-x").String()
	authTokenFile = kingpin.Flag("auth.token-file", "File contains comma split tokens, i.e pwd-a,token-x. If specified, auth.tokens will be ignored").String()
)

const (
	namespace                     = "pushprox" // For Prometheus metrics.
	connReadTimeout time.Duration = 10 * time.Second
)

var (
	knownTargets = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "targets",
			Help:      "Number of known pushprox targets.",
		},
	)

	httpProxyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_proxy_duration_seconds",
			Help:      "Time taken by code.",
		}, []string{"code"})
)

func init() {
	prometheus.MustRegister(httpProxyHistogram)
}

type server struct {
	l      net.Listener
	lg     log.Logger
	tokens []string

	mu      sync.Mutex
	remotes map[string]*Coordinator
}

func (s *server) StartServe() {
	s.HandleListener()
}

func (s *server) HandleListener() {
	for {
		con, err := s.l.Accept()
		if err != nil {
			level.Warn(s.lg).Log("msg", "Listener for incoming connections from client closed")
			return
		}
		ctx := context.Background()
		go func() {
			session, err := yamux.Server(con, nil)
			if err != nil {
				level.Error(s.lg).Log("msg", "failed to create mux connection: %v", err)
				con.Close()
				return
			}
			as := &authSession{Session: session}
			for {
				stream, err := session.AcceptStream()
				if err != nil {
					level.Warn(s.lg).Log("msg", fmt.Sprintf("accept new mux stream error: %v", err))
					session.Close()
					return
				}

				var sc net.Conn = stream
				if token, ok := as.token.Load().(string); ok {
					sc, err = util.WrapAsCryptoConn(stream, []byte(token))
					if err != nil {
						level.Warn(s.lg).Log("msg", fmt.Sprintf("wrap stream with crypto failed: %v", err))
						session.Close()
						return
					}
				}
				go s.handleConnection(ctx, as, sc)
			}
		}()
	}
}

type authSession struct {
	token atomic.Value
	*yamux.Session
}

func (s *server) handleConnection(ctx context.Context, session *authSession, conn net.Conn) {
	s.lg.Log("msg", fmt.Sprintf("rcv conn: %s", conn.RemoteAddr()))

	conn.SetReadDeadline(time.Now().Add(connReadTimeout))
	msgType, msg, err := util.ReadMsg(conn)
	if err != nil {
		level.Debug(s.lg).Log("msg", "Failed to read message", "err", err)
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	switch msgType {
	case util.MsgTypeNewMachine:
		newClientMsg, err := util.UnmarshalIntoNewClientMessage(msg)
		if err != nil {
			level.Warn(s.lg).Log("msg", "broken MsgTypeNewMachine", "err", err)
			conn.Close()
			return
		}
		token, err := s.auth(newClientMsg)
		if err != nil {
			level.Warn(s.lg).Log("msg", newClientMsg, "err", err)
			conn.Close()
			return
		}
		session.token.Store(token)
		cryptoConn, err := util.WrapAsCryptoConn(conn, []byte(token))
		if err != nil {
			level.Error(s.lg).Log("msg", "wrap raw conn as crypto conn error")
			conn.Close()
			return
		}
		err = util.WriteMsg(cryptoConn, util.MsgTypeNewMachineOK, []byte{})
		if err != nil {
			level.Error(s.lg).Log("msg", "write MsgTypeNewMachineOK", "err", err)
			cryptoConn.Close()
			return
		}

		fqdn := newClientMsg.Fqdn

		s.mu.Lock()
		if old := s.remotes[fqdn]; old != nil {
			go old.stop()
		}
		c := &Coordinator{
			lg:           s.lg,
			fqdn:         fqdn,
			known:        map[string]time.Time{},
			ctlConn:      cryptoConn,
			scrapeConnCh: make(chan net.Conn, 10),
		}
		s.remotes[fqdn] = c
		go c.start()
		s.mu.Unlock()
	case util.MsgTypeNewScrapeConn:
		fqdn := string(msg)
		var c *Coordinator
		s.mu.Lock()
		if c = s.remotes[fqdn]; c == nil {
			level.Warn(s.lg).Log("msg", "Error can't find coordinator", "machine", fqdn, "addr", conn.RemoteAddr().String())
			conn.Close()
		}
		s.mu.Unlock()
		c.registerScrapeConn(conn)
	default:
		level.Warn(s.lg).Log("msg", fmt.Sprintf("Error message type for the new connection [%s]", conn.RemoteAddr().String()))
		conn.Close()
	}
}

func (s *server) auth(msg *util.NewClientMessage) (token string, err error) {
	for i := range s.tokens {
		if util.SignAuth(s.tokens[i], msg.Timestamp) == msg.Auth {
			return s.tokens[i], nil
		}
	}
	return "", fmt.Errorf("auth failed")
}

type httpHandler struct {
	proxy     http.Handler
	mux       *http.ServeMux
	transport http.RoundTripper
	logger    log.Logger
	s         *server
}

func newHttpHandler(s *server, lg log.Logger) *httpHandler {
	h := &httpHandler{s: s, logger: lg, mux: http.NewServeMux()}
	// api handlers
	handlers := map[string]http.HandlerFunc{
		"/targets": h.handleListTargets,
		"/metrics": promhttp.Handler().ServeHTTP,
	}
	for path, handlerFunc := range handlers {
		h.mux.Handle(path, handlerFunc)
	}
	h.proxy = promhttp.InstrumentHandlerDuration(httpProxyHistogram, http.HandlerFunc(h.handleScrape))
	return h
}

type targetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// handleListTargets handles requests to list available clients as a JSON array.
func (h *httpHandler) handleListTargets(w http.ResponseWriter, r *http.Request) {
	var known []string
	for _, c := range h.s.remotes {
		known = append(known, c.KnownTargets()...)
	}
	targets := make([]*targetGroup, 0, len(known))
	for _, k := range known {
		targets = append(targets, &targetGroup{Targets: []string{k}})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(targets)
	level.Info(h.logger).Log("msg", "Responded to /clients", "client_count", len(known))
}

// ServeHTTP discriminates between proxy requests (e.g. from Prometheus) and other requests (e.g. from the Client).
func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Host != "" { // Proxy request
		h.proxy.ServeHTTP(w, r)
	} else { // Non-proxy requests
		h.mux.ServeHTTP(w, r)
	}
}

func (h *httpHandler) handleScrape(w http.ResponseWriter, r *http.Request) {
	h.s.mu.Lock()
	// <process_name>.<fqdn>:80
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	parts := strings.SplitN(host, ".", 2)
	if len(parts) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c := h.s.remotes[parts[1]]
	h.s.mu.Unlock()
	if c == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	c.handleScrape(w, r)
}

func getAuthTokens() ([]string, error) {
	var tokens string
	if authTokens != nil && *authTokens != "" {
		tokens = *authTokens
	}
	if authTokenFile != nil && *authTokenFile != "" {
		b, err := ioutil.ReadFile(*authTokenFile)
		if err != nil {
			return nil, err
		}
		tokens = string(b)
	}
	return strings.Split(strings.TrimSpace(tokens), ","), nil
}

func main() {
	promlogConfig := promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, &promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(&promlogConfig)
	kingpin.Parse()
	l, err := net.Listen("tcp", *listenServerAddress)
	if err != nil {
		level.Error(logger).Log("error", err)
		os.Exit(1)
	}
	tokens, err := getAuthTokens()
	if err != nil {
		level.Error(logger).Log("msg", "bad token args", "error", err)
		os.Exit(1)
	}
	s := &server{
		l:       l,
		lg:      logger,
		remotes: map[string]*Coordinator{},
		tokens:  tokens,
	}
	s.lg.Log("msg", fmt.Sprintf("handle proxyc request on %s", *listenServerAddress))
	ha := newHttpHandler(s, log.NewLogfmtLogger(os.Stdout))
	go func() {
		s.lg.Log("msg", fmt.Sprintf("handle prometheus request on %s", *listenPxyAddress))
		http.ListenAndServe(*listenPxyAddress, ha)
	}()
	s.StartServe()
}
