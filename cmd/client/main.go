package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Showmax/go-fqdn"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus-community/pushprox/util"
	"gopkg.in/yaml.v2"
)

type Endpoint struct {
	Name string `yaml:"name,omitempty"`
	Addr string `yaml:"addr"` // ip:port
	Path string `yaml:"path"`
}

func (ep Endpoint) Url() (*url.URL, error) {
	var host, port string
	var err error
	if host, port, err = net.SplitHostPort(ep.Addr); err != nil {
		return nil, err
	}
	if host == "" {
		host = "127.0.0.1"
	}

	if !strings.HasPrefix(ep.Path, "/") {
		ep.Path = "/" + ep.Path
	}
	schema := "http"
	return url.Parse(fmt.Sprintf("%s://%s:%s%s", schema, host, port, ep.Path))
}

type Config struct {
	// Token specifies the authorization token used to create keys to be sent to the server.
	Token string `yaml:"token,omitempty"`

	ProxyAddr  string            `yaml:"proxy_addr"`
	FQDN       string            `yaml:"fqdn,omitempty"`
	Eps        []Endpoint        `yaml:"metrics"`
	LabelPairs map[string]string `yaml:"label_pairs,omitempty"`

	transport   http.RoundTripper
	rspModifier func(*http.Response) error
	logger      log.Logger
}

var configFile = flag.String("f", "./pushproxc.yaml", "the Config file")

func (c *Config) complete() {
	if c.FQDN == "" {
		FQDN, err := fqdn.FqdnHostname()
		if err != nil {
			FQDN = fmt.Sprintf("rand-fqdn-%s", util.RandString(5))
		}
		c.FQDN = FQDN
	}
	for i := range c.Eps {
		if c.Eps[i].Name == "" {
			c.Eps[i].Name = util.RandString(5)
		}
	}
}

func mustLoad(path string, c interface{}) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(b, c)
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()
	var conf Config
	mustLoad(*configFile, &conf)
	conf.complete()

	lg := log.NewLogfmtLogger(os.Stdout)

	if len(conf.LabelPairs) > 0 {
		conf.rspModifier = (&metricModifier{addonLabelPairs: conf.LabelPairs}).injectLabelParis
	}
	conf.logger = log.With(lg, "from", "Coordinator")

	c, err := NewCoordinator(&conf)
	if err != nil {
		level.Error(lg).Log("NewCoordinator", err)
		os.Exit(1)
	}

	sigTerm := util.SetupSignalHandler()

	go func() {
		backoff.RetryNotify(
			func() error {
				select {
				case <-sigTerm:
					return nil
				default:
				}
				c.Start()
				return fmt.Errorf("coordinator exit abnormal")
			},
			backoff.NewExponentialBackOff(),
			func(err error, duration time.Duration) {
				level.Warn(lg).Log("err", err, "duration", duration)
			},
		)
	}()

	<-sigTerm
	c.Stop()
}
