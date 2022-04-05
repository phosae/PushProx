package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
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
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

var (
	proxyAddr       = kingpin.Flag("proxy-addr", "Addresses of proxy servers(example.com:7080,example.com:7081), multiple address are split by comma.").Default("127.1:7080").String()
	authToken       = kingpin.Flag("auth-token", "Authorization token used to create keys to be sent to the server.").Default("").String()
	myFqdn          = kingpin.Flag("fqdn", "FQDN to register with").String()
	metricEndpoints = kingpin.Flag("metrics", "Metric endpoints of processes wait for scraping(http://127.1:8999/metrics,http://127.0.0.1:8900/metrics), multiple endpoints are split by comma.").String()
	labelPairs      = kingpin.Flag("label-pairs", "Label pairs add to prometheus metrics if not specified(i.e node=my-node,region=shanghai)").String()
	configFile      = kingpin.Flag("config", "Config file of proxy client, arguments in file takes priority over command arguments(i.e ./pushproxc.yaml").Short('f').String()
)

type Endpoint struct {
	Name string   `yaml:"name,omitempty"`
	URL  *url.URL `yaml:"url"`
}

type Config struct {
	// Token specifies the authorization token used to create keys to be sent to the server.
	Token string `yaml:"token,omitempty"`
	// ProxyAddr are addresses of proxy servers(example.com:8080,example.com:8081), multiple address are split by comma.
	ProxyAddr string `yaml:"proxy-addr"`
	FQDN      string `yaml:"fqdn,omitempty"`
	// Eps are metric endpoints contains process address(ip:port) and path(i.e /metrics)
	Eps []Endpoint `yaml:"metrics"`
	// LabelPairs add to prometheus metrics if not specified(i.e node=my-node,region=shanghai)
	LabelPairs map[string]string `yaml:"label-pairs,omitempty"`

	transport   http.RoundTripper
	rspModifier func(*http.Response) error
	logger      log.Logger
}

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
			c.Eps[i].Name = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%s%s", c.Eps[i].URL.Host, c.Eps[i].URL.Path)))
		}
	}
}

func mustLoadConf() *Config {
	var conf Config
	conf.ProxyAddr = *proxyAddr
	conf.Token = *authToken
	if *myFqdn != "" {
		conf.FQDN = *myFqdn
	}
	if *metricEndpoints != "" {
		for _, ep := range strings.Split(*metricEndpoints, ",") {
			URL, err := url.Parse(strings.TrimSpace(ep))
			if err != nil {
				panic(fmt.Sprintf("invalid metricEndpoints: %v", err))
			}
			conf.Eps = append(conf.Eps, Endpoint{URL: URL})
		}
	}
	if *labelPairs != "" {
		for _, labelPair := range strings.Split(*labelPairs, ",") {
			parts := strings.SplitN(labelPair, "=", 2)
			if len(parts) != 2 {
				panic(fmt.Errorf("expected LABLE=VALUE got '%s'", labelPair))
			}
			if conf.LabelPairs == nil {
				conf.LabelPairs = map[string]string{}
			}
			conf.LabelPairs[parts[0]] = parts[1]
		}
	}
	if *configFile != "" {
		b, err := ioutil.ReadFile(*configFile)
		if err != nil {
			panic(err)
		}
		err = yaml.Unmarshal(b, &conf)
		if err != nil {
			panic(fmt.Sprintf("err parse conf: %v", err))
		}
	}
	return &conf
}

func main() {
	promlogConfig := promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, &promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	lg := promlog.New(&promlogConfig)
	kingpin.Parse()
	kingpin.Parse()
	var conf = mustLoadConf()
	conf.complete()

	if len(conf.LabelPairs) > 0 {
		conf.rspModifier = (&metricModifier{addonLabelPairs: conf.LabelPairs}).injectLabelParis
	}
	conf.logger = log.With(lg, "from", "Coordinator")

	c, err := NewCoordinator(conf)
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
