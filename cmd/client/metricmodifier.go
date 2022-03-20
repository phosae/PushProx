package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"net/http"

	clientmodel "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type metricModifier struct {
	addonLabelPairs map[string]string
}

func (mh *metricModifier) injectLabelParis(rsp *http.Response) error {
	var err error
	var inputMetrics []byte
	var reader io.ReadCloser
	var isGzip bool

	switch rsp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(rsp.Body)
		isGzip = true
	default:
		reader = rsp.Body
	}
	defer reader.Close()
	if inputMetrics, err = ioutil.ReadAll(reader); err != nil {
		return err
	}

	// enforce label pairs for prom metrics
	metrics, err := decodeMetric(bytes.NewReader(inputMetrics), expfmt.FmtText)
	if err != nil {
		return err
	}

	appendLabelPairIfAbsent(metrics, mh.addonLabelPairs)

	var newMets bytes.Buffer
	var w io.Writer = &newMets
	if isGzip {
		w = gzip.NewWriter(&newMets)
	}
	metricEnc := expfmt.NewEncoder(w, expfmt.FmtText)
	for i := range metrics {
		err = metricEnc.Encode(metrics[i])
		if err != nil {
			return err
		}
	}
	if gw, ok := w.(io.WriteCloser); ok {
		gw.Close()
	}

	rsp.Body = ioutil.NopCloser(&newMets)
	rsp.ContentLength = int64(newMets.Len())
	return nil
}

func decodeMetric(input io.Reader, format expfmt.Format) (ret []*clientmodel.MetricFamily, err error) {
	dec := expfmt.NewDecoder(input, format)

	for {
		var met clientmodel.MetricFamily
		if err = dec.Decode(&met); err == nil {
			ret = append(ret, &met)
		} else if err == io.EOF {
			return ret, nil
		} else {
			return nil, err
		}
	}
}

func appendLabelPairIfAbsent(mfs []*clientmodel.MetricFamily, pairs map[string]string) {
	var stringP = func(s string) *string {
		return &s
	}
	var appendTo = func(inputLabels []*clientmodel.LabelPair) []*clientmodel.LabelPair {
		var provided = map[string]struct{}{}
		var ret []*clientmodel.LabelPair = inputLabels

		for i := range inputLabels {
			provided[inputLabels[i].GetName()] = struct{}{}
		}

		for elk, elv := range pairs {
			if _, ok := provided[elk]; !ok {
				ret = append(ret, &clientmodel.LabelPair{
					Name:  stringP(elk),
					Value: stringP(elv),
				})
			}
		}
		return ret
	}

	for _, mf := range mfs {
		for _, met := range mf.Metric {
			met.Label = appendTo(met.Label)
		}
	}
}
