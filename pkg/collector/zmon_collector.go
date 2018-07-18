package collector

import (
	"fmt"
	"net/url"
	"time"

	"golang.org/x/oauth2"
	autoscalingv2beta1 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

const (
	ZMONCheckMetric     = "zmon-check"
	zmonCheckIDLabelKey = "check-id"
	zmonQueryLabelKey   = "query"
)

type ZMONCollectorPlugin struct {
	tokenSource oauth2.TokenSource
	endpoint    *url.URL
}

func NewZMONCollectorPlugin(endpoint string, tokenSource oauth2.TokenSource) (*ZMONCollectorPlugin, error) {
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	return &ZMONCollectorPlugin{
		endpoint:    endpointURL,
		tokenSource: tokenSource,
	}, nil
}

// NewCollector initializes a new ZMON collector from the specified HPA.
func (c *ZMONCollectorPlugin) NewCollector(hpa *autoscalingv2beta1.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (Collector, error) {
	switch config.Name {
	case ZMONCheckMetric:
		return NewZMONCollector(c.endpoint, c.tokenSource, config, interval)
	}

	return nil, fmt.Errorf("metric '%s' not supported", config.Name)
}

type ZMONCollector struct {
	endpoint    *url.URL
	tokenSource oauth2.TokenSource
	interval    time.Duration
	checkID     string
	query       string
	labels      map[string]string
	metricName  string
	metricType  autoscalingv2beta1.MetricSourceType
}

func NewZMONCollector(endpoint *url.URL, tokenSource oauth2.TokenSource, config *MetricConfig, interval time.Duration) (*ZMONCollector, error) {
	checkID, ok := config.Labels[zmonCheckIDLabelKey]
	if !ok {
		return nil, fmt.Errorf("ZMON check ID not specified on metric")
	}

	// TODO: optional
	query, ok := config.Labels[zmonQueryLabelKey]
	if !ok {
		return nil, fmt.Errorf("ZMON query not specified on metric")
	}

	return &ZMONCollector{
		endpoint:    endpoint,
		tokenSource: tokenSource,
		interval:    interval,
		checkID:     checkID,
		query:       query,
		metricName:  config.Name,
		metricType:  config.Type,
		labels:      config.Labels,
	}, nil
}

// curl -H "Authorization: Bearer $(ztoken)" \
//  -H "Accept: application/json" \
//  -H "Content-Type: application/json" -X POST \
//  https://data-service.zmon.zalan.do/kairosdb-proxy/api/v1/datapoints/query \
//  -d @kariosdb_query.json -v | jq .
//
// kariosdb_query.json
// {
//   "start_relative": {
//     "value": "1",
//     "unit": "minutes"
//   },
//   "metrics": [
//     {
//       "name": "zmon.check.15131",
//       "limit": 10000,
//       "group_by": [{"name": "tag", "tags":["entity", "key"]}]
//     }
//   ]
// }
func (c *ZMONCollector) GetMetrics() ([]CollectedMetric, error) {
	metricValue := CollectedMetric{
		Type: c.metricType,
		External: external_metrics.ExternalMetricValue{
			MetricName:   c.metricName,
			MetricLabels: c.labels,
			Timestamp:    metav1.Time{Time: time.Now().UTC()},
			Value:        *resource.NewQuantity(int64(0), resource.DecimalSI),
		},
	}

	return []CollectedMetric{metricValue}, nil
}

// Interval returns the interval at which the collector should run.
func (c *ZMONCollector) Interval() time.Duration {
	return c.interval
}
