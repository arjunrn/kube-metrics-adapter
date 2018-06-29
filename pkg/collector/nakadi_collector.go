package collector

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/oauth2"
	autoscalingv2beta1 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

const (
	NakadiUnconsumedEventsMetric = "nakadi-unconsumed-events"
	NakadiConsumerLagSeconds     = "nakadi-consumer-lag-seconds"
	nakadiSubscriptionIDLabelKey = "subscription-id"
	nakadiEventTypeLabelKey      = "event-type"
	subscriptionStatsEndpoint    = "/subscriptions/%s/stats"
)

type NakadiCollectorPlugin struct {
	tokenSource oauth2.TokenSource
	endpoint    *url.URL
}

func NewNakadiCollectorPlugin(endpoint *url.URL, tokenSource oauth2.TokenSource) *NakadiCollectorPlugin {
	return &NakadiCollectorPlugin{
		endpoint:    endpoint,
		tokenSource: tokenSource,
	}
}

// NewCollector initializes a new nakadi collector from the specified HPA.
func (c *NakadiCollectorPlugin) NewCollector(hpa *autoscalingv2beta1.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (Collector, error) {
	switch config.Name {
	case NakadiUnconsumedEventsMetric, NakadiConsumerLagSeconds:
		return NewNakadiStatsCollector(c.endpoint, c.tokenSource, config, interval)
	}

	return nil, fmt.Errorf("metric '%s' not supported", config.Name)
}

type NakadiStatsCollector struct {
	endpoint       *url.URL
	tokenSource    oauth2.TokenSource
	interval       time.Duration
	subscriptionID string
	eventType      string
	labels         map[string]string
	metricName     string
	metricType     autoscalingv2beta1.MetricSourceType
}

func NewNakadiStatsCollector(endpoint *url.URL, tokenSource oauth2.TokenSource, config *MetricConfig, interval time.Duration) (*NakadiStatsCollector, error) {
	subscriptionID, ok := config.Labels[nakadiSubscriptionIDLabelKey]
	if !ok {
		return nil, fmt.Errorf("nakadi subscription id not specified on metric")
	}

	eventType, ok := config.Labels[nakadiEventTypeLabelKey]
	if !ok {
		return nil, fmt.Errorf("nakadi event type not specified on metric")
	}

	return &NakadiStatsCollector{
		endpoint:       endpoint,
		tokenSource:    tokenSource,
		interval:       interval,
		subscriptionID: subscriptionID,
		eventType:      eventType,
		metricName:     config.Name,
		metricType:     config.Type,
		labels:         config.Labels,
	}, nil
}

func (c *NakadiStatsCollector) GetMetrics() ([]CollectedMetric, error) {
	params := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.queueURL),
		AttributeNames: aws.StringSlice([]string{sqs.QueueAttributeNameApproximateNumberOfMessages}),
	}

	resp, err := c.sqs.GetQueueAttributes(params)
	if err != nil {
		return nil, err
	}

	if v, ok := resp.Attributes[sqs.QueueAttributeNameApproximateNumberOfMessages]; ok {
		i, err := strconv.Atoi(aws.StringValue(v))
		if err != nil {
			return nil, err
		}

		metricValue := CollectedMetric{
			Type: c.metricType,
			External: external_metrics.ExternalMetricValue{
				MetricName:   c.metricName,
				MetricLabels: c.labels,
				Timestamp:    metav1.Time{Time: time.Now().UTC()},
				Value:        *resource.NewQuantity(int64(i), resource.DecimalSI),
			},
		}

		return []CollectedMetric{metricValue}, nil
	}

	return nil, fmt.Errorf("failed to get queue length for '%s'", c.queueName)
}

// Interval returns the interval at which the collector should run.
func (c *NakadiStatsCollector) Interval() time.Duration {
	return c.interval
}
