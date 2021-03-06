package collector

import (
	"fmt"
	"strings"
	"time"

	autoscalingv2beta1 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/metrics/pkg/apis/custom_metrics"
)

const (
	rpsQuery      = `scalar(sum(rate(skipper_serve_host_duration_seconds_count{host="%s"}[1m])))`
	rpsMetricName = "requests-per-second"
)

// SkipperCollectorPlugin is a collector plugin for initializing metrics
// collectors for getting skipper ingress metrics.
type SkipperCollectorPlugin struct {
	client kubernetes.Interface
	plugin CollectorPlugin
}

// NewSkipperCollectorPlugin initializes a new SkipperCollectorPlugin.
func NewSkipperCollectorPlugin(client kubernetes.Interface, prometheusPlugin *PrometheusCollectorPlugin) (*SkipperCollectorPlugin, error) {
	return &SkipperCollectorPlugin{
		client: client,
		plugin: prometheusPlugin,
	}, nil
}

// NewCollector initializes a new skipper collector from the specified HPA.
func (c *SkipperCollectorPlugin) NewCollector(hpa *autoscalingv2beta1.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (Collector, error) {
	switch config.Name {
	case rpsMetricName:
		return NewSkipperCollector(c.client, c.plugin, hpa, config, interval)
	default:
		return nil, fmt.Errorf("metric '%s' not supported", config.Name)
	}
}

// SkipperCollector is a metrics collector for getting skipper ingress metrics.
// It depends on the prometheus collector for getting the metrics.
type SkipperCollector struct {
	client          kubernetes.Interface
	metricName      string
	objectReference custom_metrics.ObjectReference
	hpa             *autoscalingv2beta1.HorizontalPodAutoscaler
	interval        time.Duration
	plugin          CollectorPlugin
	config          MetricConfig
}

// NewSkipperCollector initializes a new SkipperCollector.
func NewSkipperCollector(client kubernetes.Interface, plugin CollectorPlugin, hpa *autoscalingv2beta1.HorizontalPodAutoscaler, config *MetricConfig, interval time.Duration) (*SkipperCollector, error) {
	return &SkipperCollector{
		client:          client,
		objectReference: config.ObjectReference,
		hpa:             hpa,
		metricName:      config.Name,
		interval:        interval,
		plugin:          plugin,
		config:          *config,
	}, nil
}

// getCollector returns a collector for getting the metrics.
func (c *SkipperCollector) getCollector() (Collector, error) {
	ingress, err := c.client.ExtensionsV1beta1().Ingresses(c.objectReference.Namespace).Get(c.objectReference.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	config := c.config

	var collector Collector
	collectors := make([]Collector, 0, len(ingress.Spec.Rules))
	for _, rule := range ingress.Spec.Rules {
		host := strings.Replace(rule.Host, ".", "_", -1)
		config.Config = map[string]string{
			"query": fmt.Sprintf(rpsQuery, host),
		}

		config.PerReplica = false // per replica is handled outside of the prometheus collector
		collector, err := c.plugin.NewCollector(c.hpa, &config, c.interval)
		if err != nil {
			return nil, err
		}

		collectors = append(collectors, collector)
	}
	if len(collectors) > 1 {
		collector = NewMaxCollector(c.interval, collectors...)
	} else if len(collectors) == 1 {
		collector = collectors[0]
	} else {
		return nil, fmt.Errorf("no hosts defined on ingress %s/%s, unable to create collector", c.objectReference.Namespace, c.objectReference.Name)
	}

	return collector, nil
}

// GetMetrics gets skipper metrics from prometheus.
func (c *SkipperCollector) GetMetrics() ([]CollectedMetric, error) {
	collector, err := c.getCollector()
	if err != nil {
		return nil, err
	}

	values, err := collector.GetMetrics()
	if err != nil {
		return nil, err
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("expected to only get one metric value, got %d", len(values))
	}

	// get current replicas for the targeted scale object. This is used to
	// calculate an average metric instead of total.
	// targetAverageValue will be available in Kubernetes v1.12
	// https://github.com/kubernetes/kubernetes/pull/64097
	replicas, err := targetRefReplicas(c.client, c.hpa)
	if err != nil {
		return nil, err
	}

	if replicas < 1 {
		return nil, fmt.Errorf("unable to get average value for %d replicas", replicas)
	}

	value := values[0]
	avgValue := float64(value.Custom.Value.MilliValue()) / float64(replicas)
	value.Custom.Value = *resource.NewMilliQuantity(int64(avgValue), resource.DecimalSI)

	return []CollectedMetric{value}, nil
}

// Interval returns the interval at which the collector should run.
func (c *SkipperCollector) Interval() time.Duration {
	return c.interval
}

func targetRefReplicas(client kubernetes.Interface, hpa *autoscalingv2beta1.HorizontalPodAutoscaler) (int32, error) {
	var replicas int32
	switch hpa.Spec.ScaleTargetRef.Kind {
	case "Deployment":
		deployment, err := client.AppsV1().Deployments(hpa.Namespace).Get(hpa.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		replicas = deployment.Status.ReadyReplicas
	case "StatefulSet":
		sts, err := client.AppsV1().StatefulSets(hpa.Namespace).Get(hpa.Spec.ScaleTargetRef.Name, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		replicas = sts.Status.ReadyReplicas
	}

	return replicas, nil
}
