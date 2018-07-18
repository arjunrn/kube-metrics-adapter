/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/cmd/server"
	"github.com/mikkeloscar/kube-metrics-adapter/pkg/collector"
	"github.com/mikkeloscar/kube-metrics-adapter/pkg/provider"
	"github.com/spf13/cobra"
	"github.com/zalando-incubator/cluster-lifecycle-manager/pkg/credentials-loader/platformiam"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewCommandStartAdapterServer provides a CLI handler for 'start adapter server' command
func NewCommandStartAdapterServer(out, errOut io.Writer, stopCh <-chan struct{}) *cobra.Command {
	baseOpts := server.NewCustomMetricsAdapterServerOptions(out, errOut)
	o := AdapterServerOptions{
		CustomMetricsAdapterServerOptions: baseOpts,
		EnableCustomMetricsAPI:            true,
		EnableExternalMetricsAPI:          true,
		ZMONTokenName:                     "zmon",
		CredentialsDir:                    "/meta/credentials",
	}

	cmd := &cobra.Command{
		Short: "Launch the custom metrics API adapter server",
		Long:  "Launch the custom metrics API adapter server",
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(args); err != nil {
				return err
			}
			if err := o.RunCustomMetricsAdapterServer(stopCh); err != nil {
				return err
			}
			return nil
		},
	}

	flags := cmd.Flags()
	o.SecureServing.AddFlags(flags)
	o.Authentication.AddFlags(flags)
	o.Authorization.AddFlags(flags)
	o.Features.AddFlags(flags)

	flags.StringVar(&o.RemoteKubeConfigFile, "lister-kubeconfig", o.RemoteKubeConfigFile, ""+
		"kubeconfig file pointing at the 'core' kubernetes server with enough rights to list "+
		"any described objects")
	flags.BoolVar(&o.EnableCustomMetricsAPI, "enable-custom-metrics-api", o.EnableCustomMetricsAPI, ""+
		"whether to enable Custom Metrics API")
	flags.BoolVar(&o.EnableExternalMetricsAPI, "enable-external-metrics-api", o.EnableExternalMetricsAPI, ""+
		"whether to enable External Metrics API")
	flags.StringVar(&o.PrometheusServer, "prometheus-server", o.PrometheusServer, ""+
		"url of prometheus server to query")
	flags.StringVar(&o.ZMONKariosDBServer, "zmon-kariosdb-server", o.ZMONKariosDBServer, ""+
		"url of ZMON KariosDB server to query for ZMON checks")
	flags.StringVar(&o.ZMONTokenName, "zmon-token-name", o.ZMONTokenName, ""+
		"name of the token used to query ZMON")
	flags.StringVar(&o.CredentialsDir, "credentials-dir", o.CredentialsDir, ""+
		"path to the credentials dir where tokens are stored")
	flags.BoolVar(&o.SkipperIngressMetrics, "skipper-ingress-metrics", o.SkipperIngressMetrics, ""+
		"whether to enable skipper ingress metrics")
	flags.BoolVar(&o.AWSExternalMetrics, "aws-external-metrics", o.AWSExternalMetrics, ""+
		"whether to enable AWS external metrics")

	return cmd
}

func (o AdapterServerOptions) RunCustomMetricsAdapterServer(stopCh <-chan struct{}) error {
	config, err := o.Config()
	if err != nil {
		return err
	}

	var clientConfig *rest.Config
	if len(o.RemoteKubeConfigFile) > 0 {
		loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: o.RemoteKubeConfigFile}
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

		clientConfig, err = loader.ClientConfig()
	} else {
		clientConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return fmt.Errorf("unable to construct lister client config to initialize provider: %v", err)
	}

	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize new client: %v", err)
	}

	collectorFactory := collector.NewCollectorFactory()

	if o.PrometheusServer != "" {
		promPlugin, err := collector.NewPrometheusCollectorPlugin(client, o.PrometheusServer)
		if err != nil {
			return fmt.Errorf("failed to initialize prometheus collector plugin: %v", err)
		}

		err = collectorFactory.RegisterObjectCollector("", "prometheus", promPlugin)
		if err != nil {
			return fmt.Errorf("failed to register prometheus collector plugin: %v", err)
		}

		// skipper collector can only be enabled if prometheus is.
		if o.SkipperIngressMetrics {
			skipperPlugin, err := collector.NewSkipperCollectorPlugin(client, promPlugin)
			if err != nil {
				return fmt.Errorf("failed to initialize skipper collector plugin: %v", err)
			}

			err = collectorFactory.RegisterObjectCollector("Ingress", "", skipperPlugin)
			if err != nil {
				return fmt.Errorf("failed to register skipper collector plugin: %v", err)
			}
		}
	}

	if o.ZMONKariosDBServer != "" {
		tokenSource := platformiam.NewTokenSource(o.ZMONTokenName, o.CredentialsDir)

		zmonPlugin, err := collector.NewZMONCollectorPlugin(o.ZMONKariosDBServer, tokenSource)
		if err != nil {
			return fmt.Errorf("failed to initialize ZMON collector plugin: %v", err)
		}

		collectorFactory.RegisterExternalCollector([]string{collector.ZMONCheckMetric}, zmonPlugin)
	}

	// register generic pod collector
	err = collectorFactory.RegisterPodsCollector("", collector.NewPodCollectorPlugin(client))
	if err != nil {
		return fmt.Errorf("failed to register pod collector plugin: %v", err)
	}

	if o.AWSExternalMetrics {
		sess, err := session.NewSession()
		if err != nil {
			return fmt.Errorf("failed to setup AWS session: %v", err)
		}
		collectorFactory.RegisterExternalCollector([]string{collector.AWSSQSQueueLengthMetric}, collector.NewAWSCollectorPlugin(sess))
	}

	hpaProvider := provider.NewHPAProvider(client, 30*time.Second, 1*time.Minute, collectorFactory)

	// convert stop channel to a context
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()

	go hpaProvider.Run(ctx)

	customMetricsProvider := hpaProvider
	externalMetricsProvider := hpaProvider

	// var externalMetricsProvider := nil
	if !o.EnableCustomMetricsAPI {
		customMetricsProvider = nil
	}
	if !o.EnableExternalMetricsAPI {
		externalMetricsProvider = nil
	}

	// In this example, the same provider implements both Custom Metrics API and External Metrics API
	server, err := config.Complete().New("kube-metrics-adapter", customMetricsProvider, externalMetricsProvider)
	if err != nil {
		return err
	}
	return server.GenericAPIServer.PrepareRun().Run(ctx.Done())
}

type AdapterServerOptions struct {
	*server.CustomMetricsAdapterServerOptions

	// RemoteKubeConfigFile is the config used to list pods from the master API server
	RemoteKubeConfigFile string
	// EnableCustomMetricsAPI switches on sample apiserver for Custom Metrics API
	EnableCustomMetricsAPI bool
	// EnableExternalMetricsAPI switches on sample apiserver for External Metrics API
	EnableExternalMetricsAPI bool
	// PrometheusServer enables prometheus queries to the specified
	// server
	PrometheusServer string
	// ZMONKariosDBServer enables ZMON check queries to the specified
	// kariosDB server
	ZMONKariosDBServer string
	// ZMONTokenName is the name of the token used to query ZMON
	ZMONTokenName string
	// CredentialsDir is the path to the dir where tokens are stored
	CredentialsDir string
	// SkipperIngressMetrics switches on support for skipper ingress based
	// metric collection.
	SkipperIngressMetrics bool
	// AWSExternalMetrics switches on support for getting external metrics
	// from AWS.
	AWSExternalMetrics bool
}
