/*
Copyright 2025 The Kubernetes Authors.

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

package common

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

// TODO: Add Kusto SDK imports when ready to enable real Kusto integration:
// "github.com/Azure/azure-kusto-go/kusto"

// KustoClient interface abstracts the Kusto client to allow for mock implementations
type KustoClient interface {
	Query(ctx context.Context, database string, query string) ([]KustoResult, error)
	Close() error
}

const (
	apiserverSLIMetricName = "APIServerSLILatency_kusto"
)

func init() {
	if err := measurement.Register(apiserverSLIMetricName, createAPIServerSLIMetricMeasurement); err != nil {
		klog.Fatalf("Cannot register %s: %v", apiserverSLIMetricName, err)
	}
}

func createAPIServerSLIMetricMeasurement() measurement.Measurement {
	return &apiserverSLIMetricMeasurement{
		start:   time.Now(), // record the start time when the measurement is created
		metrics: make(map[string]*MetricPoint),
	}
}

// MetricPoint represents a single metric point from Kusto
type MetricPoint struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

// KustoResult represents the result from Kusto query
type KustoResult struct {
	Timestamp   time.Time `json:"Timestamp"`
	Verb        string    `json:"verb"`
	Resource    string    `json:"resource"`
	Scope       string    `json:"scope"`
	Subresource string    `json:"subresource"`
	LeStr       string    `json:"leStr"`
	Value       float64   `json:"Value"`
}

type apiserverSLIMetricMeasurement struct {
	kustoEndpoint string
	clusterID     string
	database      string
	kustoClient   *azkustodata.Client
	metrics       map[string]*MetricPoint
	start         time.Time
	end           time.Time
}

// Dispose implements measurement.Measurement.
func (e *apiserverSLIMetricMeasurement) Dispose() {
	if e.kustoClient != nil {
		e.kustoClient.Close()
	}
}

// String implements measurement.Measurement.
func (e *apiserverSLIMetricMeasurement) String() string {
	return apiserverSLIMetricName
}

// createKustoClient creates a Kusto client with Azure CLI authentication
func (e *apiserverSLIMetricMeasurement) createKustoClient() (*azkustodata.Client, error) {
	kustoConnectionStringBuilder := azkustodata.NewConnectionStringBuilder(e.kustoEndpoint)
	kustoConnectionString := kustoConnectionStringBuilder.WithAzCli()
	client, err := azkustodata.New(kustoConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kusto client: %v", err)
	}
	return client, nil
}

// Execute supports two actions:
// - start - Records the start time for SLI metrics collection.
// - gather - Collects, gathers and prints current SLI metrics.
func (e *apiserverSLIMetricMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	switch action {
	case "start":
		// Record the start time
		e.start = time.Now()
		klog.V(2).Infof("%s: starting apiserver SLI metrics collection at %s", e, e.start.Format(time.RFC3339))

		return nil, nil

	case "gather":
		klog.V(2).Infof("%s: starting apiserver SLI collecting...", e)

		var err error
		// Get Kusto configuration from params
		e.kustoEndpoint, _ = util.GetString(config.Params, "kustoEndpoint")
		e.clusterID, _ = util.GetString(config.Params, "clusterID")
		e.database, _ = util.GetString(config.Params, "database")
		sleepDurationStr, _ := util.GetStringOrDefault(config.Params, "kustoSleep", "15m")

		if e.clusterID == "" || e.kustoEndpoint == "" || e.database == "" {
			return nil, fmt.Errorf("clusterID, kustoEndpoint or database parameters are required for gather action")
		}

		sleepDuration, err := time.ParseDuration(sleepDurationStr)

		if err != nil {
			return nil, fmt.Errorf("invalid kustoSleep duration %q: %v", sleepDurationStr, err)
		}

		// Set the end time for the query to be now minus the sleep duration since sleepDuration
		// allows for data to be fully ingested into Kusto
		e.end = time.Now().Add(-1 * sleepDuration)

		e.kustoClient, err = e.createKustoClient()
		if err != nil {
			return nil, fmt.Errorf("failed to create Kusto client: %v", err)
		}

		// Start the periodic collection
		e.collectMetrics()

		klog.V(2).Infof("%s: gathering apiserver SLI metrics from %s to %s...", e, e.start, e.end)

		// Convert collected metrics to JSON format
		metricsArray := make([]*MetricPoint, 0, len(e.metrics))
		for _, metric := range e.metrics {
			metricsArray = append(metricsArray, metric)
		}

		content, err := util.PrettyPrintJSON(metricsArray)
		if err != nil {
			return nil, err
		}

		sliSummary := measurement.CreateSummary(apiserverSLIMetricName, "json", content)
		return []measurement.Summary{sliSummary}, nil

	default:
		return nil, fmt.Errorf("unknown action %v", action)
	}
}

// collectMetrics collects metrics from Kusto and converts them to the required format
func (e *apiserverSLIMetricMeasurement) collectMetrics() {
	klog.V(5).Infof("%s: collecting metrics from Kusto...", e)

	// Mock Kusto query execution - replace this with actual Kusto SDK calls
	results, err := e.executeKustoQuery()
	if err != nil {
		klog.Errorf("%s: failed to execute Kusto query: %v", e, err)
		return
	}
	primary := results.Tables()[0]
	recs, err := query.ToStructs[KustoResult](primary)

	// Convert Kusto results to metric format
	newMetrics := make(map[string]*MetricPoint)
	for _, result := range recs {
		metricKey := fmt.Sprintf("%s_%s_%s_%s_%s_%s", result.Subresource, result.Resource, result.Verb, result.Scope, result.LeStr, result.Timestamp)

		metric := &MetricPoint{
			Metric: map[string]string{
				"__name__":    "apiserver_request_sli_duration_seconds_bucket",
				"component":   "apiserver",
				"subresource": result.Subresource,
				"le":          result.LeStr,
				"resource":    result.Resource,
				"scope":       result.Scope,
				"verb":        result.Verb,
			},
			Value: []interface{}{
				0, // timestamp placeholder
				strconv.FormatFloat(result.Value, 'f', -1, 64),
			},
		}

		newMetrics[metricKey] = metric
	}

	// Update the metrics map
	e.metrics = newMetrics

	klog.V(5).Infof("%s: collected %d metrics", e, len(newMetrics))
}

// executeKustoQuery executes the Kusto query and returns results
func (e *apiserverSLIMetricMeasurement) executeKustoQuery() (query.Dataset, error) {
	if e.kustoClient == nil {
		return nil, fmt.Errorf("Kusto client not initialized")
	}

	query := kql.New(`
ApiserverRequestSliDurationSecondsBucket1mIncrease
| where isnan(Value) == false 
| where Timestamp between (_start .. _end)
| where Labels.cluster_id == _clusterid
| extend verb = tolower(Labels.verb)
| extend resource = tolower(Labels.resource)
| extend scope = tolower(Labels.scope)
| extend subresource = tostring(Labels.subresource)
| extend leStr = tostring(Labels.le)
| project Timestamp, verb, resource, scope, subresource, leStr, Value
| summarize Value = sum(Value) by Timestamp = bin(Timestamp, 5m), verb, resource, scope, subresource, leStr
| project Timestamp, Verb=verb, Resource=resource, Scope=scope, Subresource=subresource, LeStr=leStr, Value`)
	params := kql.NewParameters().AddDateTime("_start", e.start).AddDateTime("_end", e.end).AddString("_clusterid", e.clusterID)
	klog.V(5).Infof("%s: executing Kusto query: %s", e, query.String())

	// Execute the query using our interface
	results, err := e.kustoClient.Query(context.Background(), e.database, query, azkustodata.QueryParameters(params))
	if err != nil {
		return nil, fmt.Errorf("failed to execute Kusto query: %v", err)
	}

	return results, nil
}
