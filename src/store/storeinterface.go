package store

import statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"

var AppStore Store

type Store interface {
	Initialize() (deferredFunc func(), err error)
	GetPipelineStatsAggregate(req *statsv1alpha1.GetPipelineStatsAggregateRequest) *statsv1alpha1.GetPipelineStatsAggregateResponse
	GetPipelineStatsSeries(req *statsv1alpha1.GetPipelineStatsSeriesRequest) *statsv1alpha1.GetPipelineStatsSeriesResponse
	ReportPipelineStats(req *statsv1alpha1.ReportPipelineStatsRequest) *statsv1alpha1.ReportPipelineStatsResponse
}

func SetStore(store Store) {
	AppStore = store
}
