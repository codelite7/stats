package store

import statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"

var AppStore Store

type Store interface {
	Initialize() (deferredFunc func(), err error)
	GetPipelineStats(req *statsv1alpha1.GetPipelineStatsRequest) *statsv1alpha1.GetPipelineStatsResponse
	ClearPipelineStats(req *statsv1alpha1.ClearPipelineStatsRequest) *statsv1alpha1.ClearPipelineStatsResponse
	ReportPipelineStats(req *statsv1alpha1.ReportPipelineStatsRequest) *statsv1alpha1.ReportPipelineStatsResponse
}

func SetStore(store Store) {
	AppStore = store
}
