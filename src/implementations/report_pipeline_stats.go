package implementations

import (
	"context"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/store"
)

func (s *V1Alpha1Server) ReportPipelineStats(ctx context.Context, req *statsv1alpha1.ReportPipelineStatsRequest) (*statsv1alpha1.ReportPipelineStatsResponse, error) {
	response := store.AppStore.ReportPipelineStats(req)
	return response, nil
}
