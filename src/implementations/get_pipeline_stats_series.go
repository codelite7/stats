package implementations

import (
	"context"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/store"
)

func (s *V1Alpha1Server) GetPipelineStatsSeries(ctx context.Context, req *statsv1alpha1.GetPipelineStatsSeriesRequest) (*statsv1alpha1.GetPipelineStatsSeriesResponse, error) {
	response := store.AppStore.GetPipelineStatsSeries(req)
	return response, nil
}
