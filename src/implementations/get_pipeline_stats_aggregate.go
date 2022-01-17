package implementations

import (
	"context"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/store"
)

func (s *V1Alpha1Server) GetPipelineStatsAggregate(ctx context.Context, req *statsv1alpha1.GetPipelineStatsAggregateRequest) (*statsv1alpha1.GetPipelineStatsAggregateResponse, error) {
	response := store.AppStore.GetPipelineStatsAggregate(req)
	return response, nil
}
