package implementations

import (
	"context"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/store"
)

func (s *V1Alpha1Server) GetPipelineStats(ctx context.Context, req *statsv1alpha1.GetPipelineStatsRequest) (*statsv1alpha1.GetPipelineStatsResponse, error) {
	response := store.AppStore.GetPipelineStats(req)
	return response, nil
}
