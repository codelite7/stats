package implementations

import (
	"context"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/store"
)

func (s *V1Alpha1Server) ClearPipelineStats(ctx context.Context, req *statsv1alpha1.ClearPipelineStatsRequest) (*statsv1alpha1.ClearPipelineStatsResponse, error) {
	response := store.AppStore.ClearPipelineStats(req)
	return response, nil
}
