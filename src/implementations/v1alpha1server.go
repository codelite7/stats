package implementations

import (
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
)

// implements the ingest api
type V1Alpha1Server struct {
	statsv1alpha1.UnimplementedStatsServiceServer
}
