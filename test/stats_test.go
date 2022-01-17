package test

import (
	"context"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/swarm-io/app-utils-go/logging"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"google.golang.org/grpc"
	"testing"
	"time"
)

var Client statsv1alpha1.StatsServiceClient
var Conn *grpc.ClientConn

type StatsTestSuite struct {
	suite.Suite
}

func (suite *StatsTestSuite) SetupSuite() {
	// initialize the Client
	connectTo := "127.0.0.1:8083"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, connectTo, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		logging.LogErrWithTrace(logging.Log.WithFields(nil), "error connecting to stats in stats", err)
	}
	Conn = conn
	Client = statsv1alpha1.NewStatsServiceClient(conn)
}

func (suite *StatsTestSuite) TearDownSuite() {
	if Conn != nil {
		Conn.Close()
	}
}

func (suite *StatsTestSuite) TearDownTest() {
	// delete everything
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(StatsTestSuite))
}

func (suite *StatsTestSuite) TestReportPipelineStats() {
	mustReportRandomPipelineStats(suite.T(), uuid.New().String())
}

func (suite *StatsTestSuite) TestGetPipelineStatsAggregateDefaults() {
	total := mustReportRandomPipelineStats(suite.T(), uuid.New().String())
	req := &statsv1alpha1.GetPipelineStatsAggregateRequest{
		Ids:  nil,
		From: 0,
		To:   0,
	}
	response, err := Client.GetPipelineStatsAggregate(context.Background(), req)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), total.GetRecordsHandled(), response.GetTotalRecordsHandled())
	require.Equal(suite.T(), total.GetBytesTotal(), response.GetTotalBytesHandled())
	require.Equal(suite.T(), total.GetNumErrors(), response.GetTotalNumErrors())
	require.Equal(suite.T(), total.GetNumRetries(), response.GetTotalNumRetries())
}

func getRandomReportPipelineStatsRequest(pipelineId string) statsv1alpha1.ReportPipelineStatsRequest {
	return statsv1alpha1.ReportPipelineStatsRequest{
		PipelineId:     pipelineId,
		RecordsHandled: int64(gofakeit.Number(1, 100)),
		BytesTotal:     int64(gofakeit.Number(1, 100)),
		NumErrors:      int64(gofakeit.Number(1, 100)),
		NumRetries:     int64(gofakeit.Number(1, 100)),
	}
}

func mustReportRandomStat(t *testing.T, pipelineId string) statsv1alpha1.ReportPipelineStatsRequest {
	request := getRandomReportPipelineStatsRequest(pipelineId)
	_, err := Client.ReportPipelineStats(context.Background(), &request)
	require.NoError(t, err)
	return request
}

func mustReportRandomPipelineStats(t *testing.T, pipelineId string) *statsv1alpha1.ReportPipelineStatsRequest {
	total := &statsv1alpha1.ReportPipelineStatsRequest{}
	numStats := gofakeit.Number(3, 10)
	for i := 0; i < numStats; i++ {
		req := mustReportRandomStat(t, pipelineId)
		total.RecordsHandled += req.RecordsHandled
		total.BytesTotal += req.BytesTotal
		total.NumErrors += req.NumErrors
		total.NumRetries += req.NumRetries
	}
	return total
}
