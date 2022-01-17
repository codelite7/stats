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
	clearStats()
}

func (suite *StatsTestSuite) TearDownSuite() {
	if Conn != nil {
		Conn.Close()
	}
}

func (suite *StatsTestSuite) TearDownTest() {
	clearStats()
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(StatsTestSuite))
}

func (suite *StatsTestSuite) TestReportPipelineStats() {
	mustReportRandomPipelineStats(suite.T(), uuid.New().String())
}

func (suite *StatsTestSuite) TestGetPipelineStats() {
	id1 := uuid.New().String()
	id2 := uuid.New().String()
	total1 := mustReportRandomPipelineStats(suite.T(), id1)
	total2 := mustReportRandomPipelineStats(suite.T(), id2)
	totalRecordsHandled := total1.GetRecordsHandled() + total2.GetRecordsHandled()
	totalBytesHandled := total1.GetBytesHandled() + total2.GetBytesHandled()
	totalNumErrors := total1.GetNumErrors() + total2.GetNumErrors()
	totalNumRetries := total1.GetNumRetries() + total2.GetNumRetries()
	req := &statsv1alpha1.GetPipelineStatsRequest{
		Ids:           nil,
		IncludeStats:  true,
		FromTimestamp: 0,
		ToTimestamp:   time.Now().UnixNano(),
	}
	response, err := Client.GetPipelineStats(context.Background(), req)
	require.NoError(suite.T(), err)
	validatePipelineStats(suite.T(), totalRecordsHandled, totalBytesHandled, totalNumErrors, totalNumRetries, []*statsv1alpha1.ReportPipelineStatsRequest{total1, total2}, response)
}

func validatePipelineStats(t *testing.T, totalRecords, totalBytes, totalErrors, totalRetries int64, expectedStats []*statsv1alpha1.ReportPipelineStatsRequest, response *statsv1alpha1.GetPipelineStatsResponse) {
	require.Len(t, response.GetStats(), len(expectedStats))
	require.Equal(t, totalRecords, response.GetTotalRecordsHandled())
	require.Equal(t, totalBytes, response.GetTotalBytesHandled())
	require.Equal(t, totalErrors, response.GetTotalNumErrors())
	require.Equal(t, totalRetries, response.GetTotalNumRetries())
	for _, expectedStat := range expectedStats {
		pipelineStats := response.Stats[expectedStat.GetPipelineId()]
		require.NotNil(t, pipelineStats)
		require.Equal(t, expectedStat.GetRecordsHandled(), pipelineStats.TotalRecordsHandled)
		require.Equal(t, expectedStat.GetBytesHandled(), pipelineStats.TotalBytesHandled)
		require.Equal(t, expectedStat.GetNumErrors(), pipelineStats.TotalNumErrors)
		require.Equal(t, expectedStat.GetNumRetries(), pipelineStats.TotalNumRetries)
	}
}

func getRandomReportPipelineStatsRequest(pipelineId string) statsv1alpha1.ReportPipelineStatsRequest {
	return statsv1alpha1.ReportPipelineStatsRequest{
		PipelineId:     pipelineId,
		RecordsHandled: int64(gofakeit.Number(1, 100)),
		BytesHandled:   int64(gofakeit.Number(1, 100)),
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
	total := &statsv1alpha1.ReportPipelineStatsRequest{PipelineId: pipelineId}
	numStats := gofakeit.Number(3, 10)
	for i := 0; i < numStats; i++ {
		req := mustReportRandomStat(t, pipelineId)
		total.RecordsHandled += req.RecordsHandled
		total.BytesHandled += req.BytesHandled
		total.NumErrors += req.NumErrors
		total.NumRetries += req.NumRetries
	}
	return total
}

func clearStats() {
	if Client != nil {
		// delete everything
		stats, err := Client.GetPipelineStats(context.Background(), &statsv1alpha1.GetPipelineStatsRequest{ToTimestamp: time.Now().UnixNano()})
		if err != nil {
			logging.LogErrWithTrace(logging.Log.WithFields(nil), "error getting stats in teardown", err)
		}
		ids := []string{}
		for id, _ := range stats.GetStats() {
			ids = append(ids, id)
		}
		if len(ids) > 0 {
			clearRequest := &statsv1alpha1.ClearPipelineStatsRequest{
				Ids:           ids,
				FromTimestamp: 0,
				ToTimestamp:   time.Now().UnixNano(),
			}
			_, err = Client.ClearPipelineStats(context.Background(), clearRequest)
			if err != nil {
				logging.LogErrWithTrace(logging.Log.WithFields(nil), "error clearing stats in teardown", err)
			}
		}
	}
}
