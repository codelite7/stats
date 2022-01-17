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

func (suite *StatsTestSuite) TestGetAllPipelineStats() {
	id1 := uuid.New().String()
	id2 := uuid.New().String()
	total1 := mustReportRandomPipelineStats(suite.T(), id1)
	total2 := mustReportRandomPipelineStats(suite.T(), id2)
	expectedResponse := &statsv1alpha1.GetPipelineStatsResponse{
		TotalRecordsHandled: total1.GetTotalRecordsHandled() + total2.GetTotalRecordsHandled(),
		TotalBytesHandled:   total1.GetTotalBytesHandled() + total2.GetTotalBytesHandled(),
		TotalNumErrors:      total1.GetTotalNumErrors() + total2.GetTotalNumErrors(),
		TotalNumRetries:     total1.GetTotalNumRetries() + total2.GetTotalNumRetries(),
		Stats: map[string]*statsv1alpha1.PipelineAggregateStats{
			id1: total1,
			id2: total2,
		},
	}
	req := &statsv1alpha1.GetPipelineStatsRequest{
		Ids:           nil,
		IncludeStats:  true,
		FromTimestamp: 0,
		ToTimestamp:   time.Now().UnixNano(),
	}
	response, err := Client.GetPipelineStats(context.Background(), req)
	require.NoError(suite.T(), err)
	validatePipelineStats(suite.T(), expectedResponse, response)
}

func (suite *StatsTestSuite) TestGetSinglePipelineStats() {
	id := uuid.New().String()
	total1 := mustReportRandomPipelineStats(suite.T(), id)
	expectedResponse := &statsv1alpha1.GetPipelineStatsResponse{
		TotalRecordsHandled: total1.GetTotalRecordsHandled(),
		TotalBytesHandled:   total1.GetTotalBytesHandled(),
		TotalNumErrors:      total1.GetTotalNumErrors(),
		TotalNumRetries:     total1.GetTotalNumRetries(),
		Stats: map[string]*statsv1alpha1.PipelineAggregateStats{
			id: total1,
		},
	}
	mustReportRandomPipelineStats(suite.T(), uuid.New().String())
	mustReportRandomPipelineStats(suite.T(), uuid.New().String())
	req := &statsv1alpha1.GetPipelineStatsRequest{
		Ids:           []string{id},
		IncludeStats:  true,
		FromTimestamp: 0,
		ToTimestamp:   time.Now().UnixNano(),
	}
	response, err := Client.GetPipelineStats(context.Background(), req)
	require.NoError(suite.T(), err)
	validatePipelineStats(suite.T(), expectedResponse, response)
}

func (suite *StatsTestSuite) TestGetPipelineStatsNoSeries() {
	id := uuid.New().String()
	total := mustReportRandomPipelineStats(suite.T(), id)
	total.Stats = nil
	expectedResponse := &statsv1alpha1.GetPipelineStatsResponse{
		TotalRecordsHandled: total.GetTotalRecordsHandled(),
		TotalBytesHandled:   total.GetTotalBytesHandled(),
		TotalNumErrors:      total.GetTotalNumErrors(),
		TotalNumRetries:     total.GetTotalNumRetries(),
		Stats: map[string]*statsv1alpha1.PipelineAggregateStats{
			id: total,
		},
	}
	req := &statsv1alpha1.GetPipelineStatsRequest{
		Ids:           nil,
		IncludeStats:  false,
		FromTimestamp: 0,
		ToTimestamp:   time.Now().UnixNano(),
	}
	response, err := Client.GetPipelineStats(context.Background(), req)
	require.NoError(suite.T(), err)
	validatePipelineStats(suite.T(), expectedResponse, response)
}

func (suite *StatsTestSuite) TestGetPipelineStatsInWindow() {
	id := uuid.New().String()
	// add stats before and after the time window
	mustReportRandomPipelineStats(suite.T(), id)
	fromTimestamp := time.Now().UnixNano()
	total := mustReportRandomPipelineStats(suite.T(), id)
	toTimestamp := time.Now().UnixNano()
	mustReportRandomPipelineStats(suite.T(), id)
	expectedResponse := &statsv1alpha1.GetPipelineStatsResponse{
		TotalRecordsHandled: total.GetTotalRecordsHandled(),
		TotalBytesHandled:   total.GetTotalBytesHandled(),
		TotalNumErrors:      total.GetTotalNumErrors(),
		TotalNumRetries:     total.GetTotalNumRetries(),
		Stats: map[string]*statsv1alpha1.PipelineAggregateStats{
			id: total,
		},
	}
	req := &statsv1alpha1.GetPipelineStatsRequest{
		Ids:           []string{id},
		IncludeStats:  true,
		FromTimestamp: fromTimestamp,
		ToTimestamp:   toTimestamp,
	}
	response, err := Client.GetPipelineStats(context.Background(), req)
	require.NoError(suite.T(), err)
	validatePipelineStats(suite.T(), expectedResponse, response)
}

func validatePipelineStats(t *testing.T, expected, actual *statsv1alpha1.GetPipelineStatsResponse) {
	require.Len(t, actual.GetStats(), len(expected.GetStats()))
	require.Equal(t, expected.GetTotalRecordsHandled(), actual.GetTotalRecordsHandled())
	require.Equal(t, expected.GetTotalBytesHandled(), actual.GetTotalBytesHandled())
	require.Equal(t, expected.GetTotalNumErrors(), actual.GetTotalNumErrors())
	require.Equal(t, expected.GetTotalNumRetries(), actual.GetTotalNumRetries())
	require.Len(t, actual.GetStats(), len(expected.GetStats()))
	for _, expectedStat := range expected.GetStats() {
		pipelineStats := actual.GetStats()[expectedStat.GetPipelineId()]
		require.Equal(t, expectedStat.GetTotalRecordsHandled(), pipelineStats.TotalRecordsHandled)
		require.Equal(t, expectedStat.GetTotalBytesHandled(), pipelineStats.TotalBytesHandled)
		require.Equal(t, expectedStat.GetTotalNumErrors(), pipelineStats.TotalNumErrors)
		require.Equal(t, expectedStat.GetTotalNumRetries(), pipelineStats.TotalNumRetries)
		require.Len(t, pipelineStats.GetStats(), len(expectedStat.GetStats()))
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

func mustReportRandomPipelineStats(t *testing.T, pipelineId string) *statsv1alpha1.PipelineAggregateStats {
	total := &statsv1alpha1.PipelineAggregateStats{PipelineId: pipelineId, Stats: []*statsv1alpha1.PipelineStat{}}
	numStats := gofakeit.Number(3, 10)
	for i := 0; i < numStats; i++ {
		req := mustReportRandomStat(t, pipelineId)
		total.Stats = append(total.Stats, &statsv1alpha1.PipelineStat{
			PipelineId:     pipelineId,
			RecordsHandled: req.RecordsHandled,
			BytesHandled:   req.BytesHandled,
			NumErrors:      req.NumErrors,
			NumRetries:     req.NumRetries,
		})
		total.TotalRecordsHandled += req.RecordsHandled
		total.TotalBytesHandled += req.BytesHandled
		total.TotalNumErrors += req.NumErrors
		total.TotalNumRetries += req.NumRetries
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
