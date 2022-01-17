package mongodb_store

import (
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"github.com/swarm-io/app-utils-go/logging"
	sentryutils "github.com/swarm-io/app-utils-go/sentry"
	driver "github.com/swarm-io/client-mongodb"
	statsv1alpha1 "github.com/swarm-io/protos-stats/gen/proto/go/stats/v1alpha1"
	"github.com/swarm-io/stats/src/config"
	"github.com/swarm-io/stats/src/store/mongodb_store/models"
	"github.com/swarm-io/stats/src/store/mongodb_store/queries"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/protobuf/encoding/protojson"
	"time"
)

type MongodbStore struct{}

func (s MongodbStore) Initialize() (func(), error) {
	driver.InitMongoClient()
	return driver.CloseConnection, nil
}

func (s MongodbStore) GetPipelineStats(req *statsv1alpha1.GetPipelineStatsRequest) *statsv1alpha1.GetPipelineStatsResponse {
	session, err := driver.MongoClient.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())
	callback := func(sessionContext mongo.SessionContext) (result interface{}, err error) {
		defer func() {
			err = sentryutils.RecoverWithCapture(logging.Log.WithFields(nil), "error persisting pipeline stats", nil, recover())
		}()
		pipeline := queries.GetPipelineStatsQuery(req.GetIds(), req.GetFromTimestamp(), req.GetToTimestamp(), req.GetIncludeStats())
		cursor, err := getPipelineStatsCollection().Aggregate(context.Background(), pipeline)
		if err != nil {
			panic(err)
		}
		var aggResult []models.PipelineAggregateStats
		err = cursor.All(context.Background(), &aggResult)
		if err != nil {
			panic(err)
		}
		return aggResult, nil
	}
	aggResult, err := session.WithTransaction(context.Background(), callback)
	if err != nil {
		panic(err)
	}
	response := statsv1alpha1.GetPipelineStatsResponse{Stats: map[string]*statsv1alpha1.PipelineAggregateStats{}}
	for _, pipelineStats := range aggResult.([]models.PipelineAggregateStats) {
		response.TotalRecordsHandled += pipelineStats.TotalRecordsHandled
		response.TotalBytesHandled += pipelineStats.TotalBytesHandled
		response.TotalNumRetries += pipelineStats.TotalNumRetries
		response.TotalNumErrors += pipelineStats.TotalNumErrors
		pipelineStatsJson, err := json.Marshal(&pipelineStats)
		if err != nil {
			panic(err)
		}
		var responseStats statsv1alpha1.PipelineAggregateStats
		err = protojson.Unmarshal(pipelineStatsJson, &responseStats)
		if err != nil {
			panic(err)
		}
		response.Stats[pipelineStats.PipelineId] = &responseStats
	}
	return &response
}

func (s MongodbStore) ReportPipelineStats(req *statsv1alpha1.ReportPipelineStatsRequest) *statsv1alpha1.ReportPipelineStatsResponse {
	session, err := driver.MongoClient.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())
	callback := func(sessionContext mongo.SessionContext) (result interface{}, err error) {
		defer func() {
			err = sentryutils.RecoverWithCapture(logging.Log.WithFields(nil), "error persisting pipeline stats", nil, recover())
		}()
		protoJson, err := protojson.Marshal(req)
		if err != nil {
			panic(err)
		}
		var model models.PipelineStat
		err = json.Unmarshal(protoJson, &model)
		if err != nil {
			panic(err)
		}
		model.Timestamp = time.Now().UnixNano()
		insertResult, err := getPipelineStatsCollection().InsertOne(context.Background(), model)
		if err != nil {
			panic(err)
		}
		logging.Log.WithFields(logrus.Fields{"_id": insertResult.InsertedID.(primitive.ObjectID).Hex()}).Info("inserted document")
		return nil, err
	}
	_, err = session.WithTransaction(context.Background(), callback)
	if err != nil {
		panic(err)
	}
	return &statsv1alpha1.ReportPipelineStatsResponse{}
}

func (s MongodbStore) ClearPipelineStats(req *statsv1alpha1.ClearPipelineStatsRequest) *statsv1alpha1.ClearPipelineStatsResponse {
	session, err := driver.MongoClient.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())
	callback := func(sessionContext mongo.SessionContext) (result interface{}, err error) {
		defer func() {
			err = sentryutils.RecoverWithCapture(logging.Log.WithFields(nil), "error persisting pipeline stats", nil, recover())
		}()
		query := queries.GetClearPipelineStatsQuery(req.GetIds(), req.GetFromTimestamp(), req.GetToTimestamp())
		_, err = getPipelineStatsCollection().DeleteMany(context.Background(), query)
		if err != nil {
			panic(err)
		}
		return nil, nil
	}
	_, err = session.WithTransaction(context.Background(), callback)
	if err != nil {
		panic(err)
	}
	return &statsv1alpha1.ClearPipelineStatsResponse{}
}

func getPipelineStatsCollection() *mongo.Collection {
	return driver.MongoClient.Database(config.AccountUuid).Collection("pipeline_stats")
}
