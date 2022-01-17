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
	"go.mongodb.org/mongo-driver/bson"
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

func (s MongodbStore) GetPipelineStatsAggregate(req *statsv1alpha1.GetPipelineStatsAggregateRequest) *statsv1alpha1.GetPipelineStatsAggregateResponse {
	session, err := driver.MongoClient.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())
	callback := func(sessionContext mongo.SessionContext) (result interface{}, err error) {
		defer func() {
			err = sentryutils.RecoverWithCapture(logging.Log.WithFields(nil), "error persisting pipeline stats", nil, recover())
		}()
		matchStage := bson.D{{"$match", bson.D{}}}
		groupStage := bson.D{{"$group", bson.D{
			{"_id", "$pipeline_id"},
			{"total_records_handled", bson.D{{"$sum", "$records_handled"}}},
			{"total_bytes_handled", bson.D{{"$sum", "$bytes_total"}}},
			{"total_num_errors", bson.D{{"$sum", "$num_errors"}}},
			{"total_num_retries", bson.D{{"$sum", "$num_retries"}}},
		}},
		}
		pipeline := mongo.Pipeline{matchStage, groupStage}
		cursor, err := getPipelineStatsCollection().Aggregate(context.Background(), pipeline)
		if err != nil {
			panic(err)
		}
		var aggResult []models.PipelineAggregateResult
		err = cursor.All(context.Background(), &aggResult)
		if err != nil {
			panic(err)
		}
		return aggResult[0], nil
	}
	aggResult, err := session.WithTransaction(context.Background(), callback)
	if err != nil {
		panic(err)
	}
	aggResultBytes, err := json.Marshal(aggResult)
	if err != nil {
		panic(err)
	}
	var response statsv1alpha1.GetPipelineStatsAggregateResponse
	err = protojson.Unmarshal(aggResultBytes, &response)
	if err != nil {
		panic(err)
	}
	return &response
}

func (s MongodbStore) GetPipelineStatsSeries(req *statsv1alpha1.GetPipelineStatsSeriesRequest) *statsv1alpha1.GetPipelineStatsSeriesResponse {
	logging.Log.Info("GetPipelineStatsSeries Unimplemented")

	return &statsv1alpha1.GetPipelineStatsSeriesResponse{}
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
		var model models.PipelineStats
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

func getPipelineStatsCollection() *mongo.Collection {
	return driver.MongoClient.Database(config.AccountUuid).Collection("pipeline_stats")
}
