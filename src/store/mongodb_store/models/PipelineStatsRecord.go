package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type PipelineStats struct {
	InternalId     primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	PipelineId     string             `json:"pipelineId,omitempty" bson:"pipeline_id"`
	Timestamp      int64              `json:"timestamp,string" bson:"timestamp"`
	RecordsHandled int64              `json:"recordsHandled,string" bson:"records_handled"`
	BytesTotal     int64              `json:"bytesTotal,string" bson:"bytes_total"`
	NumErrors      int64              `json:"numErrors,string" bson:"num_errors"`
	NumRetries     int64              `json:"numRetries,string" bson:"num_retries"`
}
