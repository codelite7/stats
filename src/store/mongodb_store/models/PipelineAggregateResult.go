package models

type PipelineAggregateStats struct {
	PipelineId          string         `json:"pipelineId" bson:"_id"` // only used in aggregate queries where we specify _id as a field reference
	Stats               []PipelineStat `json:"stats"`
	TotalRecordsHandled int64          `json:"totalRecordsHandled,string" bson:"total_records_handled"`
	TotalBytesHandled   int64          `json:"totalBytesHandled,string" bson:"total_bytes_handled"`
	TotalNumErrors      int64          `json:"totalNumErrors,string" bson:"total_num_errors"`
	TotalNumRetries     int64          `json:"totalNumRetries,string" bson:"total_num_retries"`
}
