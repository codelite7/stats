package models

type PipelineAggregateResult struct {
	TotalRecordsHandled int64 `json:"totalRecordsHandled,string" bson:"total_records_handled"`
	TotalBytesHandled   int64 `json:"totalBytesHandled,string" bson:"total_bytes_handled"`
	TotalNumErrors      int64 `json:"totalNumErrors,string" bson:"total_num_errors"`
	TotalNumRetries     int64 `json:"totalNumRetries,string" bson:"total_num_retries"`
}
