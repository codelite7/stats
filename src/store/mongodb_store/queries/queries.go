package queries

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/sjson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func GetPipelineStatsQuery(ids []string, from, to int64, includeStats bool) mongo.Pipeline {
	matchStage, err := sjson.Set("{}", "$match", getPipelineStatsMatchQuery(ids, from, to))
	if err != nil {
		panic(err)
	}
	groupStage, err := sjson.Set("{}", "$group", getPipelineStatsGroupQuery(includeStats))
	if err != nil {
		panic(err)
	}
	return mongo.Pipeline{
		mustGetDocumentFromQueryString(matchStage),
		mustGetDocumentFromQueryString(groupStage),
	}
}

func getPipelineStatsMatchQuery(ids []string, from, to int64) map[string]interface{} {
	var err error
	queryString := `{"$and": []}`
	timestampFilter := mustGetMapFromQueryString(fmt.Sprintf(`{"timestamp": {"$gte": %d, "$lte": %d}}`, from, to))
	queryString, err = sjson.Set(queryString, "$and.-1", timestampFilter)
	if err != nil {
		panic(err)
	}
	if len(ids) > 0 {
		idsFilter := map[string]map[string][]string{
			"pipeline_id": {
				"$in": ids,
			},
		}
		queryString, err = sjson.Set(queryString, "$and.-1", idsFilter)
	}
	return mustGetMapFromQueryString(queryString)
}

func getPipelineStatsGroupQuery(includeStats bool) map[string]interface{} {
	var err error
	queryString := `{
"_id": "$pipeline_id",
"total_records_handled": {"$sum": "$records_handled"},
"total_bytes_handled": {"$sum": "$bytes_handled"},
"total_num_errors": {"$sum": "$num_errors"},
"total_num_retries": {"$sum": "$num_retries"}
}`
	if includeStats {
		statsMap := mustGetMapFromQueryString(`{"$push": "$$ROOT"}`)
		queryString, err = sjson.Set(queryString, "stats", statsMap)
		if err != nil {
			panic(err)
		}
	}
	return mustGetMapFromQueryString(queryString)
}

// Clear
func GetClearPipelineStatsQuery(ids []string, from, to int64) bson.D {
	idsBytes, err := json.Marshal(ids)
	if err != nil {
		panic(err)
	}
	queryString := fmt.Sprintf(`{"$and": [{"pipeline_id": {"$in": %s}}, {"timestamp": {"$gte": %d, "$lte": %d}}]}`, idsBytes, from, to)
	return mustGetDocumentFromQueryString(queryString)
}

// Utils
func mustGetMapFromQueryString(query string) map[string]interface{} {
	var queryMap map[string]interface{}
	err := json.Unmarshal([]byte(query), &queryMap)
	if err != nil {
		panic(err)
	}
	return queryMap
}

func mustTransformMapToDoc(bsonM bson.M) bson.D {
	bytes, err := bson.Marshal(bsonM)
	if err != nil {
		panic(err)
	}
	var doc bson.D
	err = bson.Unmarshal(bytes, &doc)
	if err != nil {
		panic(err)
	}
	return doc
}

func mustGetDocumentFromQueryString(query string) bson.D {
	bsonMap := mustGetMapFromQueryString(query)
	doc := mustTransformMapToDoc(bsonMap)
	return doc
}
