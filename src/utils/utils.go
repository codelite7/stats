package utils

import (
	"encoding/json"
	"github.com/swarm-io/app-utils-go/env"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var SentryTags = map[string]string{"app": "stats", "namespace": env.GetEnvOrDefault("NAMESPACE", "local")}

func StructToProto(source interface{}, dest proto.Message) error {
	bytes, err := json.Marshal(source)
	if err != nil {
		return err
	}
	err = protojson.Unmarshal(bytes, dest)
	if err != nil {
		return err
	}
	return nil
}
