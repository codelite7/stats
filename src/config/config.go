package config

import (
	"github.com/google/uuid"
	"github.com/swarm-io/app-utils-go/env"
)

var AccountUuid = getAccountUuid()

func getAccountUuid() string {
	accountUuid := env.GetEnvOrDefault("ACCOUNT_UUID", "")
	uuid.MustParse(accountUuid)
	return accountUuid
}
