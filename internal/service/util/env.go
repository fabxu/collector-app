package util

import (
	"fmt"
	cmsql "gitlab.senseauto.com/apcloud/library/common-go/client/sqldb"
	"os"
)

func GetMysqlDsn(config cmsql.Config) string {
	addr := config.Addr
	if v, ok := os.LookupEnv("MYSQL_ADDRESS"); ok {
		addr = v
	}

	dbName := config.DBName
	if v, ok := os.LookupEnv("MYSQL_DATABASE"); ok {
		dbName = v
	}

	username := config.Username
	if v, ok := os.LookupEnv("MYSQL_USERNAME"); ok {
		username = v
	}

	password := config.Password
	if v, ok := os.LookupEnv("MYSQL_PASSWORD"); ok {
		password = v
	}

	protocol := config.Protocol
	param := "?charset=utf8mb4&parseTime=True&loc=Local"
	return fmt.Sprintf("%v:%v@%v(%v)/%v%v", username, password, protocol, addr, dbName, param)
}
