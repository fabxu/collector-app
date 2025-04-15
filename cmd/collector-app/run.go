package main

import (
	"context"
	"database/sql"
	"github.com/spf13/cobra"
	"gitlab.senseauto.com/apcloud/app/collector-app/global"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/lib/constant"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/service"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/service/util"
	cmclient "gitlab.senseauto.com/apcloud/library/common-go/client"
	cmsql "gitlab.senseauto.com/apcloud/library/common-go/client/sqldb"
	cmconfig "gitlab.senseauto.com/apcloud/library/common-go/config"
	cmlog "gitlab.senseauto.com/apcloud/library/common-go/log"
	cmserver "gitlab.senseauto.com/apcloud/library/common-go/server"
	cf_api "gitlab.senseauto.com/apcloud/library/proto/api/collector-app/v1"
)

func attachRunCommand(rootCmd *cobra.Command) {
	runCmd := &cobra.Command{
		Use: "run",
		Run: func(cmd *cobra.Command, args []string) {
			logger := cmlog.New(
				// 请填写自己的服务名称，便于日志查询
				cmlog.WithAppName("app.collectorapp"),
			)

			if verbose, err := cmd.Flags().GetCount(constant.FlagVerbose); err != nil {
				logger.Panic(err)
			} else {
				logger.SetLevel(cmlog.Level(0 - verbose))
			}

			cfgFile, err := cmd.Flags().GetString(constant.FlagConfig)
			if err != nil {
				logger.Panic(err)
			}

			if err := cmconfig.Load(cfgFile); err != nil {
				logger.Panic(err)
			}

			if err := cmconfig.Global().BindPFlags(cmd.Flags()); err != nil {
				logger.Panic(err)
			}

			ctx := cmlog.Inject(context.Background(), logger)
			setups(ctx)
			setupGlobal(ctx)
			opts := []cmserver.Option{
				cmserver.WithLogger(logger),
				cmserver.AddShutdown(),
				cmserver.WithMaxRecvSize(50 * 1024 * 1024),
			}

			formService := service.NewFormService(ctx)
			fieldtestService := service.NewFieldTestService(ctx)
			collectService := service.NewCollectService(ctx)
			cmserver.New(opts...).
				ChainRPC(cmconfig.Global().GetInt(constant.CfgRPCPort),
					cmserver.RPCRegister{Register: cf_api.RegisterFormServiceServer, Server: formService},
					cmserver.RPCRegister{Register: cf_api.RegisterFieldTestServiceServer, Server: fieldtestService},
					cmserver.RPCRegister{Register: cf_api.RegisterCollectServiceServer, Server: collectService},
				).
				ChainHTTP(cmconfig.Global().GetInt(constant.CfgHTTPPort),
					cmserver.HTTPRegister{Register: cf_api.RegisterFormServiceHandlerFromEndpoint},
					cmserver.HTTPRegister{Register: cf_api.RegisterFieldTestServiceHandlerFromEndpoint},
					cmserver.HTTPRegister{Register: cf_api.RegisterCollectServiceHandlerFromEndpoint},
				).
				Run()
		},
	}
	runCmd.Flags().String(constant.FlagConfig, "conf/config.yaml", "set the path of configuration file")
	runCmd.Flags().CountP(constant.FlagVerbose, "v", "print verbose info")
	rootCmd.AddCommand(runCmd)
}

func setupGlobal(ctx context.Context) {
	var err error
	logger := cmlog.Extract(ctx)
	sqldbCfg := cmsql.Config{}
	if err = cmconfig.Global().UnmarshalKey(constant.CfgSQLDB, &sqldbCfg); err != nil {
		logger.Panic(err)
	}
	dsn := util.GetMysqlDsn(sqldbCfg)

	logger.Errorf("dsn: %s", dsn)
	global.MYSQLDB, err = sql.Open(string(sqldbCfg.DBType), dsn)
	if err != nil {
		logger.Panic(err)
	}

}
func setups(ctx context.Context) {
	logger := cmlog.Extract(ctx)

	// 初始化sqldb
	sqldbCfg := cmsql.Config{}
	if err := cmconfig.Global().UnmarshalKey(constant.CfgSQLDB, &sqldbCfg); err != nil {
		logger.Panic(err)
	}

	cmclient.SQLDB.Global(ctx, sqldbCfg)
}
