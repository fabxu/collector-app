package model

import (
	"context"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model/dao"
	cmclient "gitlab.senseauto.com/apcloud/library/common-go/client"
	"gorm.io/gorm"
)

type CollectTaskModel interface {
	GetSingleCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetSingleCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetSingleCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

	GetSingleCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetSingleCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetSingleCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

	GetCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

	GetCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
}

type collectTaskModelImpl struct {
	db *gorm.DB
}

func NewCollectTaskModel() CollectTaskModel {
	return &collectTaskModelImpl{db: cmclient.SQLDB.DB}
}

func (m *collectTaskModelImpl) GetSingleCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,year,time") // group 这边的写法和

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.duration)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil

}

func (m *collectTaskModelImpl) GetSingleCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,time,year")

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.duration)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetSingleCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,time,year")

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.duration)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetSingleCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(mileage) as miles,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,year,time") // group 这边的写法和

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.miles)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil

}

func (m *collectTaskModelImpl) GetSingleCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(mileage)as miles,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,time,year")

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.miles)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetSingleCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(miles) as duration,vehicle").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("vehicle,time,year")

	tx.Table("(?) rt", subQuery).Select("year,time, SUM(rt.miles)/COUNT(*) AS num").
		Group("year,time").
		Scan(&partitionStatusRecord)

	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("year,time")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("time,year")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("time,year")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(mileage) as miles").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("year,time")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(mileage)as miles").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("time,year")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *collectTaskModelImpl) GetCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(miles) as duration").
		Where("meta_type != ? AND DATE(collection_at) BETWEEN ? AND ?",
			"raw", request.StartTime, request.EndTime)
	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("time,year")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}
