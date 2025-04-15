package model

import (
	"context"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model/dao"
	cmclient "gitlab.senseauto.com/apcloud/library/common-go/client"
	"gorm.io/gorm"
)

type FieldTestTaskModel interface {
	GetMilesNumByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetMilesNumByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetMilesNumByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

	GetTicketNumByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetTicketNumByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetTicketNumByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

	GetTicketAvgExecTimeByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetTicketAvgExecTimeByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)
	GetTicketAvgExecTimeByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error)

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

type fieldTestTaskModelImpl struct {
	db *gorm.DB
}

func NewFieldTestTaskModel() FieldTestTaskModel {
	return &fieldTestTaskModelImpl{db: cmclient.SQLDB.DB}
}

func (m *fieldTestTaskModelImpl) GetMilesNumByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//query := `
	//SELECT
	//DATE(FROM_UNIXTIME(update_at / 1000)) AS time,
	//SUM(mileage) AS sum
	//FROM
	//	fieldtest_task
	//WHERE
	//	type = 1
	//	AND status IN (2, 3)
	//	AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN startDate AND endDate
	//GROUP BY time;`

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,DATE(FROM_UNIXTIME(update_at / 1000)) AS time, SUM(mileage) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("DATE(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetMilesNumByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,WEEK(FROM_UNIXTIME(update_at / 1000)) AS time, SUM(mileage) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("WEEK(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetMilesNumByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,MONTH(FROM_UNIXTIME(update_at / 1000)) AS time, SUM(mileage) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("MONTH(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetTicketNumByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,DATE(FROM_UNIXTIME(update_at / 1000)) AS time, COUNT(*) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("DATE(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetTicketNumByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,WEEK(FROM_UNIXTIME(update_at / 1000)) AS time, COUNT(*) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("WEEK(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetTicketNumByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,MONTH(FROM_UNIXTIME(update_at / 1000)) AS time, COUNT(*) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("MONTH(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetTicketAvgExecTimeByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,DATE(FROM_UNIXTIME(update_at / 1000)) AS time, avg((end_time - start_time) / 3600000) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("DATE(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil

}

func (m *fieldTestTaskModelImpl) GetTicketAvgExecTimeByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,WEEK(FROM_UNIXTIME(update_at / 1000)) AS time, avg((end_time - start_time) / 3600000) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("WEEK(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetTicketAvgExecTimeByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task").Select("YEAR(FROM_UNIXTIME(update_at / 1000)) AS year,MONTH(FROM_UNIXTIME(update_at / 1000)) AS time, avg((end_time - start_time) / 3600000) AS num").
		Where("type = ? AND status IN ? AND DATE(FROM_UNIXTIME(update_at / 1000)) BETWEEN ? AND ?",
			1, []int{1, 2, 3}, request.StartTime, request.EndTime)

	if request.Project != 0 {
		tx = tx.Where("project_id = ?", request.Project)
	}
	tx = tx.Group("YEAR(FROM_UNIXTIME(update_at / 1000))").Group("MONTH(FROM_UNIXTIME(update_at / 1000))")

	if err := tx.Scan(&partitionStatusRecord).Error; err != nil {
		return nil, err
	}
	return partitionStatusRecord, nil
}

func (m *fieldTestTaskModelImpl) GetSingleCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetSingleCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetSingleCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(duration)/3600 as duration,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetSingleCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(mileage) as miles,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetSingleCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(mileage)as miles,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetSingleCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	subQuery := tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(miles) as duration,vehicle").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectDurationByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectDurationByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectDurationByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(duration)/3600 as duration").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectMilesByDateAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	//区分
	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,DATE(collection_at) as time, sum(mileage) as miles").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectMilesByWeekAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,WEEK(collection_at) as time, sum(mileage)as miles").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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

func (m *fieldTestTaskModelImpl) GetCollectMilesByMonthAndProject(ctx context.Context, request dao.FieldTestMilesResquest) ([]*dao.FieldTestMilesResponse, error) {
	tx := cmclient.SQLDB.Extract(ctx, m.db)
	var partitionStatusRecord []*dao.FieldTestMilesResponse

	tx = tx.Table("fieldtest_task_result").Select("YEAR(collection_at) as year,MONTH(collection_at) as time, sum(miles) as duration").
		Where("meta_type = ? AND DATE(collection_at) BETWEEN ? AND ?",
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
