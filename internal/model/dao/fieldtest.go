package dao

import "gorm.io/datatypes"

type FieldTestTask struct {
	ID                   uint64         `json:"id" gorm:"primaryKey;type:bigint;comment:编号"`
	Name                 string         `json:"name" gorm:"type:varchar(100);index:task_idx_name;comment:任务名称"`
	Type                 int32          `json:"type" gorm:"type:int;index:task_idx_type;comment:任务类型"`
	RouteID              int64          `json:"route_id" gorm:"type:int;index:task_idx_route_id;comment:路线编号"`
	ProjectID            uint32         `json:"project_id" gorm:"type:int;index:task_idx_project_id;comment:项目编号"`
	SpaceName            string         `json:"space_name" gorm:"type:varchar(100);index:task_idx_space_name;comment:项目空间"`
	ModuleIDs            datatypes.JSON `json:"module_ids" gorm:"type:json;comment:任务模块"`
	CollectionTaskTypeID int64          `json:"collection_task_type_id" gorm:"type:int;index:task_idx_collection_task_type_id;comment:采集任务类型编号"`
	HdmapKey             string         `json:"hdmap_key" gorm:"type:varchar(100);index:task_idx_hdmap_key;comment:高精度地图编号"`
	VehicleID            datatypes.JSON `json:"vehicle_id" gorm:"type:json;comment:车辆编号"`
	DriverID             datatypes.JSON `json:"driver_id" gorm:"type:json;comment:司机编号"`
	StartTime            int64          `json:"start_time" gorm:"type:bigint;index:task_idx_start_time;comment:开始时间"`
	EndTime              int64          `json:"end_time" gorm:"type:bigint;index:task_idx_end_time;comment:结束时间"`
	Status               int32          `json:"status" gorm:"type:int;not null;index:task_idx_status;default:0;comment:状态"`
	ProjectManagerID     datatypes.JSON `json:"project_manager_id" gorm:"type:json;comment:测试员编号"`
	TagCount             int32          `gorm:"type:int;not null;default:0;comment:标签数量"`
	CustomTagCount       int32          `gorm:"type:int;not null;default:0;comment:用户标签数量"`
	Mileage              float32        `json:"mileage" gorm:"type:float;comment:里程"`
	Duration             int64          `gorm:"type:int;comment:采集时长"`
	CreatedAt            int64          `json:"created_at" gorm:"type:bigint;comment:创建时间"`
	CreatedBy            string         `json:"created_by" gorm:"type:varchar(50);comment:创建人员"`
	UpdateAt             int64          `json:"updated_at" gorm:"type:bigint;comment:修改时间"`
	UpdatedBy            string         `json:"updated_by" gorm:"type:varchar(50);comment:修改人员"`
	ProductAlgorithmType int32          `json:"product_algorithm_type" gorm:"type:int;index:task_idx_product_type;comment:测试任务产品类型"`
	RoadTaskSubType      int32          `json:"road_task_sub_type" gorm:"type:int;index:task_idx_road_test_type;comment:路测任务类型"`
}

type FieldTestMilesResquest struct {
	StartTime string
	EndTime   string
	Project   uint32
}

type FieldTestMilesResponse struct {
	Year string  `json:"year"`
	Time string  `json:"time"`
	Num  float64 `json:"num"`
}
