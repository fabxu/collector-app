package service

import (
	"context"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model/dao"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/service/util"
	cmerror "gitlab.senseauto.com/apcloud/library/common-go/error"
	cf_api "gitlab.senseauto.com/apcloud/library/proto/api/collector-app/v1"
)

type _collectService struct {
	*_baseService
	cf_api.UnsafeCollectServiceServer
	CollectModel model.CollectTaskModel
}

// 搞清楚 时间问题

func NewCollectService(ctx context.Context) cf_api.CollectServiceServer {
	return &_collectService{
		_baseService: newBase(ctx),
		CollectModel: model.NewCollectTaskModel(),
	}
}

func (s *_collectService) GetSingleCollectTime(
	ctx context.Context,
	req *cf_api.GetIndicatorRequest) (*cf_api.GetIndicatorResponse, error) {

	var data []*dao.FieldTestMilesResponse
	var err error
	if len(req.TimeRange) != 2 {
		return nil, cmerror.BadRequest
	}
	request := dao.FieldTestMilesResquest{
		StartTime: req.TimeRange[0],
		EndTime:   req.TimeRange[1],
		Project:   projectMap[req.Project],
	}

	if req.TimeType == SELECT_BY_DATE {
		data, err = s.CollectModel.GetSingleCollectDurationByDateAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_WEEK {
		request.StartTime, request.EndTime = util.WeekTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetSingleCollectDurationByWeekAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_MONTH {
		request.StartTime, request.EndTime = util.MonthTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetSingleCollectDurationByMonthAndProject(ctx, request)
	}

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	indicatorList, err := s._baseService.Convert2IndicatorList(data)

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	return &cf_api.GetIndicatorResponse{
		Data: &cf_api.GetIndicatorResponse_Data{List: indicatorList},
	}, nil
}

func (s *_collectService) GetSingleCollectMiles(
	ctx context.Context,
	req *cf_api.GetIndicatorRequest) (*cf_api.GetIndicatorResponse, error) {

	var data []*dao.FieldTestMilesResponse
	var err error
	if len(req.TimeRange) != 2 {
		return nil, cmerror.BadRequest
	}
	request := dao.FieldTestMilesResquest{
		StartTime: req.TimeRange[0],
		EndTime:   req.TimeRange[1],
		Project:   projectMap[req.Project],
	}

	if req.TimeType == SELECT_BY_DATE {
		data, err = s.CollectModel.GetSingleCollectMilesByDateAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_WEEK {
		request.StartTime, request.EndTime = util.WeekTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetSingleCollectMilesByWeekAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_MONTH {
		request.StartTime, request.EndTime = util.MonthTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetSingleCollectMilesByMonthAndProject(ctx, request)
	}

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	indicatorList, err := s._baseService.Convert2IndicatorList(data)

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	return &cf_api.GetIndicatorResponse{
		Data: &cf_api.GetIndicatorResponse_Data{List: indicatorList},
	}, nil
}

func (s *_collectService) GetCollectMiles(
	ctx context.Context,
	req *cf_api.GetIndicatorRequest) (*cf_api.GetIndicatorResponse, error) {

	var data []*dao.FieldTestMilesResponse
	var err error
	if len(req.TimeRange) != 2 {
		return nil, cmerror.BadRequest
	}
	request := dao.FieldTestMilesResquest{
		StartTime: req.TimeRange[0],
		EndTime:   req.TimeRange[1],
		Project:   projectMap[req.Project],
	}

	if req.TimeType == SELECT_BY_DATE {
		data, err = s.CollectModel.GetCollectDurationByDateAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_WEEK {
		request.StartTime, request.EndTime = util.WeekTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetCollectDurationByWeekAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_MONTH {
		request.StartTime, request.EndTime = util.MonthTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetCollectDurationByMonthAndProject(ctx, request)
	}

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	indicatorList, err := s._baseService.Convert2IndicatorList(data)

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	return &cf_api.GetIndicatorResponse{
		Data: &cf_api.GetIndicatorResponse_Data{List: indicatorList},
	}, nil
}

func (s *_collectService) GetCollectTime(
	ctx context.Context,
	req *cf_api.GetIndicatorRequest) (*cf_api.GetIndicatorResponse, error) {

	var data []*dao.FieldTestMilesResponse
	var err error
	if len(req.TimeRange) != 2 {
		return nil, cmerror.BadRequest
	}
	request := dao.FieldTestMilesResquest{
		StartTime: req.TimeRange[0],
		EndTime:   req.TimeRange[1],
		Project:   projectMap[req.Project],
	}

	if req.TimeType == SELECT_BY_DATE {
		data, err = s.CollectModel.GetCollectMilesByDateAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_WEEK {
		request.StartTime, request.EndTime = util.WeekTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetCollectMilesByWeekAndProject(ctx, request)
	} else if req.TimeType == SELECT_BY_MONTH {
		request.StartTime, request.EndTime = util.MonthTime2DateTime(request.StartTime, request.EndTime)
		data, err = s.CollectModel.GetCollectMilesByMonthAndProject(ctx, request)
	}

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	indicatorList, err := s._baseService.Convert2IndicatorList(data)

	if err != nil {
		return nil, cmerror.ErrAccessMySQL.WithError(err)
	}

	return &cf_api.GetIndicatorResponse{
		Data: &cf_api.GetIndicatorResponse_Data{List: indicatorList},
	}, nil
}
