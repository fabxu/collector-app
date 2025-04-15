package service

import (
	"context"
	"encoding/json"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/helper"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model"
	cf_api "gitlab.senseauto.com/apcloud/library/proto/api/collector-app/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

type _formService struct {
	*_baseService
	cf_api.UnsafeFormServiceServer
	FormModel model.FormModel
}

func NewFormService(ctx context.Context) cf_api.FormServiceServer {
	return &_formService{
		_baseService: newBase(ctx),
		FormModel:    model.NewFormModel(),
	}
}

func (s *_formService) CreateFormTable(
	ctx context.Context,
	req *cf_api.CreateTableRequest) (*cf_api.CollectResponse, error) {

	err := s.FormModel.CreateTable(ctx, req.Info.JsonScheme, req.Info.TableName)
	if err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}
	return &cf_api.CollectResponse{}, nil
}

func (s *_formService) DeleteFormTable(
	ctx context.Context,
	req *cf_api.DeleteTableRequest) (*cf_api.CollectResponse, error) {

	err := s.FormModel.DeleteTable(ctx, req.Info.TableName)
	if err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}
	return &cf_api.CollectResponse{}, nil
}

func (s *_formService) InsertFormData(
	ctx context.Context,
	req *cf_api.InsertDataRequest) (*cf_api.CollectResponse, error) {

	tableData := make(map[string]interface{})
	for key, anyMsg := range req.Info.Info {

		unpackedMessage := &cf_api.ValueMessage{}
		if err := anyMsg.UnmarshalTo(unpackedMessage); err != nil {
			panic(err)
		}

		result := helper.ValueMessageToInterface(unpackedMessage)
		tableData[key] = result
	}

	err := s.FormModel.InsertTableData(ctx, req.Info.Table, tableData)

	if err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}
	return &cf_api.CollectResponse{}, nil
}

func (s *_formService) InsertFormDataString(
	ctx context.Context,
	req *cf_api.InsertStringDataRequest) (*cf_api.CollectResponse, error) {

	var dataMap map[string]interface{}
	if err := json.Unmarshal([]byte(req.Info.Data), &dataMap); err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}

	err := s.FormModel.InsertTableData(ctx, req.Info.Table, dataMap)
	if err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}

	return &cf_api.CollectResponse{}, nil
}

// 暂时只支持 单个查询，原因是 protobuf 不支 repeated map
func (s *_formService) GetFormData(
	ctx context.Context,
	req *cf_api.GetDataRequest) (*cf_api.GetDataResponse, error) {

	tableData, err := s.FormModel.GetTableData(ctx, req.Table, 2)
	if err != nil {
		return &cf_api.GetDataResponse{
			Message: err.Error(),
		}, err
	}

	result := make(map[string]*anypb.Any)
	for k, v := range tableData {
		valueMessage, err := helper.InterfacetoValueMessage(v)
		if err != nil {
			panic(err)
		}
		anyValue, err := anypb.New(valueMessage)
		if err != nil {
			panic(err)
		}
		result[k] = anyValue
	}

	return &cf_api.GetDataResponse{
		Data: result,
	}, nil
}

func (s *_formService) GetFormDataString(
	ctx context.Context,
	req *cf_api.GetDataRequest) (*cf_api.GetFormDataResponse, error) {

	tableData, err := s.FormModel.GetTableData(ctx, req.Table, 2)
	if err != nil {
		return &cf_api.GetFormDataResponse{
			Message: err.Error(),
		}, err
	}

	result, err := json.Marshal(tableData)
	if err != nil {
		return &cf_api.GetFormDataResponse{
			Message: err.Error(),
		}, err
	}
	return &cf_api.GetFormDataResponse{
		Data: string(result),
	}, nil
}

func (s *_formService) DeleteFormData(
	ctx context.Context,
	req *cf_api.DeleteDataRequest) (*cf_api.CollectResponse, error) {

	err := s.FormModel.DeleteTableData(ctx, req.Table, []uint64{req.Id})
	if err != nil {
		return &cf_api.CollectResponse{
			Message: err.Error(),
		}, err
	}
	return &cf_api.CollectResponse{}, nil
}
