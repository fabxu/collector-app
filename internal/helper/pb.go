package helper

import (
	"fmt"
	cf_api "gitlab.senseauto.com/apcloud/library/proto/api/collector-app/v1"
)

func ValueMessageToInterface(msg *cf_api.ValueMessage) interface{} {
	switch v := msg.GetValue().(type) {
	case *cf_api.ValueMessage_StringValue:
		return v.StringValue
	case *cf_api.ValueMessage_Int32Value:
		return v.Int32Value
	case *cf_api.ValueMessage_Int64Value:
		return v.Int64Value
	case *cf_api.ValueMessage_Uint64Value:
		return v.Uint64Value
	case *cf_api.ValueMessage_FloatValue:
		return v.FloatValue
	case *cf_api.ValueMessage_DoubleValue:
		return v.DoubleValue
	case *cf_api.ValueMessage_BoolValue:
		return v.BoolValue
	default:
		return nil
	}
}

func InterfacetoValueMessage(value interface{}) (*cf_api.ValueMessage, error) {
	switch v := value.(type) {
	case string:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_StringValue{StringValue: v}}, nil
	case int32:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_Int32Value{Int32Value: v}}, nil
	case int64:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_Int64Value{Int64Value: v}}, nil
	case uint64:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_Uint64Value{Uint64Value: v}}, nil
	case float32:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_FloatValue{FloatValue: v}}, nil
	case float64:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_DoubleValue{DoubleValue: v}}, nil
	case bool:
		return &cf_api.ValueMessage{Value: &cf_api.ValueMessage_BoolValue{BoolValue: v}}, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", value)
	}
}
