package service

//nolint:gosec
import (
	"context"
	"database/sql"
	"gitlab.senseauto.com/apcloud/app/collector-app/global"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/model/dao"
	cf_api "gitlab.senseauto.com/apcloud/library/proto/api/collector-app/v1"
	"strings"
)

type _baseService struct {
	db *sql.DB
}

func newBase(ctx context.Context) *_baseService {
	return &_baseService{
		db: global.MYSQLDB,
	}
}

func (s *_baseService) Convert2IndicatorList(lists []*dao.FieldTestMilesResponse) ([]*cf_api.IndicatorList, error) {
	indicatorList := []*cf_api.IndicatorList{}

	for _, list := range lists {
		indicator := &cf_api.IndicatorList{
			Time: list.Time,
			Num:  list.Num,
		}
		if !strings.Contains(indicator.Time, "-") {
			indicator.Time = list.Year + "-" + indicator.Time
		}
		indicatorList = append(indicatorList, indicator)
	}

	return indicatorList, nil
}
