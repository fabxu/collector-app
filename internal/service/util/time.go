package util

import (
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/lib/constant"
	cmlib "gitlab.senseauto.com/apcloud/library/common-go/lib"
	"strconv"
	"strings"
	"time"
)

func getWeekStartEnd(year, week int) (start, end time.Time, err error) {
	// 获取年份的第一天
	firstDayOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, cmlib.GetCSTLocation())

	// 计算该年第一个星期一的日期
	offset := int(time.Monday - firstDayOfYear.Weekday())
	if offset < 0 {
		offset += 7
	}

	// 第一个星期一
	firstMonday := firstDayOfYear.Add(time.Duration(offset) * 24 * time.Hour)

	// 目标周的开始日期
	start = firstMonday.Add(time.Duration((week-1)*7) * 24 * time.Hour)
	// 目标周的结束日期
	end = start.Add(6 * 24 * time.Hour)

	return
}

func getMonthStartEnd(year, month int) (start, end time.Time, err error) {
	// 获取该月的第一天
	start = time.Date(year, time.Month(month), 1, 0, 0, 0, 0, cmlib.GetCSTLocation())

	// 计算下个月的第一天
	nextMonth := start.AddDate(0, 1, 0)
	// 上个月的最后一天是下个月的第一天前一天
	end = nextMonth.Add(-time.Second)

	return
}

func WeekTime2DateTime(startTime, endTime string) (string, string) {

	startTimeArr := strings.Split(startTime, "-")
	endTimeArr := strings.Split(endTime, "-")

	if len(startTimeArr) != 2 || len(endTimeArr) != 2 {
		return "", ""
	}

	startTimeYear, _ := strconv.Atoi(startTimeArr[0])
	startTimeWeek, _ := strconv.Atoi(startTimeArr[1])
	endTimeYear, _ := strconv.Atoi(endTimeArr[0])
	endTimeWeek, _ := strconv.Atoi(endTimeArr[1])

	startDate, _, _ := getWeekStartEnd(startTimeYear, startTimeWeek)
	_, endDate, _ := getWeekStartEnd(endTimeYear, endTimeWeek)
	return startDate.Format(constant.DateTemplate), endDate.Format(constant.DateTemplate)
}

func MonthTime2DateTime(startTime, endTime string) (string, string) {
	startTimeArr := strings.Split(startTime, "-")
	endTimeArr := strings.Split(endTime, "-")

	if len(startTimeArr) != 2 || len(endTimeArr) != 2 {
		return "", ""
	}

	startTimeYear, _ := strconv.Atoi(startTimeArr[0])
	startTimeMonth, _ := strconv.Atoi(startTimeArr[1])
	endTimeYear, _ := strconv.Atoi(endTimeArr[0])
	endTimeMonth, _ := strconv.Atoi(endTimeArr[1])

	startDate, _, _ := getMonthStartEnd(startTimeYear, startTimeMonth)
	_, endDate, _ := getMonthStartEnd(endTimeYear, endTimeMonth)
	return startDate.Format(constant.DateTemplate), endDate.Format(constant.DateTemplate)
}
