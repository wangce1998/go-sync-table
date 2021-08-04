package utils

import (
	"time"
)

const (
	TimeLayout = "2006-01-02 15:04:05"
)

func DateTime() string {
	return time.Unix(time.Now().Unix(), 0).Format(TimeLayout)
}

func FormatDateTime(t int64) string {
	return time.Unix(t, 0).Format(TimeLayout)
}

func Format(timeStr string) string {
	date, _ := time.Parse(time.RFC3339, timeStr)
	t := time.Unix(date.In(time.Local).Unix(), 0)

	return t.Format(TimeLayout)
}

func FormatTime(datetime string) int64 {
	if datetime == "0001-01-01 08:00:00" {
		return 0
	}
	loc, _ := time.LoadLocation("Local")    //获取时区
	tmp, _ := time.ParseInLocation(TimeLayout, datetime, loc)

	return tmp.Unix()
}
