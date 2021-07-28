package utils

import (
	"time"
)

const (
	FormatTemplate = "2006-01-02 15:04:05"
)

func DateTime() string {
	return time.Unix(time.Now().Unix(), 0).Format(FormatTemplate)
}

func FormatDateTime(t int64) string {
	return time.Unix(t, 0).Format(FormatTemplate)
}

func Format(timeStr string) string {
	date, _ := time.Parse(time.RFC3339, timeStr)
	t := time.Unix(date.In(time.Local).Unix(), 0)

	return t.Format(FormatTemplate)
}

func FormatTime(timeStr string) int64 {
	date, _ := time.Parse(time.RFC3339, timeStr)
	t := time.Unix(date.In(time.Local).Unix(), 0)
	str := t.Format(FormatTemplate)
	stamp, _ := time.ParseInLocation(FormatTemplate, str, time.Local)

	return stamp.Unix()
}
