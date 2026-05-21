package cronutil

import (
	"time"

	"github.com/robfig/cron/v3"
)

// Parser 是标准 cron 表达式解析器，支持 5 字段表达式和 descriptor。
var Parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextRunTime 解析 cron 表达式，并返回给定时间之后的下一次运行时间。
func NextRunTime(expr string, after time.Time) (time.Time, error) {
	sched, err := Parser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(after), nil
}
