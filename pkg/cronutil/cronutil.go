package cronutil

import (
	"time"

	"github.com/robfig/cron/v3"
)

// Parser is a standard cron expression parser supporting 5-field + descriptors.
var Parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextRunTime parses the cron expression and returns the next run time after the given time.
func NextRunTime(expr string, after time.Time) (time.Time, error) {
	sched, err := Parser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(after), nil
}
