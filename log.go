package sqlx

import (
	"context"
	
	"github.com/sirupsen/logrus"
)

var logs logger

func init() {
	logs = defaultLogger{}
}

type logger interface {
	Print(ctx context.Context, v ...interface{})
}

type defaultLogger struct {}

func (log defaultLogger) Print(ctx context.Context, v ...interface{}) {
	logrus.StandardLogger().Log(logrus.TraceLevel, v...)
}

type DisableLogger struct {}

func (log DisableLogger) Print(ctx context.Context, v ...interface{}) {}

func SetLogger(log logger) {
	logs = log
}
