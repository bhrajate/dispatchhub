package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var defaultLogger *zap.SugaredLogger

func init() {
	l, _ := zap.NewProduction()
	defaultLogger = l.Sugar()
}

// Init initializes the global logger with the given level and format.
func Init(level, format, output string) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var encoder zapcore.Encoder
	if format == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	}

	var sink zapcore.WriteSyncer
	switch output {
	case "stderr":
		sink = zapcore.AddSync(os.Stderr)
	case "stdout", "":
		sink = zapcore.AddSync(os.Stdout)
	default:
		f, err := os.OpenFile(output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			sink = zapcore.AddSync(os.Stdout)
		} else {
			sink = zapcore.AddSync(f)
		}
	}

	core := zapcore.NewCore(encoder, sink, lvl)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	defaultLogger = logger.Sugar()
}

func L() *zap.SugaredLogger                { return defaultLogger }
func Info(args ...interface{})             { defaultLogger.Info(args...) }
func Infof(t string, args ...interface{})  { defaultLogger.Infof(t, args...) }
func Warn(args ...interface{})             { defaultLogger.Warn(args...) }
func Warnf(t string, args ...interface{})  { defaultLogger.Warnf(t, args...) }
func Error(args ...interface{})            { defaultLogger.Error(args...) }
func Errorf(t string, args ...interface{}) { defaultLogger.Errorf(t, args...) }
func Debug(args ...interface{})            { defaultLogger.Debug(args...) }
func Debugf(t string, args ...interface{}) { defaultLogger.Debugf(t, args...) }
func Fatal(args ...interface{})            { defaultLogger.Fatal(args...) }
func Fatalf(t string, args ...interface{}) { defaultLogger.Fatalf(t, args...) }

func With(args ...interface{}) *zap.SugaredLogger {
	return defaultLogger.With(args...)
}

func Sync() { _ = defaultLogger.Sync() }
