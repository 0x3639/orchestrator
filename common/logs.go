package common

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
)

var (
	GlobalLogger, _     = CreateSugarLogger()
	AdministratorLogger = CreateFileLogger(DefaultAdministratorLogFile)
)

// redactingCore scrubs registered secrets from every entry before it reaches
// the wrapped core, covering messages, string fields and error fields.
type redactingCore struct {
	zapcore.Core
}

func newRedactingCore(core zapcore.Core) zapcore.Core {
	return redactingCore{Core: core}
}

func (c redactingCore) With(fields []zapcore.Field) zapcore.Core {
	return redactingCore{Core: c.Core.With(redactFields(fields))}
}

func (c redactingCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c redactingCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	entry.Message = LogRedactor.Redact(entry.Message)
	return c.Core.Write(entry, redactFields(fields))
}

func redactFields(fields []zapcore.Field) []zapcore.Field {
	if len(fields) == 0 {
		return fields
	}
	out := make([]zapcore.Field, len(fields))
	for i, f := range fields {
		switch f.Type {
		case zapcore.StringType:
			f.String = LogRedactor.Redact(f.String)
		case zapcore.ErrorType:
			if err, ok := f.Interface.(error); ok && err != nil {
				f = zap.String(f.Key, LogRedactor.Redact(err.Error()))
			}
		case zapcore.StringerType:
			if stringer, ok := f.Interface.(fmt.Stringer); ok && stringer != nil {
				f = zap.String(f.Key, LogRedactor.Redact(stringer.String()))
			}
		}
		out[i] = f
	}
	return out
}

func redactingOption() zap.Option {
	return zap.WrapCore(newRedactingCore)
}

func CreateLogger() (*zap.Logger, error) {
	logger, errInit := zap.NewDevelopment(redactingOption())
	if errInit != nil {
		return nil, errInit
	}
	return logger, nil
}

func CreateSugarLogger() (*zap.SugaredLogger, error) {
	logger, errInit := zap.NewDevelopment(redactingOption())
	if errInit != nil {
		return nil, errInit
	}
	return logger.Sugar(), nil
}

func CreateFileLogger(outputFile string) *zap.SugaredLogger {
	outputPath := path.Join(DefaultDataDir(), DefaultLogsDir)
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputPath, 0700)
		if err != nil {
			panic(err)
		}
	}
	outputPath = path.Join(outputPath, outputFile)
	sampleJSON := []byte(fmt.Sprintf(`{
       "level" : "info",
       "encoding": "json",
       "outputPaths":["stdout", "%s"],
       "errorOutputPaths":["stderr"],
       "encoderConfig": {
           "messageKey":"message",
           "levelKey":"level",
           "levelEncoder":"lowercase"
       }
   }`, outputPath))

	var cfg zap.Config
	if err := json.Unmarshal(sampleJSON, &cfg); err != nil {
		panic(err)
	}

	logger, err := cfg.Build(redactingOption())
	if err != nil {
		panic(err)
	}
	return logger.Sugar()
}
