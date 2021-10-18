package logger

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// Key used in the config got logging options
	CONFIG_KEY_LOG = "logging"

	// Config default values
	LOG_LEVEL = "debug"
	LOG_OUPUT = "stderr"

	// Config log keys used to define options
	CONFIG_KEY_LOG_OUTPUT    = "output"
	CONFIG_KEY_LOG_MESSAGE   = "message"
	CONFIG_KEY_LOG_LEVEL     = "level"
	CONFIG_KEY_LOG_CALLER    = "caller"
	CONFIG_KEY_LOG_TIMESTAMP = "timestamp"
)

func InitLogger() {

	// Get logging config values
	var zapConfig zap.Config
	if viper.GetString(CONFIG_KEY_LOG+"."+CONFIG_KEY_LOG_LEVEL) == "debug" {
		zapConfig = zap.NewDevelopmentConfig()
	} else {
		zapConfig = zap.NewProductionConfig()
	}

	zapConfig.OutputPaths = viper.GetStringSlice(CONFIG_KEY_LOG + "." + CONFIG_KEY_LOG_OUTPUT)

	zapConfig.EncoderConfig = zapcore.EncoderConfig{
		MessageKey: CONFIG_KEY_LOG_MESSAGE,

		LevelKey:    CONFIG_KEY_LOG_LEVEL,
		EncodeLevel: zapcore.CapitalColorLevelEncoder,

		TimeKey:    CONFIG_KEY_LOG_TIMESTAMP,
		EncodeTime: zapcore.ISO8601TimeEncoder,

		CallerKey:    CONFIG_KEY_LOG_CALLER,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	logger, _ := zapConfig.Build()
	zap.ReplaceGlobals(logger)
	defer logger.Sync()
}

func PanicCheck(err error) {
	if err != nil {
		zap.S().Panicf("%+v", err)
	}
}

func PanicCheckSlice(errs []error) {
	for i, e := range errs {
		if i == len(errs)-1 {
			PanicCheck(e)
		}
		ErrorCheck(e)
	}
}

func ErrorCheck(err error) {
	if err != nil {
		zap.S().Errorf("%+v", err)
	}
}
