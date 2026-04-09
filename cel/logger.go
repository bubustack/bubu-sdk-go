package cel

import (
	"log/slog"
	"time"

	"github.com/bubustack/bobrapet/pkg/observability"
)

type sdkCELLogger struct {
	logger *slog.Logger
}

var _ observability.Logger = (*sdkCELLogger)(nil)

func newSDKCELLogger(logger *slog.Logger) *sdkCELLogger {
	return &sdkCELLogger{logger: logger}
}

func (l *sdkCELLogger) CacheHit(expression, expressionType string) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Debug("Template cache hit", "expression", expression, "type", expressionType)
}

func (l *sdkCELLogger) EvaluationStart(expression, expressionType string) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Debug("Template evaluation started", "expression", expression, "type", expressionType)
}

func (l *sdkCELLogger) EvaluationError(err error, expression, expressionType string, duration time.Duration) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Error("Template evaluation failed", "expression", expression, "type", expressionType, "duration", duration.String(), "error", err) //nolint:lll
}

func (l *sdkCELLogger) EvaluationSuccess(expression, expressionType string, duration time.Duration, result any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Debug("Template evaluation succeeded", "expression", expression, "type", expressionType, "duration", duration.String()) //nolint:lll
}
