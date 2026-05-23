package logging

import (
	"log/slog"
	"os"
	"strings"
)

func InitDefaultLogger() {
	lvl := slog.LevelInfo

	envLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))

	switch envLevel {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: lvl}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}
