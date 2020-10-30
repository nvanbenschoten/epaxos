package epaxos

import (
	"fmt"
	"log"
	"os"
)

// Logger provides a logging interface similar to golang's standard Logger.
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	Info(v ...interface{})
	Infof(format string, v ...interface{})

	Warning(v ...interface{})
	Warningf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

const (
	calldepth = 2
)

// DefaultLogger is a default implementation of the Logger interface.
type DefaultLogger struct {
	*log.Logger
	debug bool
}

// NewDefaultLogger creates a default logger that prints to stderr.
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{Logger: log.New(os.Stderr, "", log.LstdFlags)}
}

// EnableDebug enables debug messages to print.
func (l *DefaultLogger) EnableDebug() {
	l.debug = true
}

// Debug implements the Logger interface.
func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprint(v...)))
	}
}

// Debugf implements the Logger interface.
func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, header("DEBUG", fmt.Sprintf(format, v...)))
	}
}

// Info implements the Logger interface.
func (l *DefaultLogger) Info(v ...interface{}) {
	l.Output(calldepth, header("INFO", fmt.Sprint(v...)))
}

// Infof implements the Logger interface.
func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	l.Output(calldepth, header("INFO", fmt.Sprintf(format, v...)))
}

// Error implements the Logger interface.
func (l *DefaultLogger) Error(v ...interface{}) {
	l.Output(calldepth, header("ERROR", fmt.Sprint(v...)))
}

// Errorf implements the Logger interface.
func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	l.Output(calldepth, header("ERROR", fmt.Sprintf(format, v...)))
}

// Warning implements the Logger interface.
func (l *DefaultLogger) Warning(v ...interface{}) {
	l.Output(calldepth, header("WARN", fmt.Sprint(v...)))
}

// Warningf implements the Logger interface.
func (l *DefaultLogger) Warningf(format string, v ...interface{}) {
	l.Output(calldepth, header("WARN", fmt.Sprintf(format, v...)))
}

// Fatal implements the Logger interface.
func (l *DefaultLogger) Fatal(v ...interface{}) {
	l.Output(calldepth, header("FATAL", fmt.Sprint(v...)))
	os.Exit(1)
}

// Fatalf implements the Logger interface.
func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	l.Output(calldepth, header("FATAL", fmt.Sprintf(format, v...)))
	os.Exit(1)
}

// Panic implements the Logger interface.
func (l *DefaultLogger) Panic(v ...interface{}) {
	l.Logger.Panic(v...)
}

// Panicf implements the Logger interface.
func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}
