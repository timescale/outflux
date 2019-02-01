package utils

import "fmt"

// Logger defines a single function that takes anything and loggs it somewhere
type Logger interface {
	Log(interface{})
}

type defaultLogger struct {
	logFn func(interface{})
}

func (log *defaultLogger) Log(arg interface{}) {
	log.logFn(arg)
}

// NewLogger creates an instance of Logger, if quiet == true, it logs nothing, else does fmt.Println
func NewLogger(quiet bool) Logger {
	var logFn func(interface{})
	if quiet {
		logFn = func(interface{}) {
		}
	} else {
		logFn = func(arg interface{}) {
			fmt.Println(arg)
		}
	}

	return &defaultLogger{logFn}
}
