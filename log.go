package eventbus

import "log"

// Shit Google! Logger should have long been a interface, so that we could adopt 3rd loggers!
type Logger interface {
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Infoln(v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Errorln(v ...interface{})
}

type ConsoleLogger struct {
	*log.Logger
}

func (l *ConsoleLogger) Info(v ...interface{}) {
	l.Print(v...)
}

func (l *ConsoleLogger) Infof(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *ConsoleLogger) Infoln(v ...interface{}) {
	l.Println(v...)
}

func (l *ConsoleLogger) Error(v ...interface{}) {
	l.Print(v...)
}

func (l *ConsoleLogger) Errorf(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *ConsoleLogger) Errorln(v ...interface{}) {
	l.Println(v...)
}

type DoNothingLogger struct {
}

func (l *DoNothingLogger) Info(v ...interface{}) {
}
func (l *DoNothingLogger) Infof(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Infoln(v ...interface{}) {
}
func (l *DoNothingLogger) Error(v ...interface{}) {
}
func (l *DoNothingLogger) Errorf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Errorln(v ...interface{}) {
}
