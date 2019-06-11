package eventbus

import "log"

// Shit Google! Logger should have long been a interface, so that we could adopt 3rd loggers!
type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Debugln(v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Infoln(v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Warnln(v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Errorln(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
}

type ConsoleLogger struct {
	*log.Logger
}

func (l *ConsoleLogger) Debug(v ...interface{}) {
	l.Print(v...)
}

func (l *ConsoleLogger) Debugf(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *ConsoleLogger) Debugln(v ...interface{}) {
	l.Println(v...)
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

func (l *ConsoleLogger) Warn(v ...interface{}) {
	l.Print(v...)
}

func (l *ConsoleLogger) Warnf(format string, v ...interface{}) {
	l.Printf(format, v...)
}

func (l *ConsoleLogger) Warnln(v ...interface{}) {
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

func (l *DoNothingLogger) Print(v ...interface{}) {
}
func (l *DoNothingLogger) Printf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Println(v ...interface{}) {
}
func (l *DoNothingLogger) Debug(v ...interface{}) {
}
func (l *DoNothingLogger) Debugf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Debugln(v ...interface{}) {
}
func (l *DoNothingLogger) Info(v ...interface{}) {
}
func (l *DoNothingLogger) Infof(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Infoln(v ...interface{}) {
}
func (l *DoNothingLogger) Warn(v ...interface{}) {
}
func (l *DoNothingLogger) Warnf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Warnln(v ...interface{}) {
}
func (l *DoNothingLogger) Error(v ...interface{}) {
}
func (l *DoNothingLogger) Errorf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Errorln(v ...interface{}) {
}
func (l *DoNothingLogger) Fatal(v ...interface{}) {
}
func (l *DoNothingLogger) Fatalf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Fatalln(v ...interface{}) {
}
func (l *DoNothingLogger) Panic(v ...interface{}) {
}
func (l *DoNothingLogger) Panicf(format string, v ...interface{}) {
}
func (l *DoNothingLogger) Panicln(v ...interface{}) {
}
