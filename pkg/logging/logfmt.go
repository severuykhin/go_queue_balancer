package logging

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type LogFmt struct {
	ErrorOutputStream *os.File
	InfoOutputStream  *os.File
	AppName           string
}

func NewLogFmt(errorOutputStream *os.File, infoOutputStream *os.File, app string) *LogFmt {
	return &LogFmt{
		ErrorOutputStream: errorOutputStream,
		InfoOutputStream:  infoOutputStream,
		AppName:           app}
}

func (l *LogFmt) Fatal(code int, module string, message string) {
	l.Log("FATAL", code, module, message, true)
}

func (l *LogFmt) Error(code int, module string, message string) {
	l.Log("ERROR", code, module, message, false)
}

func (l *LogFmt) Info(code int, module string, message string) {
	l.Log("INFO", code, module, message, false)
}

func (l *LogFmt) Debug(code int, module string, message string) {
	l.Log("DEBUG", code, module, message, false)
}

func (l *LogFmt) Log(level string, code int, module string, message string, fatal bool) {

	go func() {
		dateTime := time.Now().Format(time.RFC3339)

		var msgTemplate strings.Builder

		msgTemplate.WriteString("datetime=%s ")
		msgTemplate.WriteString("level=%s ")
		msgTemplate.WriteString("code=%s ")
		msgTemplate.WriteString("app=%s ")
		msgTemplate.WriteString("module=%s ")
		msgTemplate.WriteString("message=\"%s\" ")
		msgTemplate.WriteString("\n")

		msg := fmt.Sprintf(
			msgTemplate.String(),
			dateTime,
			level,
			strconv.Itoa(code),
			l.AppName,
			module,
			message)

		if level == "error" {
			io.WriteString(l.ErrorOutputStream, msg)
		} else {
			io.WriteString(l.InfoOutputStream, msg)
		}

		if fatal {
			os.Exit(1)
		}
	}()
}
