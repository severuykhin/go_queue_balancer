package logging

type Logger interface {
	Fatal(code int, module string, message string)
	Error(code int, module string, message string)
	Info(code int, module string, message string)
	Debug(code int, module string, message string)
}
