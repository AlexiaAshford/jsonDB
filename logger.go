package jsonDB

import (
	"fmt"
	"io"
	"log"
	"os"
)

// LogLevel 定义日志级别的枚举类型
type LogLevel int

const (
	// LogLevelOff 表示完全关闭日志
	LogLevelOff LogLevel = iota
	// LogLevelError 只记录错误日志
	LogLevelError
	// LogLevelWarn 记录警告和错误日志
	LogLevelWarn
	// LogLevelInfo 记录信息、警告和错误日志
	LogLevelInfo
	// LogLevelDebug 记录所有级别的日志,包括调试信息
	LogLevelDebug
)

// Logger 接口定义了日志系统应该实现的方法
// 这个接口允许我们在将来轻松地替换日志实现,而不影响其他代码
type Logger interface {
	// Info 记录一般信息日志
	Info(v ...interface{})
	// Warn 记录警告日志
	Warn(v ...interface{})
	// Error 记录错误日志
	Error(v ...interface{})
	// Debug 记录调试信息
	Debug(v ...interface{})
	// SetLevel 设置日志记录的级别
	SetLevel(level LogLevel)
	// SetOutput 设置日志输出的目标
	SetOutput(output io.Writer)
}

// DefaultLogger 是默认的日志实现
// 它封装了标准库的 log.Logger,并添加了日志级别控制
type DefaultLogger struct {
	level  LogLevel    // 当前的日志级别
	logger *log.Logger // 标准库的日志器
}

// NewDefaultLogger 创建一个新的默认日志器
// 它初始化日志级别为 Info,并将输出设置为标准输出
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		level:  LogLevelInfo,
		logger: log.New(os.Stdout, "", log.Ldate|log.Ltime),
	}
}

// Info 记录信息级别的日志
func (l *DefaultLogger) Info(v ...interface{}) { l.log(LogLevelInfo, "INFO: ", v...) }

// Warn 记录警告级别的日志
func (l *DefaultLogger) Warn(v ...interface{}) { l.log(LogLevelWarn, "WARN: ", v...) }

// Error 记录错误级别的日志
func (l *DefaultLogger) Error(v ...interface{}) { l.log(LogLevelError, "ERROR: ", v...) }

// Debug 记录调试级别的日志
func (l *DefaultLogger) Debug(v ...interface{}) { l.log(LogLevelDebug, "DEBUG: ", v...) }

// log 是内部方法,用于实际记录日志
// 它会检查日志级别,只有当要记录的日志级别不高于当前设置的级别时,才会实际写入日志
func (l *DefaultLogger) log(level LogLevel, prefix string, v ...interface{}) {
	if level <= l.level {
		l.logger.Print(prefix, fmt.Sprint(v...))
	}
}

// SetLevel 设置日志记录的级别
// 这允许在运行时动态调整日志的详细程度
func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.level = level
}

// SetOutput 设置日志输出的目标
// 这允许将日志重定向到不同的输出,如文件或网络流
func (l *DefaultLogger) SetOutput(output io.Writer) {
	l.logger.SetOutput(output)
}
