package jsonDB

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Database 结构体定义了数据库的核心结构
type Database struct {
	data       *sync.Map      // 存储文档的主要数据结构,使用 sync.Map 保证并发安全
	indexes    *sync.Map      // 存储索引的数据结构,也使用 sync.Map 保证并发安全
	primaryKey string         // 主键的字段名
	dbPath     string         // 数据库文件的存储路径
	dataFile   *os.File       // 数据文件的文件句柄
	walFile    *os.File       // Write-Ahead Log (WAL) 文件的文件句柄
	mu         sync.RWMutex   // 用于保护文件操作的读写锁
	workerPool chan struct{}  // 用于限制并发写操作的工作池
	docCount   int64          // 文档总数,使用原子操作保证并发安全
	writeWg    sync.WaitGroup // 用于等待所有写操作完成的等待组
	logger     Logger         // 日志器
}

// NewDatabase 创建一个新的数据库实例
func NewDatabase(primaryKey, dbPath string, numWorkers int) (*Database, error) {
	db := &Database{
		data:       &sync.Map{},                     // 初始化文档存储
		indexes:    &sync.Map{},                     // 初始化索引存储
		primaryKey: primaryKey,                      // 设置主键
		dbPath:     dbPath,                          // 设置数据库路径
		workerPool: make(chan struct{}, numWorkers), // 创建工作池通道
		logger:     NewDefaultLogger(),              // 创建默认日志器
	}

	db.logger.Info(fmt.Sprintf("Initializing database with primary key: %s, path: %s, workers: %d", primaryKey, dbPath, numWorkers))

	if err := os.MkdirAll(dbPath, DBDirPerm); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to create database directory: %v", err))
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	var err error
	db.dataFile, err = os.OpenFile(filepath.Join(dbPath, DataFileName), FileOpenModeRW, DBFilePerm)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to open data file: %v", err))
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	db.walFile, err = os.OpenFile(filepath.Join(dbPath, WALFileName), FileOpenModeWAL, DBFilePerm)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to open WAL file: %v", err))
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	if err = db.loadData(); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to load data: %v", err))
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	if err = db.recoverFromWAL(); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to recover from WAL: %v", err))
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	db.logger.Info("Database initialized successfully")
	return db, nil
}

// SetLogLevel 设置日志级别
func (db *Database) SetLogLevel(level LogLevel) {
	db.logger.SetLevel(level)
	db.logger.Info(fmt.Sprintf("Log level set to: %v", level))
}

// LogLevelOff 关闭日志
func (db *Database) LogLevelOff() {
	db.SetLogLevel(LogLevelOff)
	db.logger.Info("Logging turned off")
}

// SetLogOutput 设置日志输出
func (db *Database) SetLogOutput(output io.Writer) {
	db.logger.SetOutput(output)
	db.logger.Info("Log output changed")
}

// Close 关闭数据库,确保所有写操作完成并关闭文件句柄
func (db *Database) Close() error {
	db.logger.Info("Closing database")
	db.writeWg.Wait() // 等待所有写操作完成

	// 关闭数据文件
	if err := db.dataFile.Close(); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to close data file: %v", err))
		return fmt.Errorf("failed to close data file: %w", err)
	}
	// 关闭WAL文件
	if err := db.walFile.Close(); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to close WAL file: %v", err))
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	db.logger.Info("Database closed successfully")
	return nil
}

// Count 返回数据库中的文档总数
func (db *Database) Count() int64 {
	count := atomic.LoadInt64(&db.docCount) // 原子操作读取文档数量
	db.logger.Debug(fmt.Sprintf("Current document count: %d", count))
	return count
}
