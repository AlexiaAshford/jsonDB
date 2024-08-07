// write.go

// 介绍:
// write.go 文件包含了 jsonDB 数据库的写操作相关功能。
// 这个文件实现了数据的持久化存储、WAL (Write-Ahead Logging) 机制,
// 以及数据的加载和恢复功能。主要目的是确保数据的持久性和一致性,
// 即使在系统崩溃或意外关闭的情况下也能保证数据的完整性。
//
// 主要功能:
// 1. WAL (Write-Ahead Logging): 在执行实际的数据修改之前,先将操作记录到日志文件中。
// 2. 数据文件操作: 将文档数据写入持久化存储。
// 3. 数据加载: 在启动时从持久化存储中加载数据到内存。
// 4. 数据恢复: 使用 WAL 文件在系统崩溃后恢复数据。
//
// 这些功能共同确保了数据库的 ACID 特性中的持久性 (Durability)。

package jsonDB

import (
	"encoding/binary"                   // 用于二进制数据的编码和解码
	"fmt"                               // 用于格式化字符串
	"github.com/vmihailenco/msgpack/v5" // 用于数据序列化
	"io"                                // 提供 I/O 原语
	"sync/atomic"                       // 提供原子操作
)

// writeWAL 函数用于将操作写入WAL（Write-Ahead Log）文件
// 参数:
// - operation: 操作类型 (如 "INSERT", "UPDATE", "DELETE")
// - id: 文档的唯一标识符
// - doc: 文档内容
// 返回: 错误信息 (如果有)
func (db *Database) writeWAL(operation, id string, doc map[string]interface{}) error {
	db.logger.Debug(fmt.Sprintf("Writing WAL entry: operation=%s, id=%s", operation, id))

	// 创建一个包含操作信息的结构体
	entry := struct {
		Operation string
		ID        string
		Document  map[string]interface{}
	}{
		Operation: operation,
		ID:        id,
		Document:  doc,
	}

	// 使用 MessagePack 序列化 entry 结构体
	data, err := msgpack.Marshal(entry)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to marshal WAL entry: %v", err))
		return fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// 获取数据库的写锁
	db.mu.Lock()
	defer db.mu.Unlock()

	// 写入数据长度 (4字节无符号整数)
	if err := binary.Write(db.walFile, binary.LittleEndian, uint32(len(data))); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to write WAL entry size: %v", err))
		return fmt.Errorf("failed to write WAL entry size: %w", err)
	}

	// 写入实际数据
	_, err = db.walFile.Write(data)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to write WAL entry data: %v", err))
		return fmt.Errorf("failed to write WAL entry data: %w", err)
	}

	db.logger.Debug("WAL entry written successfully")
	return nil
}

// writeToDataFile 函数用于将文档写入数据文件
// 参数:
// - id: 文档的唯一标识符
// - doc: 要写入的文档内容
// 返回: 错误信息 (如果有)
func (db *Database) writeToDataFile(id string, doc map[string]interface{}) error {
	db.logger.Debug(fmt.Sprintf("Writing document to data file: id=%s", id))

	// 创建一个包含文档ID和数据的结构体,并序列化
	data, err := msgpack.Marshal(struct {
		ID   string
		Data map[string]interface{}
	}{
		ID:   id,
		Data: doc,
	})
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to marshal document: %v", err))
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	// 获取数据库的写锁
	db.mu.Lock()
	defer db.mu.Unlock()

	// 将文件指针移动到文件末尾
	_, err = db.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to seek to the end of the data file: %v", err))
		return fmt.Errorf("failed to seek to the end of the data file: %w", err)
	}

	// 写入数据长度 (4字节无符号整数)
	if err := binary.Write(db.dataFile, binary.LittleEndian, uint32(len(data))); err != nil {
		db.logger.Error(fmt.Sprintf("Failed to write document size: %v", err))
		return fmt.Errorf("failed to write document size: %w", err)
	}

	// 写入实际数据
	_, err = db.dataFile.Write(data)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to write document data: %v", err))
		return fmt.Errorf("failed to write document data: %w", err)
	}

	db.logger.Debug("Document written to data file successfully")
	return nil
}

// loadData 函数用于从数据文件加载数据
// 返回: 错误信息 (如果有)
func (db *Database) loadData() error {
	db.logger.Info("Loading data from data file")

	// 将文件指针移动到文件开头
	_, err := db.dataFile.Seek(0, 0)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to seek to the beginning of the data file: %v", err))
		return fmt.Errorf("failed to seek to the beginning of the data file: %w", err)
	}

	// 循环读取文件中的所有文档
	for {
		var size uint32
		// 读取数据长度
		err = binary.Read(db.dataFile, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				break // 如果到达文件末尾,退出循环
			}
			db.logger.Error(fmt.Sprintf("Failed to read document size: %v", err))
			return fmt.Errorf("failed to read document size: %w", err)
		}

		// 读取实际数据
		data := make([]byte, size)
		_, err = io.ReadFull(db.dataFile, data)
		if err != nil {
			db.logger.Error(fmt.Sprintf("Failed to read document data: %v", err))
			return fmt.Errorf("failed to read document data: %w", err)
		}

		// 反序列化文档数据
		var docEntry struct {
			ID   string
			Data map[string]interface{}
		}
		err = msgpack.Unmarshal(data, &docEntry)
		if err != nil {
			db.logger.Error(fmt.Sprintf("Failed to unmarshal document data: %v", err))
			return fmt.Errorf("failed to unmarshal document data: %w", err)
		}

		// 创建文档对象并存储到内存中
		doc := &Document{data: docEntry.Data}
		db.data.Store(docEntry.ID, doc)

		// 更新索引
		db.indexes.Range(func(_, indexValue interface{}) bool {
			switch idx := indexValue.(type) {
			case *Index:
				db.indexDocument(doc, docEntry.ID, idx)
			case *CompositeIndex:
				db.indexDocumentComposite(doc, docEntry.ID, idx)
			}
			return true
		})

		// 原子操作增加文档计数
		atomic.AddInt64(&db.docCount, 1)
	}

	db.logger.Info(fmt.Sprintf("Loaded %d documents from data file", atomic.LoadInt64(&db.docCount)))
	return nil
}

// recoverFromWAL 函数用于从WAL文件恢复数据
// 返回: 错误信息 (如果有)
func (db *Database) recoverFromWAL() error {
	db.logger.Info("Recovering from WAL file")

	// 将文件指针移动到WAL文件开头
	_, err := db.walFile.Seek(0, 0)
	if err != nil {
		db.logger.Error(fmt.Sprintf("Failed to seek to the beginning of the WAL file: %v", err))
		return fmt.Errorf("failed to seek to the beginning of the WAL file: %w", err)
	}

	recoveredCount := 0
	// 循环读取WAL文件中的所有条目
	for {
		var size uint32
		// 读取条目长度
		err = binary.Read(db.walFile, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				break // 如果到达文件末尾,退出循环
			}
			db.logger.Error(fmt.Sprintf("Failed to read WAL entry size: %v", err))
			return fmt.Errorf("failed to read WAL entry size: %w", err)
		}

		// 读取实际数据
		data := make([]byte, size)
		_, err = io.ReadFull(db.walFile, data)
		if err != nil {
			db.logger.Error(fmt.Sprintf("Failed to read WAL entry data: %v", err))
			return fmt.Errorf("failed to read WAL entry data: %w", err)
		}

		// 反序列化WAL条目
		var entry struct {
			Operation string
			ID        string
			Document  map[string]interface{}
		}
		err = msgpack.Unmarshal(data, &entry)
		if err != nil {
			db.logger.Error(fmt.Sprintf("Failed to unmarshal WAL entry: %v", err))
			return fmt.Errorf("failed to unmarshal WAL entry: %w", err)
		}

		// 根据操作类型执行相应的恢复操作
		switch entry.Operation {
		case OperationInsert, OperationUpdate:
			db.data.Store(entry.ID, &Document{data: entry.Document})
			recoveredCount++
		case OperationDelete:
			db.data.Delete(entry.ID)
			recoveredCount++
		}
	}

	db.logger.Info(fmt.Sprintf("Recovered %d operations from WAL file", recoveredCount))
	return nil
}
