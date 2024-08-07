package jsonDB

import (
	"encoding/json"
	"fmt"         // 导入格式化包
	"sync"        // 导入同步包
	"sync/atomic" // 导入原子操作包
)

// Document 结构体表示数据库中的一个文档
type Document struct {
	data map[string]interface{} // 存储文档数据的map
	mu   sync.RWMutex           // 用于保护文档数据的读写锁
}

// Insert 方法用于向数据库中插入新文档
//
// 介绍:
// Insert 是 jsonDB 的核心方法之一，用于将新文档添加到数据库中。该方法支持两种输入格式：
// 1. map[string]interface{} 类型的文档数据
// 2. JSON 格式的字符串
//
// 该方法执行以下主要步骤：
// - 解析和验证输入数据
// - 检查文档的唯一性（基于主键）
// - 将操作记录到 WAL（Write-Ahead Log）
// - 将文档存储在内存中
// - 更新所有相关索引
// - 异步将文档写入持久化存储
//
// Insert 方法在整个过程中都采取了必要的并发控制措施，确保了数据的一致性和完整性。
// 同时，该方法还实现了详细的日志记录，有助于监控和调试。
//
// 参数:
// - docData: 要插入的文档数据，可以是 map[string]interface{} 或 JSON 字符串
//
// 返回值:
// - error: 如果插入过程中发生错误，将返回相应的错误信息；如果插入成功，则返回 nil
func (db *Database) Insert(docData interface{}) error {
	// 记录 Insert 操作的开始
	db.logger.Debug("Starting Insert operation")

	var doc map[string]interface{}

	// 使用 switch 语句处理不同类型的输入
	switch v := docData.(type) {
	case map[string]interface{}:
		// 如果输入已经是 map[string]interface{}，直接使用
		doc = v
		db.logger.Debug("Input is a map[string]interface{}")
	case string:
		// 如果输入是字符串，尝试解析为 JSON
		db.logger.Debug("Input is a JSON string, attempting to parse")
		if err := json.Unmarshal([]byte(v), &doc); err != nil {
			// JSON 解析失败，记录错误并返回
			db.logger.Error(fmt.Sprintf("Failed to parse JSON string: %v", err))
			return fmt.Errorf("failed to parse JSON string: %w", err)
		}
		db.logger.Debug("Successfully parsed JSON string")
	default:
		// 不支持的输入类型，记录错误并返回
		db.logger.Error(fmt.Sprintf("Unsupported input type: %T", docData))
		return fmt.Errorf("unsupported input type: %T", docData)
	}

	// 检查文档中是否包含主键
	id, ok := doc[db.primaryKey]
	if !ok {
		// 主键不存在，记录错误并返回
		db.logger.Error(fmt.Sprintf("Primary key '%s' not found in document", db.primaryKey))
		return fmt.Errorf("primary key '%s' not found in document", db.primaryKey)
	}

	// 将主键转换为字符串
	idStr := fmt.Sprintf("%v", id)
	db.logger.Debug(fmt.Sprintf("Document ID: %s", idStr))

	// 检查具有相同 ID 的文档是否已存在
	if _, exists := db.Get(idStr); exists {
		// 文档已存在，记录警告并返回错误
		db.logger.Warn(fmt.Sprintf("Document with id '%s' already exists", idStr))
		return fmt.Errorf("document with id '%s' already exists", idStr)
	}

	// 创建新的 Document 对象
	newDoc := &Document{
		data: doc,
	}

	// 将插入操作写入 WAL
	db.logger.Debug("Writing to WAL")
	if err := db.writeWAL(OperationInsert, idStr, doc); err != nil {
		// WAL 写入失败，记录错误并返回
		db.logger.Error(fmt.Sprintf("Failed to write to WAL: %v", err))
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// 获取写锁
	db.logger.Debug("Acquiring write lock")
	db.writeWg.Add(1)
	db.workerPool <- struct{}{}
	defer func() {
		<-db.workerPool
		db.writeWg.Done()
		db.logger.Debug("Released write lock")
	}()

	// 将文档存储在内存中
	db.logger.Debug("Storing document in memory")
	db.data.Store(idStr, newDoc)

	// 更新所有索引
	db.logger.Debug("Updating indexes")
	db.indexes.Range(func(_, indexValue interface{}) bool {
		switch idx := indexValue.(type) {
		case *Index:
			// 更新单字段索引
			db.logger.Debug(fmt.Sprintf("Updating single field index for field: %s", idx.field))
			db.indexDocument(newDoc, idStr, idx)
		case *CompositeIndex:
			// 更新复合索引
			db.logger.Debug(fmt.Sprintf("Updating composite index for fields: %v", idx.fields))
			db.indexDocumentComposite(newDoc, idStr, idx)
		}
		return true
	})

	// 增加文档计数
	db.logger.Debug("Incrementing document count")
	atomic.AddInt64(&db.docCount, 1)

	// 异步将文档写入数据文件
	db.logger.Debug("Starting asynchronous write to data file")
	go func() {
		if err := db.writeToDataFile(idStr, doc); err != nil {
			// 数据文件写入失败，记录错误
			db.logger.Error(fmt.Sprintf("Failed to write document to data file: %v", err))
		} else {
			// 数据文件写入成功
			db.logger.Debug("Successfully wrote document to data file")
		}
	}()

	// 记录插入操作成功
	db.logger.Info(fmt.Sprintf("Successfully inserted document with id: %s", idStr))
	return nil
}

// Update 方法用于更新数据库中指定ID的文档
//
// 介绍:
// Update 方法是一个关键的数据操作函数，用于修改数据库中已存在的文档。
// 这个方法实现了原子性更新，确保在高并发环境下的数据一致性。
// 它不仅更新内存中的文档，还会更新相关的索引，并记录更新操作到WAL(Write-Ahead Log)中。
//
// 实现细节:
// 1. 使用乐观锁策略（Compare-and-Swap）来处理并发更新。
// 2. 创建文档的新版本，而不是直接修改原文档，以支持原子性更新。
// 3. 更新所有相关索引以保持数据一致性。
// 4. 使用WAL记录更新操作，确保数据持久性和可恢复性。
// 5. 异步写入数据文件，提高性能。
//
// 参数:
// - id: 要更新的文档的唯一标识符
// - updates: 包含要更新的字段和其新值的映射
//
// 返回值:
// - error: 如果更新过程中发生错误，返回相应的错误信息；如果更新成功，返回nil
func (db *Database) Update(id string, updates map[string]interface{}) error {
	// 记录更新尝试的日志
	db.logger.Debug(fmt.Sprintf("Attempting to update document with ID: %s, Updates: %v", id, updates))

	// 使用无限循环来处理并发更新冲突
	for {
		// 尝试从数据库中加载文档
		if value, ok := db.data.Load(id); ok {
			oldDoc := value.(*Document)
			oldDoc.mu.Lock() // 锁定文档，防止其他goroutine同时修改

			// 创建新的文档数据，首先复制原有数据
			newData := make(map[string]interface{})
			for k, v := range oldDoc.data {
				newData[k] = v
			}

			// 应用更新
			for k, v := range updates {
				newData[k] = v
			}

			// 创建新的Document对象
			newDoc := &Document{data: newData}

			// 尝试原子性地替换旧文档
			if db.data.CompareAndSwap(id, value, newDoc) {
				// 更新成功，执行后续操作

				// 将更新操作记录到WAL(Write-Ahead Log)
				if err := db.writeWAL(OperationUpdate, id, newData); err != nil {
					oldDoc.mu.Unlock() // 确保在返回错误前解锁
					db.logger.Error(fmt.Sprintf("Failed to write to WAL: %v", err))
					return fmt.Errorf("failed to write to WAL: %w", err)
				}

				// 更新所有相关索引
				db.indexes.Range(func(key, value interface{}) bool {
					switch idx := value.(type) {
					case *Index:
						db.updateIndex(id, oldDoc, newDoc, idx)
					case *CompositeIndex:
						db.updateCompositeIndex(id, oldDoc, newDoc, idx)
					}
					return true
				})

				// 异步写入数据文件
				db.writeWg.Add(1)
				go func() {
					db.workerPool <- struct{}{} // 获取工作池令牌，限制并发写入数量
					defer func() {
						<-db.workerPool   // 释放工作池令牌
						db.writeWg.Done() // 标记写入完成
					}()
					if err := db.writeToDataFile(id, newData); err != nil {
						db.logger.Error(fmt.Sprintf("Error writing to data file: %v", err))
					}
				}()

				oldDoc.mu.Unlock() // 解锁文档
				db.logger.Info(fmt.Sprintf("Document updated successfully with ID: %s", id))
				return nil
			}

			oldDoc.mu.Unlock() // 解锁文档
			// 如果 CompareAndSwap 失败，说明有并发更新，重试整个过程
			continue
		}

		// 如果文档不存在，记录警告并返回错误
		db.logger.Warn(fmt.Sprintf("Document with id '%s' not found", id))
		return fmt.Errorf("document with id '%s' not found", id)
	}
}

// Delete 方法用于从数据库中删除指定ID的文档
//
// 介绍:
// Delete 方法是一个关键的数据操作函数,用于从数据库中移除特定的文档。
// 这个方法不仅从内存中删除文档,还会更新相关的索引,并记录删除操作到WAL(Write-Ahead Log)中。
//
// 实现细节:
// 1. 使用 sync.Map 的 LoadAndDelete 方法原子性地删除文档。
// 2. 更新所有相关索引以保持数据一致性。
// 3. 使用 WAL 记录删除操作,确保数据持久性和可恢复性。
// 4. 使用原子操作更新文档计数,保证并发安全。
//
// 参数:
// - id: 要删除的文档的唯一标识符
//
// 返回值:
// - error: 如果删除过程中发生错误,返回相应的错误信息；如果删除成功或文档不存在,返回 nil
func (db *Database) Delete(id string) error {
	// 记录删除尝试的日志
	db.logger.Debug(fmt.Sprintf("Attempting to delete document with ID: %s", id))

	// 尝试从数据库中删除文档,LoadAndDelete 方法确保了操作的原子性
	if value, ok := db.data.LoadAndDelete(id); ok {
		doc := value.(*Document)
		// 对文档加写锁,确保在处理过程中不会被其他goroutine访问
		doc.mu.Lock()
		defer doc.mu.Unlock()

		// 将删除操作记录到WAL(Write-Ahead Log)
		if err := db.writeWAL(OperationDelete, id, nil); err != nil {
			db.logger.Error(fmt.Sprintf("Failed to write to WAL: %v", err))
			return fmt.Errorf("failed to write to WAL: %w", err)
		}

		// 更新所有相关索引
		db.indexes.Range(func(key, value interface{}) bool {
			switch idx := value.(type) {
			case *Index:
				db.removeFromIndex(id, doc, idx)
			case *CompositeIndex:
				db.removeFromCompositeIndex(id, doc, idx)
			}
			return true
		})

		// 使用原子操作减少文档计数,确保并发安全
		atomic.AddInt64(&db.docCount, -1)

		// 记录删除成功的日志
		db.logger.Info(fmt.Sprintf("Document deleted successfully with ID: %s", id))
		return nil
	}

	// 如果文档不存在,记录警告日志并静默返回
	db.logger.Warn(fmt.Sprintf("Document with id '%s' not found for deletion", id))
	return nil
}

// Get 方法用于从数据库中获取指定ID的文档
//
// 介绍:
// Get 方法是一个基本的数据检索函数,用于根据文档ID获取完整的文档内容。
// 这个方法直接从内存中读取数据,因此速度很快,但同时也确保了并发安全。
//
// 实现细节:
// 1. 使用 sync.Map 的 Load 方法安全地获取文档。
// 2. 使用读锁确保在读取过程中文档不会被修改。
// 3. 返回文档的直接引用,而不是副本,以提高性能。但这要求调用者不应修改返回的数据。
//
// 参数:
// - id: 要获取的文档的唯一标识符
//
// 返回值:
// - map[string]interface{}: 如果文档存在,返回文档内容
// - bool: 表示文档是否存在
func (db *Database) Get(id string) (map[string]interface{}, bool) {
	// 记录获取尝试的日志
	db.logger.Debug(fmt.Sprintf("Attempting to get document with ID: %s", id))

	// 尝试从数据库中加载文档
	if value, ok := db.data.Load(id); ok {
		doc := value.(*Document)
		// 对文档加读锁,确保在读取过程中数据不会被修改
		doc.mu.RLock()
		defer doc.mu.RUnlock() // 使用 defer 确保在函数返回时解锁

		// 记录成功获取文档的日志
		db.logger.Debug(fmt.Sprintf("Document retrieved successfully with ID: %s", id))

		// 返回文档数据和true表示成功
		return doc.data, true
	}

	// 如果文档不存在,记录警告日志
	db.logger.Warn(fmt.Sprintf("Document with id '%s' not found", id))

	// 返回nil和false表示文档不存在
	return nil, false
}

// GetAll 方法用于获取数据库中的所有文档
//
// 介绍:
// GetAll 是一个全量查询方法,它返回数据库中存储的所有文档。
// 这个方法对于需要处理或分析整个数据集的场景非常有用,比如数据导出、全局统计或批量操作。
//
// 实现细节:
// 1. 该方法使用 sync.Map 的 Range 方法遍历所有存储的文档。
// 2. 为了保证并发安全,在访问每个文档时都会使用读锁。
// 3. 方法会创建每个文档的深拷贝,以防止在返回后对原始数据的意外修改。
// 4. 使用日志记录操作的开始和结束,包括获取的文档总数,有助于监控和调试。
//
// 性能考虑:
// 对于大型数据库,这个方法可能会消耗大量内存和时间。在处理大量数据时,
// 应考虑使用分页或流式处理的替代方法。
//
// 返回值:
// - []map[string]interface{}: 包含所有文档的切片,每个文档表示为一个 map
func (db *Database) GetAll() []map[string]interface{} {
	// 记录方法调用,用于调试
	db.logger.Debug("Attempting to get all documents")

	// 初始化结果切片,用于存储所有文档
	var allDocs []map[string]interface{}

	// 使用 sync.Map 的 Range 方法遍历所有文档
	db.data.Range(func(key, value interface{}) bool {
		// 将 value 转换为 Document 类型
		doc := value.(*Document)

		// 对文档加读锁,确保并发安全
		doc.mu.RLock()

		// 创建文档数据的深拷贝
		docCopy := make(map[string]interface{})
		for k, v := range doc.data {
			// 注意:这里假设文档中的值不包含需要深拷贝的复杂类型
			// 如果有嵌套的 map 或 slice,可能需要递归复制
			docCopy[k] = v
		}

		// 将文档副本添加到结果切片中
		allDocs = append(allDocs, docCopy)

		// 释放文档的读锁
		doc.mu.RUnlock()

		// 返回 true 以继续遍历
		return true
	})

	// 记录操作完成的信息,包括获取的文档总数
	db.logger.Info(fmt.Sprintf("Retrieved all documents, total count: %d", len(allDocs)))

	// 返回包含所有文档的切片
	return allDocs
}
