package jsonDB

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Index 结构体定义了单字段索引
type Index struct {
	field  string       // 索引字段名
	values *sync.Map    // 存储索引的数据结构,key是字段值,value是文档ID的集合
	trie   *Trie        // 用于支持模糊查询的 trie 结构
	mu     sync.RWMutex // 保护索引操作的读写锁
}

// CompositeIndex 结构体定义了复合索引
type CompositeIndex struct {
	fields []string     // 复合索引的字段名列表
	values *sync.Map    // 存储索引的数据结构,key是复合字段值,value是文档ID的集合
	mu     sync.RWMutex // 保护索引操作的读写锁
}

// CreateIndex 方法用于创建单字段索引
//
// 介绍:
// CreateIndex 是一个关键的数据库操作,用于为指定的字段创建索引。索引是提高查询性能的重要机制,
// 特别是在大型数据集上。通过创建索引,数据库可以快速定位满足特定条件的文档,而无需扫描整个集合。
//
// 这个方法不仅为新文档创建索引,还会遍历现有的所有文档并为它们建立索引。这确保了索引的完整性,
// 但也意味着在大型数据集上创建索引可能是一个耗时的操作。
//
// 索引的实现使用了 sync.Map 来存储索引数据,这提供了良好的并发性能。此外,还使用了 Trie 数据
// 结构来支持模糊查询,这对于文本搜索等场景非常有用。
//
// 需要注意的是,虽然索引可以显著提升读取性能,但会略微降低写入性能,因为每次插入或更新操作都
// 需要维护索引。因此,应该只为经常在查询中使用的字段创建索引。
//
// 参数:
// - field: 要创建索引的字段名
//
// 注意: 这个方法没有返回值,但会在日志中记录索引创建的结果
func (db *Database) CreateIndex(field string) {
	// 记录开始创建索引的日志
	db.logger.Info(fmt.Sprintf("Creating index for field: %s", field))

	// 获取数据库的写锁,确保在创建索引时数据不被修改
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查索引是否已存在
	if _, exists := db.indexes.Load(field); !exists {
		// 创建新索引
		index := &Index{
			field:  field,       // 设置索引字段
			values: &sync.Map{}, // 初始化存储索引数据的 sync.Map
			trie:   NewTrie(),   // 初始化用于支持模糊查询的 Trie
		}
		// 将新创建的索引存储到数据库的索引集合中
		db.indexes.Store(field, index)

		// 为现有文档创建索引
		indexedCount := 0 // 用于记录已索引的文档数量
		db.data.Range(func(key, value interface{}) bool {
			doc := value.(*Document)
			// 为每个文档创建索引
			db.indexDocument(doc, key.(string), index)
			indexedCount++
			return true // 继续遍历
		})

		// 记录索引创建完成的日志,包括索引的文档数量
		db.logger.Info(fmt.Sprintf("Index created for field %s, indexed %d documents", field, indexedCount))
	} else {
		// 如果索引已存在,记录警告日志
		db.logger.Warn(fmt.Sprintf("Index already exists for field: %s", field))
	}
}

// CreateCompositeIndex 方法用于创建复合索引
//
// 介绍:
// CreateCompositeIndex 是一个高级索引创建方法,用于为多个字段的组合创建索引。复合索引在需要
// 同时满足多个条件的查询中特别有用,如"查找特定年龄范围内且工资超过某个值的员工"。
//
// 复合索引的工作原理是将多个字段的值组合成一个唯一的键。这允许数据库在一次查找中匹配多个条件,
// 大大提高了复杂查询的效率,尤其是在大型数据集上。
//
// 这个方法不仅为新文档创建复合索引,还会遍历所有现有文档并为它们建立索引。这确保了索引的完整性,
// 但在大型数据集上可能会是一个耗时的操作。
//
// 需要注意的是,复合索引的字段顺序很重要。查询时必须使用相同的字段顺序才能利用到这个索引。
//
// 参数:
// - fields: 一个字符串切片,包含要创建复合索引的字段名
//
// 注意: 这个方法没有返回值,但会在日志中记录索引创建的结果
func (db *Database) CreateCompositeIndex(fields []string) {
	// 生成复合索引的键,使用'-'连接所有字段名
	indexKey := strings.Join(fields, "-")

	// 记录开始创建复合索引的日志
	db.logger.Info(fmt.Sprintf("Creating composite index for fields: %v", fields))

	// 获取数据库的写锁,确保在创建索引时数据不被修改
	db.mu.Lock()
	defer db.mu.Unlock()

	// 检查索引是否已存在
	if _, exists := db.indexes.Load(indexKey); !exists {
		// 创建新的复合索引
		index := &CompositeIndex{
			fields: fields,      // 设置复合索引的字段列表
			values: &sync.Map{}, // 初始化存储索引数据的 sync.Map
		}
		// 将新创建的复合索引存储到数据库的索引集合中
		db.indexes.Store(indexKey, index)

		// 为现有文档创建复合索引
		indexedCount := 0 // 用于记录已索引的文档数量
		db.data.Range(func(key, value interface{}) bool {
			doc := value.(*Document)
			// 为每个文档创建复合索引
			db.indexDocumentComposite(doc, key.(string), index)
			indexedCount++
			return true // 继续遍历
		})

		// 记录复合索引创建完成的日志,包括索引的文档数量
		db.logger.Info(fmt.Sprintf("Composite index created for fields %v, indexed %d documents", fields, indexedCount))
	} else {
		// 如果复合索引已存在,记录警告日志
		db.logger.Warn(fmt.Sprintf("Composite index already exists for fields: %v", fields))
	}
}

// indexDocument 方法用于为单个文档创建单字段索引
//
// 介绍:
// indexDocument 是一个内部方法,用于将单个文档的指定字段添加到相应的索引中。这个方法在插入
// 新文档或更新现有文档时被调用,以确保索引始终与实际数据保持同步。
//
// 该方法支持多种数据类型的索引,包括数值型(整数、浮点数)、时间型和字符串型。对于数值型和时间型,
// 会将其转换为统一的格式(float64或Unix时间戳)以便于比较和排序。
//
// 此外,该方法还维护了一个 Trie 结构,用于支持字符串的模糊查询和前缀匹配。
//
// 参数:
// - doc: 要索引的文档对象
// - id: 文档的唯一标识符
// - index: 要更新的索引对象
//
// 注意: 这个方法在内部使用,不应该直接从外部调用
func (db *Database) indexDocument(doc *Document, id string, index *Index) {
	// 获取文档的读锁,确保在索引过程中文档数据不被修改
	doc.mu.RLock()
	defer doc.mu.RUnlock()

	// 检查文档是否包含要索引的字段
	if fieldValue, ok := doc.data[index.field]; ok {
		var indexValue interface{}
		// 根据字段值的类型进行相应的转换
		switch v := fieldValue.(type) {
		case int, int64, float32, float64:
			// 数值类型统一转换为 float64
			indexValue = toFloat64(v)
		case time.Time:
			// 时间类型转换为 Unix 时间戳
			indexValue = v.Unix()
		default:
			// 其他类型(如字符串)直接使用原值
			indexValue = v
		}

		// 将索引值转换为字符串,用于 Trie 索引
		strValue := fmt.Sprintf("%v", indexValue)

		// 获取索引的写锁
		index.mu.Lock()
		// 将文档 ID 添加到索引中
		valueMap, _ := index.values.LoadOrStore(indexValue, &sync.Map{})
		valueMap.(*sync.Map).Store(id, struct{}{})
		// 将字符串值插入到 Trie 中,支持模糊查询
		index.trie.Insert(strings.ToLower(strValue), id)
		index.mu.Unlock()

		// 记录索引操作的日志
		db.logger.Debug(fmt.Sprintf("Indexed document %s for field %s with value %v (type: %T, indexValue: %v)", id, index.field, fieldValue, fieldValue, indexValue))
	} else {
		// 如果文档不包含要索引的字段,记录警告日志
		db.logger.Warn(fmt.Sprintf("Document %s does not contain field %s for indexing", id, index.field))
	}
}

// indexDocumentComposite 为单个文档创建复合索引
func (db *Database) indexDocumentComposite(doc *Document, id string, index *CompositeIndex) {
	doc.mu.RLock()
	defer doc.mu.RUnlock()

	var fieldValues []string
	// 获取所有复合索引字段的值
	for _, field := range index.fields {
		if fieldValue, ok := doc.data[field]; ok {
			fieldValues = append(fieldValues, fmt.Sprintf("%v", fieldValue))
		} else {
			fieldValues = append(fieldValues, "")
		}
	}
	compositeKey := strings.Join(fieldValues, "-") // 生成复合索引键

	index.mu.Lock()
	// 将文档ID添加到对应复合索引键的集合中
	valueMap, _ := index.values.LoadOrStore(compositeKey, &sync.Map{})
	valueMap.(*sync.Map).Store(id, struct{}{})
	index.mu.Unlock()
	db.logger.Debug(fmt.Sprintf("Indexed document %s for composite fields %v with key %s", id, index.fields, compositeKey))
}

// updateIndex 方法用于更新单字段索引
//
// 介绍:
// updateIndex 是一个内部方法,用于在文档更新时同步更新相应的索引。当文档中被索引的字段
// 发生变化时,这个方法会从旧值的索引中移除文档ID,并将其添加到新值的索引中。
//
// 这个方法确保了索引始终反映数据的最新状态,这对于保持查询结果的准确性至关重要。同时,
// 它也维护了用于模糊查询的 Trie 结构。
//
// 参数:
// - id: 被更新文档的唯一标识符
// - oldDoc: 更新前的文档对象
// - newDoc: 更新后的文档对象
// - index: 需要更新的索引对象
//
// 注意: 这个方法在内部使用,不应该直接从外部调用
func (db *Database) updateIndex(id string, oldDoc, newDoc *Document, index *Index) {
	// 获取旧文档和新文档中索引字段的值
	oldValue, _ := oldDoc.data[index.field]
	newValue, _ := newDoc.data[index.field]

	// 如果索引字段的值发生变化
	if oldValue != newValue {
		// 获取索引的写锁
		index.mu.Lock()
		defer index.mu.Unlock()

		// 从旧值的索引中移除文档ID
		if oldMap, ok := index.values.Load(oldValue); ok {
			oldMap.(*sync.Map).Delete(id)
			db.logger.Debug(fmt.Sprintf("Removed document %s from index %s for old value %v", id, index.field, oldValue))
		}
		// 从 Trie 中移除旧值
		index.trie.Remove(strings.ToLower(fmt.Sprintf("%v", oldValue)), id)

		// 将文档ID添加到新值的索引中
		newMap, _ := index.values.LoadOrStore(newValue, &sync.Map{})
		newMap.(*sync.Map).Store(id, struct{}{})
		// 将新值添加到 Trie 中
		index.trie.Insert(strings.ToLower(fmt.Sprintf("%v", newValue)), id)

		// 记录索引更新的日志
		db.logger.Debug(fmt.Sprintf("Added document %s to index %s for new value %v", id, index.field, newValue))
	}
}

// updateCompositeIndex 更新复合索引
func (db *Database) updateCompositeIndex(id string, oldDoc, newDoc *Document, index *CompositeIndex) {
	var oldFieldValues, newFieldValues []string
	// 获取旧文档和新文档的所有复合索引字段值
	for _, field := range index.fields {
		oldFieldValues = append(oldFieldValues, fmt.Sprintf("%v", oldDoc.data[field]))
		newFieldValues = append(newFieldValues, fmt.Sprintf("%v", newDoc.data[field]))
	}
	oldCompositeKey := strings.Join(oldFieldValues, "-")
	newCompositeKey := strings.Join(newFieldValues, "-")

	// 如果复合索引键发生变化
	if oldCompositeKey != newCompositeKey {
		index.mu.Lock()
		// 从旧复合键的集合中移除文档ID
		if oldMap, ok := index.values.Load(oldCompositeKey); ok {
			oldMap.(*sync.Map).Delete(id)
			db.logger.Debug(fmt.Sprintf("Removed document %s from composite index for old key %s", id, oldCompositeKey))
		}
		// 将文档ID添加到新复合键的集合中
		newMap, _ := index.values.LoadOrStore(newCompositeKey, &sync.Map{})
		newMap.(*sync.Map).Store(id, struct{}{})
		index.mu.Unlock()
		db.logger.Debug(fmt.Sprintf("Added document %s to composite index for new key %s", id, newCompositeKey))
	}
}

// removeFromIndex 从单字段索引中移除文档
func (db *Database) removeFromIndex(id string, doc *Document, index *Index) {
	if fieldValue, ok := doc.data[index.field]; ok {
		index.mu.Lock()
		// 从对应字段值的集合中移除文档ID
		if valueMap, ok := index.values.Load(fieldValue); ok {
			valueMap.(*sync.Map).Delete(id)
			db.logger.Debug(fmt.Sprintf("Removed document %s from index %s for value %v", id, index.field, fieldValue))
		}
		// 从 trie 中移除文档ID
		index.trie.Remove(strings.ToLower(fmt.Sprintf("%v", fieldValue)), id)
		index.mu.Unlock()
	} else {
		db.logger.Warn(fmt.Sprintf("Document %s does not contain field %s for index removal", id, index.field))
	}
}

// removeFromCompositeIndex 从复合索引中移除文档
func (db *Database) removeFromCompositeIndex(id string, doc *Document, index *CompositeIndex) {
	var fieldValues []string
	// 获取所有复合索引字段的值
	for _, field := range index.fields {
		if fieldValue, ok := doc.data[field]; ok {
			fieldValues = append(fieldValues, fmt.Sprintf("%v", fieldValue))
		} else {
			fieldValues = append(fieldValues, "")
		}
	}
	compositeKey := strings.Join(fieldValues, "-") // 生成复合索引键

	index.mu.Lock()
	// 从对应复合索引键的集合中移除文档ID
	if valueMap, ok := index.values.Load(compositeKey); ok {
		valueMap.(*sync.Map).Delete(id)
		db.logger.Debug(fmt.Sprintf("Removed document %s from composite index for key %s", id, compositeKey))
	}
	index.mu.Unlock()
}

// PrintIndexContent 方法用于打印指定字段的索引内容
//
// 介绍:
// PrintIndexContent 是一个调试和诊断工具,用于可视化展示数据库中特定字段的索引结构。
// 这个方法对于理解索引的内部结构、验证索引的正确性,以及调试查询性能问题非常有用。
//
// 该方法会遍历指定字段的整个索引结构,并以层级形式打印出每个索引键及其对应的文档ID。
// 这样可以清晰地看到索引是如何组织数据的,以及哪些文档与特定的索引值相关联。
//
// 注意,这个方法主要用于开发和调试目的。在生产环境中,特别是对于大型索引,应谨慎使用,
// 因为它可能会产生大量的输出并消耗较多的资源。
//
// 参数:
// - field: 要打印索引内容的字段名
//
// 注意: 这个方法没有返回值,所有的输出都通过日志系统记录
func (db *Database) PrintIndexContent(field string) {
	// 记录开始打印索引内容的日志
	db.logger.Debug(fmt.Sprintf("Printing index content for field: %s", field))

	// 尝试从数据库的索引集合中获取指定字段的索引
	if indexValue, ok := db.indexes.Load(field); ok {
		// 检查索引是否为单字段索引类型
		if idx, ok := indexValue.(*Index); ok {
			// 获取索引的读锁,确保在打印过程中索引内容不被修改
			idx.mu.RLock()
			defer idx.mu.RUnlock()

			// 遍历索引中的所有键值对
			idx.values.Range(func(key, value interface{}) bool {
				// 打印索引键
				db.logger.Debug(fmt.Sprintf("Index key: %v", key))

				// 检查值是否为预期的 sync.Map 类型
				if valueMap, ok := value.(*sync.Map); ok {
					// 遍历与该索引键关联的所有文档ID
					valueMap.Range(func(docID, _ interface{}) bool {
						// 打印文档ID
						db.logger.Debug(fmt.Sprintf("  Document ID: %v", docID))
						return true // 继续遍历
					})
				}
				return true // 继续遍历下一个索引键
			})
		}
	} else {
		// 如果未找到指定字段的索引,记录相应的日志
		db.logger.Debug(fmt.Sprintf("No index found for field: %s", field))
	}
}
