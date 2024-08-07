package jsonDB

import (
	"fmt"
	"strings"
	"sync"
)

// Query 方法用于在数据库中查询符合特定条件的文档
//
// 介绍:
// Query 方法是 jsonDB 的核心查询功能,它允许用户根据指定的字段和值在数据库中搜索匹配的文档。
// 该方法支持两种查询模式:
// 1. 索引查询: 如果查询的字段已建立索引,则使用索引进行快速查询
// 2. 全表扫描: 如果查询的字段没有索引,则遍历所有文档进行匹配
//
// 该方法在查询过程中考虑了并发安全性,使用了适当的锁机制来保护数据访问。
// 为了处理可能的类型不匹配问题(例如整数和浮点数的比较),该方法使用 toFloat64 函数将值转换为统一的浮点数类型进行比较。
//
// 参数:
// - field: 要查询的字段名
// - value: 要匹配的值
//
// 返回值:
// - []map[string]interface{}: 包含所有匹配文档的切片,每个文档表示为一个 map
func (db *Database) Query(field string, value interface{}) []map[string]interface{} {
	// 记录查询的字段、值和值的类型,用于调试
	db.logger.Debug(fmt.Sprintf("Querying for field: %s, value: %v (type: %T)", field, value, value))

	// 初始化结果切片
	var results []map[string]interface{}

	// 尝试从数据库的索引中加载指定字段的索引
	indexValue, indexExists := db.indexes.Load(field)

	if indexExists {
		// 如果索引存在,使用索引进行查询
		if idx, ok := indexValue.(*Index); ok {
			// 对索引加读锁,确保并发安全
			idx.mu.RLock()
			defer idx.mu.RUnlock() // 确保在函数返回时解锁

			// 将查询值转换为浮点数,以统一比较
			queryValue := toFloat64(value)

			// 遍历索引中的所有键值对
			idx.values.Range(func(key, valueMapInterface interface{}) bool {
				// 将索引键转换为浮点数进行比较
				indexKey := toFloat64(key)
				// 如果索引键与查询值匹配
				if indexKey == queryValue {
					if valueMap, ok := valueMapInterface.(*sync.Map); ok {
						// 遍历匹配的文档ID
						valueMap.Range(func(docID, _ interface{}) bool {
							// 获取文档并添加到结果中
							if doc, exists := db.Get(docID.(string)); exists {
								results = append(results, doc)
							}
							return true
						})
					}
				}
				return true
			})

			// 记录使用索引查询的结果数量
			db.logger.Info(fmt.Sprintf("Query using index on field %s returned %d results", field, len(results)))
		}
	} else {
		// 如果索引不存在,进行全表扫描
		db.data.Range(func(_, value interface{}) bool {
			doc := value.(*Document)
			// 对文档加读锁,确保并发安全
			doc.mu.RLock()
			// 检查文档是否包含查询字段
			if fieldValue, ok := doc.data[field]; ok {
				// 将文档中的字段值和查询值都转换为float64进行比较
				docValue := toFloat64(fieldValue)
				queryValue := toFloat64(value)
				// 如果值匹配,则添加到结果中
				if docValue == queryValue {
					// 创建文档的副本以避免并发问题
					docCopy := make(map[string]interface{})
					for k, v := range doc.data {
						docCopy[k] = v
					}
					results = append(results, docCopy)
				}
			}
			// 释放文档的读锁
			doc.mu.RUnlock()
			return true
		})
		// 记录全表扫描的结果数量
		db.logger.Info(fmt.Sprintf("Full scan query on field %s returned %d results", field, len(results)))
	}

	// 返回查询结果
	return results
}

// QueryComposite 方法用于根据复合索引查询文档
//
// 介绍:
// QueryComposite 是一个高效的查询方法,专门用于处理多字段组合查询。它利用预先创建的复合索引来
// 快速定位符合多个条件的文档,而无需遍历整个数据集。这种方法特别适用于需要同时满足多个条件的
// 查询场景,如"查找年龄在25-30之间且工资超过50000的员工"。
//
// 复合索引的工作原理是将多个字段的值组合成一个唯一的键,这样可以在一次查找中匹配多个条件。
// 这种方法大大提高了查询效率,尤其是在大型数据集上执行复杂查询时。
//
// 使用复合索引查询时,字段的顺序很重要,必须与创建索引时的顺序一致。这是因为索引键是按照
// 字段顺序生成的。
//
// 参数:
// - fields: 一个字符串切片,包含要查询的字段名,顺序必须与创建复合索引时的顺序一致
// - values: 一个接口切片,包含与fields对应的查询值,顺序必须与fields一致
//
// 返回值:
// - []map[string]interface{}: 包含所有匹配文档的切片,每个文档表示为一个map
func (db *Database) QueryComposite(fields []string, values []interface{}) []map[string]interface{} {
	// 生成复合索引的键,使用'-'连接所有字段名
	indexKey := strings.Join(fields, "-")

	// 记录查询操作的日志,包括查询的字段和值
	db.logger.Debug(fmt.Sprintf("Querying composite index for fields: %v, values: %v", fields, values))

	// 初始化结果切片,用于存储匹配的文档
	var results []map[string]interface{}

	// 尝试从数据库的索引中获取复合索引
	indexValue, indexExists := db.indexes.Load(indexKey)

	// 如果复合索引存在,则使用索引进行查询
	if indexExists {
		// 将索引转换为 CompositeIndex 类型
		if idx, ok := indexValue.(*CompositeIndex); ok {
			var fieldValues []string
			// 将查询值转换为字符串切片,以便生成复合键
			for _, v := range values {
				fieldValues = append(fieldValues, fmt.Sprintf("%v", v))
			}
			// 生成复合查询键,使用'-'连接所有值
			compositeKey := strings.Join(fieldValues, "-")

			// 对复合索引加读锁,确保并发安全
			idx.mu.RLock()
			// 从复合索引中查找匹配的值
			if valueMapInterface, ok := idx.values.Load(compositeKey); ok {
				if valueMap, ok := valueMapInterface.(*sync.Map); ok {
					// 遍历复合索引中匹配的文档ID
					valueMap.Range(func(key, _ interface{}) bool {
						// 获取完整的文档并添加到结果集
						if doc, exists := db.Get(key.(string)); exists {
							results = append(results, doc)
						}
						return true // 继续遍历
					})
				}
			}
			// 释放复合索引的读锁
			idx.mu.RUnlock()

			// 记录查询结果的日志
			db.logger.Info(fmt.Sprintf("Composite query using index on fields %v returned %d results", fields, len(results)))
		}
	} else {
		// 如果复合索引不存在,记录警告日志
		db.logger.Warn(fmt.Sprintf("Composite index not found for fields: %v", fields))
	}

	// 返回查询结果
	return results
}
