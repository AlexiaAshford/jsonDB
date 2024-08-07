package jsonDB

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// FuzzyQuery 执行模糊查询
// field: 要查询的字段名
// pattern: 查询模式,支持 '*' 作为通配符
// 返回匹配的文档列表
func (db *Database) FuzzyQuery(field, pattern string) []map[string]interface{} {
	db.logger.Debug(fmt.Sprintf("Performing fuzzy query on field: %s with pattern: %s", field, pattern))

	var results []map[string]interface{}
	indexValue, indexExists := db.indexes.Load(field)

	if indexExists {
		if idx, ok := indexValue.(*Index); ok {
			idx.mu.RLock()
			defer idx.mu.RUnlock()

			// 使用 Trie 进行模糊匹配
			matchedDocs := idx.trie.FuzzySearch(strings.ToLower(pattern))

			// 收集匹配的文档
			matchedDocs.Range(func(docID, _ interface{}) bool {
				if doc, exists := db.Get(docID.(string)); exists {
					results = append(results, doc)
				}
				return true
			})

			db.logger.Info(fmt.Sprintf("Fuzzy query using trie index on field %s returned %d results", field, len(results)))
		}
	} else {
		// 如果没有索引,执行全表扫描
		results = db.fullScanFuzzyQuery(field, pattern)
	}

	return results
}

// fullScanFuzzyQuery 在没有索引时执行全表扫描的模糊查询
func (db *Database) fullScanFuzzyQuery(field, pattern string) []map[string]interface{} {
	db.logger.Debug(fmt.Sprintf("Performing full scan fuzzy query on field: %s with pattern: %s", field, pattern))

	var results []map[string]interface{}
	regex := wildcardToRegexp(pattern)

	db.data.Range(func(_, value interface{}) bool {
		doc := value.(*Document)
		doc.mu.RLock()
		if fieldValue, ok := doc.data[field]; ok {
			if regex.MatchString(fmt.Sprintf("%v", fieldValue)) {
				results = append(results, doc.data)
			}
		}
		doc.mu.RUnlock()
		return true
	})

	db.logger.Info(fmt.Sprintf("Full scan fuzzy query on field %s returned %d results", field, len(results)))
	return results
}

// wildcardToRegexp 将通配符模式转换为正则表达式
func wildcardToRegexp(pattern string) *regexp.Regexp {
	regexPattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", ".*") + "$"
	return regexp.MustCompile(regexPattern)
}

// FuzzySearch 在 Trie 中执行模糊搜索
func (t *Trie) FuzzySearch(pattern string) *sync.Map {
	results := &sync.Map{}
	t.fuzzySearchRecursive(t.root, pattern, "", results)
	return results
}

// fuzzySearchRecursive 是 FuzzySearch 的递归辅助函数
func (t *Trie) fuzzySearchRecursive(node *TrieNode, pattern, currentStr string, results *sync.Map) {
	if len(pattern) == 0 {
		// 模式匹配完成,收集结果
		node.docs.Range(func(key, value interface{}) bool {
			results.Store(key, value)
			return true
		})
		return
	}

	if pattern[0] == '*' {
		// 通配符匹配
		// 1. 匹配 0 个字符
		t.fuzzySearchRecursive(node, pattern[1:], currentStr, results)

		// 2. 匹配 1 个或多个字符
		for char, child := range node.children {
			t.fuzzySearchRecursive(child, pattern, currentStr+string(char), results)
		}
	} else {
		// 精确匹配当前字符
		if child, ok := node.children[rune(pattern[0])]; ok {
			t.fuzzySearchRecursive(child, pattern[1:], currentStr+string(pattern[0]), results)
		}
	}
}

// RangeQuery 执行范围查询，返回字段值在指定范围内的所有文档
//
// 介绍:
// RangeQuery 是一个强大的查询功能，允许用户在指定字段上执行范围搜索。
// 它支持各种数据类型，包括整数、浮点数和日期时间。
// 该函数首先检查是否存在相关的索引。如果存在，它会利用索引进行快速查询；
// 否则，它会执行全表扫描。
//
// 实现细节:
// - 使用了 toComparableValue 函数将输入值转换为可比较的类型
// - 利用 compareValues 函数进行值的比较，确保不同类型间的正确比较
// - 对于索引查询和全表扫描，使用相同的比较逻辑确保结果一致性
// - 使用读写锁保证并发安全
// - 通过日志记录查询过程，便于调试和性能分析
//
// 参数:
// - field: 要查询的字段名
// - min: 范围的最小值
// - max: 范围的最大值
//
// 返回值:
// - []map[string]interface{}: 包含所有匹配文档的切片
func (db *Database) RangeQuery(field string, min, max interface{}) []map[string]interface{} {
	// 记录查询的起始日志，包括字段名和查询范围
	db.logger.Debug(fmt.Sprintf("Performing range query on field: %s with range: [%v, %v]", field, min, max))

	// 初始化结果切片
	var results []map[string]interface{}

	// 将最小值和最大值转换为可比较的类型
	minValue := toComparableValue(min)
	maxValue := toComparableValue(max)

	// 记录转换后的最小值和最大值，便于调试
	db.logger.Debug(fmt.Sprintf("Converted min value: %v (%T)", minValue, minValue))
	db.logger.Debug(fmt.Sprintf("Converted max value: %v (%T)", maxValue, maxValue))

	// 尝试从数据库的索引中加载指定字段的索引
	indexValue, indexExists := db.indexes.Load(field)

	if indexExists {
		// 如果索引存在，使用索引进行查询
		if idx, ok := indexValue.(*Index); ok {
			// 对索引加读锁，确保并发安全
			idx.mu.RLock()
			defer idx.mu.RUnlock()

			// 遍历索引中的所有键值对
			idx.values.Range(func(key, value interface{}) bool {
				// 将索引键转换为可比较的类型
				keyValue := toComparableValue(key)

				// 记录当前比较的键值，便于调试
				db.logger.Debug(fmt.Sprintf("Comparing index key: %v (%T)", keyValue, keyValue))

				// 检查键值是否在查询范围内
				if compareValues(keyValue, minValue) >= 0 && compareValues(keyValue, maxValue) <= 0 {
					// 如果在范围内，获取对应的文档ID集合
					if valueMap, ok := value.(*sync.Map); ok {
						// 遍历文档ID集合
						valueMap.Range(func(docID, _ interface{}) bool {
							// 获取完整的文档
							if doc, exists := db.Get(docID.(string)); exists {
								// 将匹配的文档添加到结果集
								results = append(results, doc)
							}
							return true // 继续遍历
						})
					}
				}
				return true // 继续遍历索引
			})
			// 记录使用索引查询的结果数量
			db.logger.Info(fmt.Sprintf("Range query using index on field %s returned %d results", field, len(results)))
		}
	} else {
		// 如果索引不存在，执行全表扫描
		db.data.Range(func(_, value interface{}) bool {
			doc := value.(*Document)
			// 对文档加读锁，确保并发安全
			doc.mu.RLock()
			// 检查文档是否包含查询字段
			if fieldValue, ok := doc.data[field]; ok {
				// 将字段值转换为可比较的类型
				docValue := toComparableValue(fieldValue)
				// 检查字段值是否在查询范围内
				if compareValues(docValue, minValue) >= 0 && compareValues(docValue, maxValue) <= 0 {
					// 创建文档的副本以避免并发问题
					docCopy := make(map[string]interface{})
					for k, v := range doc.data {
						docCopy[k] = v
					}
					// 将匹配的文档添加到结果集
					results = append(results, docCopy)
				}
			}
			// 释放文档的读锁
			doc.mu.RUnlock()
			return true // 继续遍历下一个文档
		})
		// 记录全表扫描的结果数量
		db.logger.Info(fmt.Sprintf("Full scan range query on field %s returned %d results", field, len(results)))
	}

	// 返回查询结果
	return results
}
