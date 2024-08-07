// trie.go

// 介绍:
// 本文件实现了一个特殊的Trie数据结构,用于支持jsonDB中的模糊查询功能。
// Trie(字典树或前缀树)是一种树形数据结构,用于高效地存储和检索字符串数据集中的键。
// 这个实现特别适用于支持通配符(*)的模糊查询,允许在查询中使用前缀、后缀或中间匹配。
//
// 主要特性:
// 1. 支持插入和删除操作
// 2. 实现了模糊搜索功能,支持通配符(*)
// 3. 线程安全,使用互斥锁保护并发访问
// 4. 每个节点维护一个文档ID集合,支持快速检索匹配的文档
//
// 使用场景:
// 这个Trie结构主要用于jsonDB的索引系统,特别是在支持字符串字段的模糊查询时。
// 它能够高效地处理如"find all documents where name starts with 'Jo*'"这样的查询。

package jsonDB

import (
	"sync"
)

// TrieNode 结构体表示Trie中的一个节点
type TrieNode struct {
	children map[rune]*TrieNode // 子节点映射,key是字符,value是对应的子节点
	docs     *sync.Map          // 存储与该节点关联的文档ID,使用sync.Map确保并发安全
}

// Trie 结构体表示整个Trie树
type Trie struct {
	root *TrieNode    // 根节点
	mu   sync.RWMutex // 读写锁,用于保护整个Trie的并发访问
}

// NewTrie 函数创建并返回一个新的Trie实例
func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{
			children: make(map[rune]*TrieNode), // 初始化根节点的子节点映射
			docs:     &sync.Map{},              // 初始化根节点的文档ID集合
		},
	}
}

// Insert 方法向Trie中插入一个单词和对应的文档ID
// word: 要插入的单词
// docID: 与该单词关联的文档ID
func (t *Trie) Insert(word string, docID string) {
	t.mu.Lock()         // 获取写锁,确保并发安全
	defer t.mu.Unlock() // 确保函数结束时释放锁

	node := t.root              // 从根节点开始
	for _, char := range word { // 遍历单词的每个字符
		if _, ok := node.children[char]; !ok {
			// 如果当前字符的子节点不存在,创建一个新的子节点
			node.children[char] = &TrieNode{
				children: make(map[rune]*TrieNode),
				docs:     &sync.Map{},
			}
		}
		node = node.children[char]         // 移动到子节点
		node.docs.Store(docID, struct{}{}) // 在当前节点存储文档ID
	}
}

// Search 方法在Trie中搜索匹配给定模式的所有文档ID
// pattern: 搜索模式,可以包含通配符(*)
// 返回一个sync.Map,包含所有匹配的文档ID
func (t *Trie) Search(pattern string) *sync.Map {
	t.mu.RLock()         // 获取读锁,允许并发读取
	defer t.mu.RUnlock() // 确保函数结束时释放锁

	results := &sync.Map{}                      // 存储搜索结果
	t.searchRecursive(t.root, pattern, results) // 开始递归搜索
	return results
}

// searchRecursive 是一个递归辅助函数,用于执行实际的模糊搜索
// node: 当前正在检查的节点
// pattern: 剩余的搜索模式
// results: 用于收集匹配的文档ID
func (t *Trie) searchRecursive(node *TrieNode, pattern string, results *sync.Map) {
	if len(pattern) == 0 {
		// 如果模式为空,说明已经完全匹配,将当前节点的所有文档ID添加到结果中
		node.docs.Range(func(key, value interface{}) bool {
			results.Store(key, value)
			return true
		})
		return
	}

	if pattern[0] == '*' {
		// 如果遇到通配符,有两种可能:
		// 1. 通配符匹配0个字符,直接跳过它
		// 2. 通配符匹配1个或多个字符,继续搜索所有子节点
		for _, child := range node.children {
			t.searchRecursive(child, pattern, results)
		}
		t.searchRecursive(node, pattern[1:], results)
	} else {
		// 如果是普通字符,检查是否有匹配的子节点
		if child, ok := node.children[rune(pattern[0])]; ok {
			t.searchRecursive(child, pattern[1:], results)
		}
	}
}

// Remove 方法从Trie中删除一个单词和对应的文档ID
// word: 要删除的单词
// docID: 要删除的文档ID
func (t *Trie) Remove(word string, docID string) {
	t.mu.Lock()         // 获取写锁,确保并发安全
	defer t.mu.Unlock() // 确保函数结束时释放锁

	node := t.root
	var path []*TrieNode // 用于记录遍历路径
	for _, char := range word {
		if next, ok := node.children[char]; ok {
			path = append(path, node)
			node = next
		} else {
			// 如果单词不存在于trie中,直接返回
			return
		}
	}

	// 从文档列表中移除docID
	node.docs.Delete(docID)

	// 如果这个节点没有其他文档并且没有子节点,我们可以删除它
	for i := len(path) - 1; i >= 0; i-- {
		parent := path[i]
		char := rune(word[i])

		if len(node.children) == 0 && syncMapSize(node.docs) == 0 {
			delete(parent.children, char)
			node = parent
		} else {
			// 如果节点还有其他文档或子节点,停止删除
			break
		}
	}
}

// syncMapSize 是一个辅助函数,用于获取sync.Map的大小
// m: 要检查大小的sync.Map
// 返回sync.Map中的键值对数量
func syncMapSize(m *sync.Map) int {
	size := 0
	m.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}
