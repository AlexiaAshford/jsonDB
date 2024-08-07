package main

import (
	"fmt"
	"github.com/AlexiaAshford/jsonDB"
	"log"
	"runtime"
	"time"
)

func main() {
	// 创建数据库实例
	db, err := jsonDB.NewDatabase("id", "./my_db", runtime.NumCPU())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 设置日志级别
	db.SetLogLevel(jsonDB.LogLevelDebug)

	// 创建索引
	db.CreateIndex("age")
	db.CreateCompositeIndex([]string{"name", "age"})

	// 插入文档
	doc1 := map[string]interface{}{
		"id":       "1",
		"name":     "Alice",
		"age":      30,
		"tags":     []string{"tag1", "tag2"},
		"joinDate": time.Now().AddDate(0, -6, 0),
		"info": map[string]interface{}{
			"email": "alice@example.com",
			"phone": "123456",
		},
	}
	err = db.Insert(doc1)
	if err != nil {
		log.Printf("Insert error: %v", err)
	}

	doc2 := map[string]interface{}{
		"id":       "2",
		"name":     "Bob",
		"age":      25,
		"tags":     []string{"tag2", "tag3"},
		"joinDate": time.Now().AddDate(0, -3, 0),
		"info": map[string]interface{}{
			"email": "bob@example.com",
			"phone": "654321",
		},
	}
	err = db.Insert(doc2)
	if err != nil {
		log.Printf("Insert error: %v", err)
	}

	doc3 := map[string]interface{}{
		"id":       "3",
		"name":     "Charlie",
		"age":      35,
		"tags":     []string{"tag1", "tag3"},
		"joinDate": time.Now().AddDate(0, -1, 0),
		"info": map[string]interface{}{
			"email": "charlie@example.com",
			"phone": "987654",
		},
	}
	err = db.Insert(doc3)
	if err != nil {
		log.Printf("Insert error: %v", err)
	}

	// 基本查询
	fmt.Println("Basic Query - Age 30:")
	results := db.Query("age", 30)
	for _, doc := range results {
		fmt.Printf("Found document: %v\n", doc)
	}

	// 复合查询
	fmt.Println("\nComposite Query - Name 'Bob' and Age 25:")
	compositeResults := db.QueryComposite([]string{"name", "age"}, []interface{}{"Bob", 25})
	for _, doc := range compositeResults {
		fmt.Printf("Found document: %v\n", doc)
	}

	// 范围查询
	fmt.Println("\nRange Query - Age between 25 and 35:")
	rangeResults := db.RangeQuery("age", 25, 35)
	for _, doc := range rangeResults {
		fmt.Printf("Found document: %v\n", doc)
	}

	// 模糊查询
	fmt.Println("\nFuzzy Query - Name starts with 'A':")
	fuzzyResults := db.FuzzyQuery("name", "A*")
	for _, doc := range fuzzyResults {
		fmt.Printf("Found document: %v\n", doc)
	}

	// 更新文档
	fmt.Println("\nUpdating document with id '1'")
	updateData := map[string]interface{}{
		"age": 31,
		"info": map[string]interface{}{
			"email": "alice.new@example.com",
		},
	}
	err = db.Update("1", updateData)
	if err != nil {
		log.Printf("Update error: %v", err)
	}

	// 获取更新后的文档
	fmt.Println("Updated document:")
	updatedDoc, exists := db.Get("1")
	if exists {
		fmt.Printf("%v\n", updatedDoc)
	}

	// 删除文档
	fmt.Println("\nDeleting document with id '2'")
	err = db.Delete("2")
	if err != nil {
		log.Printf("Delete error: %v", err)
	}

	// 获取所有文档
	fmt.Println("\nAll remaining documents:")
	allDocs := db.GetAll()
	for _, doc := range allDocs {
		fmt.Printf("%v\n", doc)
	}

	// 获取文档数量
	fmt.Printf("\nTotal documents: %d\n", db.Count())
}
