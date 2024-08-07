package jsonDB

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	testDBPath    = "./test_db"
	numDocuments  = 100000
	numWorkers    = 100
	numOperations = 10000
)

func setupTestDB(t *testing.T) *Database {
	os.RemoveAll(testDBPath)
	time.Sleep(100 * time.Millisecond) // Give the OS some time to release file handles
	db, err := NewDatabase("id", testDBPath, runtime.NumCPU())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	return db
}

func cleanupTestDB(t *testing.T, db *Database) {
	// Add a small delay to ensure all async operations are completed
	time.Sleep(100 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Errorf("Failed to close database: %v", err)
	}
	os.RemoveAll(testDBPath)
}

func generateTestDocument(id int) map[string]interface{} {
	return map[string]interface{}{
		"id":    fmt.Sprintf("doc%d", id),
		"name":  fmt.Sprintf("Name%d", id),
		"age":   rand.Intn(100),
		"email": fmt.Sprintf("email%d@example.com", id),
	}
}

func TestDatabaseStressInsert(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numDocuments; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			doc := generateTestDocument(id)
			if err := db.Insert(doc); err != nil {
				t.Errorf("Failed to insert document %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	duration := time.Since(start)
	t.Logf("Inserted %d documents in %v (%.2f docs/sec)", numDocuments, duration, float64(numDocuments)/duration.Seconds())

	if count := db.Count(); count != int64(numDocuments) {
		t.Errorf("Expected %d documents, but got %d", numDocuments, count)
	}
}

func TestDatabaseStressQuery(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert test data
	for i := 0; i < numDocuments; i++ {
		doc := generateTestDocument(i)
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// Create index
	db.CreateIndex("age")

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			age := rand.Intn(100)
			results := db.Query("age", age)
			if len(results) == 0 {
				t.Errorf("No results found for age %d", age)
			}
		}()
	}

	wg.Wait()

	duration := time.Since(start)
	t.Logf("Performed %d queries in %v (%.2f queries/sec)", numOperations, duration, float64(numOperations)/duration.Seconds())
}

func TestDatabaseStressMixedOperations(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert initial test data
	for i := 0; i < numDocuments; i++ {
		doc := generateTestDocument(i)
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// Create index
	db.CreateIndex("age")

	start := time.Now()

	var wg sync.WaitGroup
	errorsChan := make(chan error, numOperations)
	semaphore := make(chan struct{}, runtime.NumCPU()*2) // Limit concurrent goroutines

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore
		go func(opID int) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			switch rand.Intn(4) {
			case 0: // Insert
				doc := generateTestDocument(numDocuments + opID)
				if err := db.Insert(doc); err != nil {
					select {
					case errorsChan <- fmt.Errorf("Insert error: %v", err):
					default:
					}
				}
			case 1: // Query
				age := rand.Intn(100)
				results := db.Query("age", age)
				if len(results) == 0 {
					select {
					case errorsChan <- fmt.Errorf("No results found for age %d", age):
					default:
					}
				}
			case 2: // Update
				id := fmt.Sprintf("doc%d", rand.Intn(numDocuments))
				updates := map[string]interface{}{
					"age": rand.Intn(100),
				}
				if err := db.Update(id, updates); err != nil {
					if err.Error() != fmt.Sprintf("document with id '%s' not found", id) {
						select {
						case errorsChan <- fmt.Errorf("Update error for document %s: %v", id, err):
						default:
						}
					}
				}
			case 3: // Delete
				id := fmt.Sprintf("doc%d", rand.Intn(numDocuments))
				if err := db.Delete(id); err != nil {
					if err != nil {
						select {
						case errorsChan <- fmt.Errorf("Delete error for document %s: %v", id, err):
						default:
						}
					}
				}
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
	close(errorsChan)

	duration := time.Since(start)
	t.Logf("Performed %d mixed operations in %v (%.2f ops/sec)", numOperations, duration, float64(numOperations)/duration.Seconds())

	// Count and log errors
	errorCount := 0
	for err := range errorsChan {
		errorCount++
		t.Logf("Operation error: %v", err)
	}

	t.Logf("Total errors: %d", errorCount)

	if errorCount > numOperations/100 { // Allow up to 1% error rate
		t.Errorf("Too many errors occurred: %d", errorCount)
	}
}

func TestDatabaseStressWithTempConcurrentReads(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert test data
	for i := 0; i < numDocuments; i++ {
		doc := generateTestDocument(i)
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				id := fmt.Sprintf("doc%d", rand.Intn(numDocuments))
				_, found := db.Get(id)
				if !found {
					t.Errorf("Document %s not found", id)
				}
			}
		}()
	}

	wg.Wait()

	duration := time.Since(start)
	t.Logf("Performed %d concurrent reads in %v (%.2f reads/sec)", numOperations, duration, float64(numOperations)/duration.Seconds())
}

func TestDatabaseStressWithTempConcurrentWrites(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations/numWorkers; j++ {
				doc := generateTestDocument(workerID*numOperations/numWorkers + j)
				if err := db.Insert(doc); err != nil {
					t.Errorf("Failed to insert document: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	duration := time.Since(start)
	t.Logf("Performed %d concurrent writes in %v (%.2f writes/sec)", numOperations, duration, float64(numOperations)/duration.Seconds())

	if count := db.Count(); count != int64(numOperations) {
		t.Errorf("Expected %d documents, but got %d", numOperations, count)
	}
}

func TestDatabaseStressWithTempDiskUsage(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert test data
	for i := 0; i < numDocuments; i++ {
		doc := generateTestDocument(i)
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// Check disk usage
	var totalSize int64
	err := filepath.Walk(testDBPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to calculate disk usage: %v", err)
	}

	t.Logf("Total disk usage for %d documents: %.2f MB", numDocuments, float64(totalSize)/(1024*1024))
}

func TestFuzzyQuery(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// 插入测试数据
	testData := []map[string]interface{}{
		{"id": "1", "name": "John Doe", "age": 30},
		{"id": "2", "name": "Jane Smith", "age": 25},
		{"id": "3", "name": "Bob Johnson", "age": 35},
		{"id": "4", "name": "Alice Brown", "age": 28},
	}

	for _, doc := range testData {
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// 创建索引
	db.CreateIndex("name")

	// 测试模糊查询
	testCases := []struct {
		pattern  string
		expected int
	}{
		{"J*", 2},
		{"*o*", 3},
		{"*Smith", 1},
		{"Alice*", 1},
		{"*z*", 0},
	}

	for _, tc := range testCases {
		results := db.FuzzyQuery("name", tc.pattern)
		if len(results) != tc.expected {
			t.Errorf("FuzzyQuery with pattern '%s' returned %d results, expected %d", tc.pattern, len(results), tc.expected)
		}
	}
}

func TestRangeQuery(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Insert test data
	testData := []map[string]interface{}{
		{"id": "1", "name": "John", "age": 30, "salary": 50000.0, "joinDate": time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"id": "2", "name": "Jane", "age": 25, "salary": 60000.0, "joinDate": time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"id": "3", "name": "Bob", "age": 35, "salary": 70000.0, "joinDate": time.Date(2019, 3, 10, 0, 0, 0, 0, time.UTC)},
		{"id": "4", "name": "Alice", "age": 28, "salary": 55000.0, "joinDate": time.Date(2022, 9, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, doc := range testData {
		if err := db.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Create index
	db.CreateIndex("age")
	db.CreateIndex("salary")
	db.CreateIndex("joinDate")

	// Test range query
	testCases := []struct {
		field    string
		min      interface{}
		max      interface{}
		expected int
	}{
		{"age", 25, 30, 3},
		{"salary", 55000.0, 70000.0, 3},
		{"joinDate", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), 2},
	}

	for _, tc := range testCases {
		results := db.RangeQuery(tc.field, tc.min, tc.max)
		if len(results) != tc.expected {
			t.Errorf("RangeQuery on field '%s' from %v to %v returned %d results, expected %d", tc.field, tc.min, tc.max, len(results), tc.expected)
			for _, doc := range results {
				t.Logf("Result: %v", doc)
			}
		}
	}
}
