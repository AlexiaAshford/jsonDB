package jsonDB

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

// DocumentData 关联 map[string]interface{}
type DocumentData map[string]interface{}

const (
	// 数据库文件名
	DataFileName = "data.db"
	WALFileName  = "wal.log"

	// 文件权限
	DBDirPerm  = 0755
	DBFilePerm = 0644

	// 操作类型
	OperationInsert = "INSERT"
	OperationUpdate = "UPDATE"
	OperationDelete = "DELETE"

	// 文件打开模式
	FileOpenModeRW  = os.O_RDWR | os.O_CREATE
	FileOpenModeWAL = os.O_RDWR | os.O_CREATE | os.O_TRUNC
)

func toFloat64(v interface{}) float64 {
	switch value := v.(type) {
	case int:
		return float64(value)
	case int64:
		return float64(value)
	case float32:
		return float64(value)
	case float64:
		return value
	default:
		// 如果无法转换，返回 NaN
		return math.NaN()
	}
}

func compareValues(a, b interface{}) int {
	aValue := toComparableValue(a)
	bValue := toComparableValue(b)

	switch va := aValue.(type) {
	case int64:
		vb, ok := bValue.(int64)
		if !ok {
			vb = int64(bValue.(float64))
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case float64:
		vb, ok := bValue.(float64)
		if !ok {
			vb = float64(bValue.(int64))
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		return strings.Compare(va, bValue.(string))
	default:
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		return strings.Compare(aStr, bStr)
	}
}

func toComparableValue(v interface{}) interface{} {
	switch value := v.(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float32:
		return float64(value)
	case float64:
		return value
	case time.Time:
		return value.Unix()
	default:
		return v
	}
}
