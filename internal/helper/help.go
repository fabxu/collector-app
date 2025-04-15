package helper

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type FormSchema struct {
	Title       string                 `json:"title"`
	Type        string                 `json:"type"`
	Properties  map[string]interface{} `json:"properties"`
	Required    []string               `json:"required"`
	Description string                 `json:"description"`
}

// PropertyType 用于表示 JSON Schema 属性的 mysql 字段类型。
type PropertyType struct {
	Name        string
	MysqlType   string
	IsNullable  bool
	Constraints []string
}

func GenerateMysqlCreateTableSQL(schema []byte, tableName string) (string, error) {
	var s FormSchema
	if err := json.Unmarshal(schema, &s); err != nil {
		return "", err
	}

	properties := s.Properties
	var sqlBuilder strings.Builder

	sqlBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", tableName))
	sqlBuilder.WriteString("\tid INT AUTO_INCREMENT PRIMARY KEY,\n")

	for propName, propValue := range properties {
		if propName == "id" {
			continue
		}
		property, err := getPropertyDetails(propName, propValue)
		if err != nil {
			return "", err
		}
		sqlBuilder.WriteString(fmt.Sprintf("\t%s %s", propName, property.MysqlType))
		if !property.IsNullable {
			sqlBuilder.WriteString(" NOT NULL")
		}
		if len(property.Constraints) > 0 {
			for _, constraint := range property.Constraints {
				sqlBuilder.WriteString(fmt.Sprintf(" %s", constraint))
			}
		}
		sqlBuilder.WriteString(",\n")
	}

	// 去掉最后一行空行以及最后一个逗号
	text := sqlBuilder.String()
	lastNewlineIndex := strings.LastIndex(text, "\n")
	if lastNewlineIndex >= 0 {
		sqlBuilder.Reset()                                // 清空 Builder
		sqlBuilder.WriteString(text[:lastNewlineIndex-1]) // 重新写入截断后的字符串
	} else {
		// 如果没有找到换行符，则可能是字符串为空或没有换行符
		sqlBuilder.Reset() // 清空 Builder
	}

	sqlBuilder.WriteString("\n);")
	return sqlBuilder.String(), nil
}

// 获取属性的详细信息
func getPropertyDetails(name string, value interface{}) (PropertyType, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		if typeStr, ok := v["type"].(string); ok {
			switch typeStr {
			case "integer":
				return PropertyType{Name: name, MysqlType: "INT"}, nil
			case "string":
				if format, ok := v["format"].(string); ok && format == "email" {
					return PropertyType{Name: name, MysqlType: "VARCHAR(255)", Constraints: []string{"UNIQUE"}}, nil
				}
				return PropertyType{Name: name, MysqlType: "VARCHAR(255)"}, nil
			case "boolean":
				return PropertyType{Name: name, MysqlType: "BOOLEAN"}, nil
			default:
				return PropertyType{}, fmt.Errorf("unsupported type: %s", typeStr)
			}
		}
	default:
		return PropertyType{}, fmt.Errorf("invalid property value: %v", value)
	}
	return PropertyType{}, fmt.Errorf("unsupported property value: %v", value)
}

func QueryAndConvertToMap(db *sql.DB, query string) (results []map[string]interface{}, err error) {

	return results, nil
}
