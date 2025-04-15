package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"gitlab.senseauto.com/apcloud/app/collector-app/global"
	"gitlab.senseauto.com/apcloud/app/collector-app/internal/helper"
	cmlog "gitlab.senseauto.com/apcloud/library/common-go/log"
	"log"
	"strings"
)

type FormModel interface {
	CreateTable(ctx context.Context, schema string, tablename string) error
	DeleteTable(ctx context.Context, table string) error
	InsertTableData(ctx context.Context, table string, tableData map[string]interface{}) error
	GetTableData(ctx context.Context, table string, id uint64) (map[string]interface{}, error)
	GetAllTableData(ctx context.Context, table string, id uint64) ([]map[string]interface{}, error)
	DeleteTableData(ctx context.Context, table string, ids []uint64) error
}

type formModelImpl struct {
	db *sql.DB
}

func NewFormModel() FormModel {
	return &formModelImpl{db: global.MYSQLDB}
}

func (s *formModelImpl) CreateTable(ctx context.Context, schema string, tablename string) error {
	logger := cmlog.Extract(ctx)
	tablesql, err := helper.GenerateMysqlCreateTableSQL([]byte(schema), tablename)
	if err != nil {
		logger.Error(err.Error())
	}
	res, err := s.db.Exec(tablesql)
	if err != nil {
		logger.Error(err.Error(), res)
	}
	return err
}

func (s *formModelImpl) DeleteTable(ctx context.Context, table string) error {
	logger := cmlog.Extract(ctx)
	tablesql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
	res, err := s.db.Exec(tablesql)
	if err != nil {
		logger.Error(err.Error(), res)
	}
	return err
}

func (s *formModelImpl) InsertTableData(ctx context.Context, table string, tableData map[string]interface{}) error {
	var values []interface{}
	var columns []string
	logger := cmlog.Extract(ctx)
	for k, v := range tableData {
		columns = append(columns, k)
		values = append(values, v)
	}
	// 主键相同需要覆盖
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ", "), strings.Repeat("?, ", len(columns)-1)+"?")
	res, err := s.db.Exec(query, values...)
	if err != nil {
		logger.Error(err.Error(), res)
	}
	return err
}

func (s *formModelImpl) GetTableData(ctx context.Context, table string, id uint64) (map[string]interface{}, error) {

	logger := cmlog.Extract(ctx)

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id ASC LIMIT 1", table)
	rows, err := s.db.Query(query)
	if err != nil {
		logger.Error(err.Error(), rows)
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	row := s.db.QueryRow(query)

	// 准备值的容器
	values := make([]interface{}, len(columns))
	valuePointers := make([]interface{}, len(values))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	// 查询并扫描结果
	if err := row.Scan(valuePointers...); err != nil {
		if err == sql.ErrNoRows {
			log.Fatal("Record not found.")
		} else {
			log.Fatal(err)
		}
	}

	// 创建结果的 map
	result := make(map[string]interface{})
	for i, colName := range columns {
		var value interface{}
		switch val := values[i].(type) {
		case []byte:
			value = string(val)
		case int64:
			value = val
		case float64:
			value = val
		case bool:
			value = val
		case nil:
			value = nil
		default:
			value = val
		}
		result[colName] = value
	}

	return result, nil
}

// 获取所有
func (s *formModelImpl) GetAllTableData(ctx context.Context, table string, id uint64) ([]map[string]interface{}, error) {

	results := make([]map[string]interface{}, 0)
	querySql := fmt.Sprintf("SELECT * FROM %s WHERE id = %s", table, id)
	rows, err := s.db.Query(querySql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, colName := range columns {
			var value interface{}
			if values[i] != nil {
				switch values[i][0] {
				case '"', '{', '[':
					// 如果是 JSON 格式的数据，则解码为 interface{}
					if err := json.Unmarshal(values[i], &value); err == nil {
						break
					}
					fallthrough
				default:
					value = string(values[i])
				}
			} else {
				value = nil
			}
			row[colName] = value
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *formModelImpl) DeleteTableData(ctx context.Context, table string, ids []uint64) error {
	logger := cmlog.Extract(ctx)
	// 构建查询id的占位符
	var placeholders []string
	for i := 0; i < len(ids); i++ {
		placeholders = append(placeholders, "?")
	}

	// 构建 SQL 删除语句
	query := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", table, strings.Join(placeholders, ", "))

	// 执行 SQL 删除语句
	res, err := s.db.Exec(query, toInterfaceSlice(ids)...)
	if err != nil {
		logger.Error(err.Error(), res)
	}
	return err
}

func toInterfaceSlice(ids []uint64) []interface{} {
	result := make([]interface{}, len(ids))
	for i, v := range ids {
		result[i] = v
	}
	return result
}
