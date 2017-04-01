// Copyright 2016 AlexStocks(https://github.com/AlexStocks).
// All rights reserved.  Use of m source code is
// governed by a BSD-style license.
//
// 2016-09-11 19:30
// Package gxdriver provides a MySQL driver for Go's database/sql package

package gxdriver

import (
	"database/sql"
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/AlexStocks/goext/strings"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbLock    sync.Mutex
	dbConnMap map[string]*sql.DB
)

func init() {
	dbConnMap = make(map[string]*sql.DB)
}

/*
1、极其简单灵活的事务触发以及嵌套管理。
2、灵活支持多DBSCHEMA以及读写分离。
3、长连接有效性检测和自动重连。
4、灵活的SQL增删改查操作。
*/
type MySql struct {
	db          *sql.DB //数据库操作
	tx          *sql.Tx //带事务的数据库操作
	txIndex     int     //事务记数器，只有当txIndex=0才会触发Begin动作，只有当txIndex=1才会触发Commit动作
	optype      string  //操作类型，分Master或Slave
	dbschema    string  //数据库连接schema
	activetime  int64   //上次建立连接的时间点，需要一种机制来检测客户端与mysql服务端连接的有效性
	waittimeout int64   //mysql服务器空闲等待时长
}

//创建一个默认的mysql操作实例
func NewMySql() *MySql {
	return NewMySqlInstance("default")
}

//创建一个默认的mysql查询实例
func NewQuery() *MySql {
	return NewMySqlInstance("default")
}

func NewMySqlInstance(schema string) *MySql {
	return _newMySqlInstance(schema, "Master")
}
func NewQueryInstance(schema string) *MySql {
	return _newMySqlInstance(schema, "Slave")
}

//@param string schema
//@param string conntype [Master, Slave]
func _newMySqlInstance(schema string, conntype string) *MySql {
	if !gxstrings.Contains([]string{"Master", "Slave"}, conntype) {
		panic("function common.NewSqlInstance's second argument must be 'Master' or 'Slave'.")
	}

	var key string = schema + conntype
	dbLock.Lock()
	_, ok := dbConnMap[key]
	dbLock.Unlock()
	if !ok {
		//建立一个新连接到mysql
		connect(schema, conntype)
	}
	return &MySql{db: dbConnMap[key], dbschema: schema, optype: conntype, activetime: time.Now().Unix()}
}

//建立数据库连接
//@param string schema 连接DB方案
//@param string conntype 连接类型，是分Master和Slave类型
func connect(schema string, contype string) {
	var key string = schema + contype

	//开始连接DB
	conn, err := sql.Open("mysql", schema)
	if err != nil {
		log.Fatalln(err.Error())
	}

	//将DB连接放入一个全局变量中
	dbLock.Lock()
	dbConnMap[key] = conn
	dbLock.Unlock()
}

//检查MySql实例的连接是否还在活跃时间范围内
func (m *MySql) checkActive() {
	var now int64 = time.Now().Unix()
	if m.tx != nil {
		//如果存在事务会话，则不再进行连接检查
		m.activetime = now
		return
	}

	//从MySql的wait_timeout变量中定位waittimeout
	if m.waittimeout == 0 {
		rows, err := m.db.Query("SHOW VARIABLES LIKE 'wait_timeout'")
		if err != nil {
			log.Fatalln(err.Error())
		}
		defer rows.Close()
		result, err := m.fetch(rows)
		if err != nil {
			log.Fatalln(err.Error())
		}
		if result != nil && len(result) != 0 {
			timeout, err := strconv.Atoi(result[0]["Value"])
			if err != nil {
				log.Fatalln(err.Error())
			}
			m.waittimeout = int64(timeout)
		}
	}

	if now-m.activetime > m.waittimeout-2 {
		//此时认为数据库连接已经超时了，重新进行一次连接
		connect(m.dbschema, m.optype)
		var key string = m.dbschema + m.optype
		dbLock.Lock()
		m.db = dbConnMap[key]
		dbLock.Unlock()
	}

	//设置当前时间为最新活跃点
	m.activetime = now
}

//根据所提供的sql语句获取数据列表
func (m *MySql) GetAll(querysql string, args ...interface{}) ([]map[string]string, error) {
	m.checkSQL(querysql)
	m.checkActive()
	var rows *sql.Rows
	var err error
	if m.tx != nil {
		rows, err = m.tx.Query(querysql, args...)
	} else {
		rows, err = m.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return m.fetch(rows)
}

//根据所提供的sql语句获取数据列表
func (m *MySql) GetAllArray(querysql string, args ...interface{}) ([]map[int]string, error) {
	m.checkSQL(querysql)
	m.checkActive()
	var rows *sql.Rows
	var err error
	if m.tx != nil {
		rows, err = m.tx.Query(querysql, args...)
	} else {
		rows, err = m.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return m.fetchArray(rows)
}

//根据SQL获取一行数据
func (m *MySql) GetRow(querysql string, args ...interface{}) (map[string]string, error) {
	m.checkSQL(querysql)
	if !strings.Contains(strings.ToLower(querysql), "limit") {
		querysql += " LIMIT 1"
	}
	m.checkActive()
	var rows *sql.Rows
	var err error
	if m.tx != nil {
		rows, err = m.tx.Query(querysql, args...)
	} else {
		rows, err = m.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := m.fetch(rows)
	if err != nil {
		return nil, err
	}

	retval := make(map[string]string, 0)
	if len(result) > 0 {
		retval = result[0]
	}
	return retval, nil
}

//根据SQL获取一行数据
func (m *MySql) GetRowArray(querysql string, args ...interface{}) (map[int]string, error) {
	m.checkSQL(querysql)
	if !strings.Contains(strings.ToLower(querysql), "limit") {
		querysql += " LIMIT 1"
	}
	m.checkActive()
	var rows *sql.Rows
	var err error
	if m.tx != nil {
		rows, err = m.tx.Query(querysql, args...)
	} else {
		rows, err = m.db.Query(querysql, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := m.fetchArray(rows)
	if err != nil {
		return nil, err
	}

	retval := make(map[int]string, 0)
	if len(result) > 0 {
		retval = result[0]
	}
	return retval, nil
}

//获了结果集中的第一行第一列元素值
func (m *MySql) GetOne(sql string, args ...interface{}) (string, error) {
	result, err := m.GetAllArray(sql, args...)
	if err != nil {
		return "", err
	}
	if len(result) > 0 {
		return result[0][0], nil
	}
	return "", nil
}

//根据表名、指定字段、条件获取数据列表
//参数1: 表名
//参数2: 字段列表[]string
//参数3: 查询条件map[string]string
//参数4: 排序条件string，格式如："Id DESC"
//参数5: 取记录限制string，格式如"10, 20" 或 "5"
func (m *MySql) GetList(table string, args ...interface{}) ([]map[string]string, error) {
	var fields []string = make([]string, 0)
	var conditions map[string]interface{} = make(map[string]interface{})
	var orderby string
	var limit string
	if len(args) > 0 {
		for i, v := range args {
			switch i {
			case 0:
				fields = v.([]string)
			case 1:
				conditions = v.(map[string]interface{})
			case 2:
				orderby = v.(string)
			case 3:
				limit = v.(string)
			}
		}
	}
	//开始组装SQL语句
	querysql, arguments := m.buildSql(table, fields, conditions, orderby, limit)
	return m.GetAll(querysql, arguments...)
}

//根据表名、指定字段、条件获取一条数据记录
//参数1: 表名
//参数2: 字段列表[]string
//参数3: 查询条件map[string]string
//参数4: 排序条件string，格式如："Id DESC"
func (m *MySql) GetDictionary(table string, args ...interface{}) (map[string]string, error) {
	var fields []string = make([]string, 0)
	var conditions map[string]interface{} = make(map[string]interface{})
	var orderby string
	if len(args) > 0 {
		for i, v := range args {
			switch i {
			case 0:
				fields = v.([]string)
			case 1:
				conditions = v.(map[string]interface{})
			case 2:
				orderby = v.(string)
			}
		}
	}
	//开始组装SQL语句
	querysql, arguments := m.buildSql(table, fields, conditions, orderby, "1")
	return m.GetRow(querysql, arguments...)
}

func Empty(a []string) bool {
	if a == nil || len(a) == 0 {
		return true
	}

	return false
}

func (m *MySql) buildSql(table string, fields []string, conditions map[string]interface{}, orderby, limit string) (string, []interface{}) {
	//拼接field部分
	var fieldstr string = "*"
	if !Empty(fields) {
		fieldstr = strings.Join(fields, ", ")
	}

	//order by
	if orderby != "" {
		orderby = " ORDER BY " + orderby
	}

	//定位where子句部分
	where, arguments := m.buildWhere(conditions)

	//limit
	if limit != "" {
		limit = " LIMIT " + limit
	}
	querysql := "SELECT " + fieldstr + " FROM " + table + where + orderby + limit
	m.checkSQL(querysql)
	return querysql, arguments
}

func StringToList(s string) []string {
	return strings.Split(s, ",")
}

//通过条件的k-v形式获取SQL中的where子句部分
//返回包括两部分: 1、where子句拼装并包括?占位符；2、?占位符参数列表
func (m *MySql) buildWhere(conditions map[string]interface{}) (where string, args []interface{}) {
	//拼接condition条件部分
	var condlist []string = make([]string, 0)
	for k, v := range conditions {
		switch v.(type) {
		case string:
			v := v.(string)
			vlist := StringToList(v)
			if len(vlist) == 1 {
				args = append(args, vlist[0])
				condlist = append(condlist, k+"=?")
			} else if len(vlist) > 1 {
				placeholders := make([]string, 0)
				for _, val := range vlist {
					args = append(args, val)
					placeholders = append(placeholders, "?")
				}
				condlist = append(condlist, k+" IN("+strings.Join(placeholders, ",")+")")
			}
		default:
			args = append(args, v)
			condlist = append(condlist, k+"=?")
		}
	}
	if !Empty(condlist) {
		where = " WHERE " + strings.Join(condlist, " AND ")
	}
	return
}

//根据db.Query的查询结果，组装成一个关联key的数据集，数据类型[]map[string]string
func (m *MySql) fetch(rows *sql.Rows) ([]map[string]string, error) {
	result := make([]map[string]string, 0)
	columns, err := rows.Columns()
	if err != nil {
		//an error occurred
		return nil, err
	}

	rawBytes := make([]sql.RawBytes, len(columns))

	//rows.Scan wants '[]interface{}' as an argument, so we must copy
	//the references into such a slice
	scanArgs := make([]interface{}, len(columns))

	for i := range rawBytes {
		scanArgs[i] = &rawBytes[i]
	}

	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		var val string
		item := make(map[string]string)
		for i, col := range rawBytes {
			if col == nil {
				val = ""
			} else {
				val = string(col)
			}
			item[columns[i]] = val
		}
		result = append(result, item)
	}
	return result, nil
}

//根据db.Query的查询结果，组装成一个array的数据集，数据类型[]map[int]string
func (m *MySql) fetchArray(rows *sql.Rows) ([]map[int]string, error) {
	result := make([]map[int]string, 0)
	columns, err := rows.Columns()
	if err != nil {
		//an error occurred
		return nil, err
	}

	rawBytes := make([]sql.RawBytes, len(columns))

	//rows.Scan wants '[]interface{}' as an argument, so we must copy
	//the references into such a slice
	scanArgs := make([]interface{}, len(columns))

	for i := range rawBytes {
		scanArgs[i] = &rawBytes[i]
	}

	item := make(map[int]string)
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		var val string
		for i, col := range rawBytes {
			if col == nil {
				val = ""
			} else {
				val = string(col)
			}
			item[i] = val
		}
		result = append(result, item)
	}
	return result, nil
}

//向前一个表中写入一条记录，如果写入成功，则返回其主键ID值
func (m *MySql) Insert(table string, data map[string]interface{}) (int64, error) {
	columns, err := m.GetTableColumns(table)
	if err != nil {
		return 0, err
	}
	for key, _ := range data {
		if _, ok := columns[key]; !ok {
			delete(data, key)
		}
	}
	if data == nil || len(data) == 0 {
		return 0, errors.New("insert data invalid.")
	}

	fields := make([]string, 0)
	args := make([]interface{}, 0)
	placeholders := make([]string, 0)
	for key, value := range data {
		fields = append(fields, key)
		args = append(args, value)
		placeholders = append(placeholders, "?")
	}
	var sql string = "INSERT INTO " + table + "(" + strings.Join(fields, ", ") + ") VALUES(" + strings.Join(placeholders, ", ") + ")"
	return m.InsertExec(sql, args...)
}

func (m *MySql) Update(table string, data map[string]interface{}, conditions map[string]interface{}) (int64, error) {
	columns, err := m.GetTableColumns(table)
	if err != nil {
		return 0, err
	}

	//数据过滤
	for key, _ := range data {
		if _, ok := columns[key]; !ok {
			delete(data, key)
		}
	}

	//检查过滤后的数据是否为空
	if data == nil || len(data) == 0 {
		return 0, errors.New("update data invalid")
	}

	args := make([]interface{}, 0)

	//拼装待更新的数据
	fields := make([]string, 0)
	for key, value := range data {
		fields = append(fields, key+"=?")
		args = append(args, value)
	}

	//拼装where子句
	where, arguments := m.buildWhere(conditions)
	if arguments != nil && len(arguments) != 0 {
		args = append(args, arguments...)
	}

	var sql string = "UPDATE " + table + " SET " + strings.Join(fields, ", ") + where
	m.checkSQL(sql)

	return m.UDExec(sql, args...)
}

//删除数据库记录，如果删除成功，则返回影响的记录条数
func (m *MySql) Delete(table string, conditions map[string]interface{}) (int64, error) {
	where, args := m.buildWhere(conditions)
	var sql string = "DELETE FROM " + table + where
	m.checkSQL(sql)
	return m.UDExec(sql, args...)
}

//执行一条写入的SQL语句，如果成功则返回上次写入的主键ID
func (m *MySql) InsertExec(execsql string, args ...interface{}) (int64, error) {
	m.checkSQL(execsql)
	m.checkActive()
	var result sql.Result
	var err error
	if m.tx != nil {
		result, err = m.tx.Exec(execsql, args...)
	} else {
		result, err = m.db.Exec(execsql, args...)
	}

	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

//执行更新或删除的SQL语句，如果成功则返回影响的记录条数
func (m *MySql) UDExec(execsql string, args ...interface{}) (int64, error) {
	m.checkSQL(execsql)
	m.checkActive()
	var result sql.Result
	var err error
	if m.tx != nil {
		result, err = m.tx.Exec(execsql, args...)
	} else {
		result, err = m.db.Exec(execsql, args...)
	}

	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

//获取一个表的列信息，字段名称为key，值为字段信息k-v
func (m *MySql) GetTableColumns(table string) (map[string]map[string]string, error) {
	list, err := m.GetAll("DESC " + table)
	if err != nil {
		return nil, err
	}
	retval := make(map[string]map[string]string, 0)
	for _, item := range list {
		retval[item["Field"]] = item
	}
	return retval, nil
}

//保证修改、写入类的操作不在slave上执行
func (m *MySql) checkSQL(sql string) {
	if m.optype == "Slave" {
		sql = strings.TrimSpace(sql)
		exp := regexp.MustCompile(`^(?i:insert|update|delete|alter|truncate|drop)`)
		if exp.MatchString(sql) {
			panic("insert|update|delete|alter|truncate|drop operation is not allowed in slave.")
		}
	}
}

//开始一个事务，开始一个事务和提交、回滚事务必须一一对应
func (m *MySql) Begin() {
	if m.txIndex == 0 {
		var err error
		m.tx, err = m.db.Begin()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
	m.txIndex++
}

//提交一个事务
func (m *MySql) Commit() {
	if m.txIndex == 1 {
		if err := m.tx.Commit(); err != nil {
			log.Fatalln(err.Error())
		}
		m.tx = nil
	}
	m.txIndex--
}

//事务回滚
func (m *MySql) Rollback() {
	if err := m.tx.Rollback(); err != nil {
		log.Fatalln(err.Error())
	}
	m.txIndex = 0
	m.tx = nil
}
