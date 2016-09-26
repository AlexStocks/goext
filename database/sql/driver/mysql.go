/******************************************************
# DESC    : mysql operation based on go-mysql-driver
# AUTHOR  : Alex Stocks
# LICENCE : Apache License 2.0
# MOD     : 2016-09-11 19:30
# FILE    : mysql.go
******************************************************/

package gxdriver

import (
	"const/path"
	"database/sql"
	"errors"
	_ "github.com/go-mysql-driver"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var db map[string]*sql.DB

func init() {
	db = make(map[string]*sql.DB)
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

//实例化一个mysql实例
//@param string schema 数据库连接方案
//@param string conntype 数据库连接类型，范围: Master, Slave
func _newMySqlInstance(schema string, conntype string) *MySql {
	if !InList(conntype, []string{"Master", "Slave"}) {
		panic("function common.NewSqlInstance's second argument must be Master or Slave.")
	}

	var key string = BuildKeyMd5(schema, conntype)
	_, ok := db[key]
	if !ok {
		//建立一个新连接到mysql
		connect(schema, conntype)
	}
	return &MySql{db: db[key], dbschema: schema, optype: conntype, activetime: time.Now().Unix()}
}

//建立数据库连接
//@param string schema 连接DB方案
//@param string conntype 连接类型，是分Master和Slave类型
func connect(schema string, contype string) {
	type item struct {
		Master string
		Slave  []string
	}
	var v map[string]item

	//获取DB连接配置文件
	if err := LoadJson(path.CONFIG_PATH+"db.json", &v); err != nil {
		log.Fatalln(err.Error())
	}
	conf, ok := v[schema]
	if !ok {
		log.Fatalln("Database configuration file error. Lost schema[" + schema + "] node.")
	}
	var dataSourceName string
	var key string = BuildKeyMd5(schema, contype)
	if contype == "Master" {
		dataSourceName = conf.Master
	} else {
		dataSourceName = conf.Slave[Rand(0, len(conf.Slave)-1)]
	}

	//开始连接DB
	dbinit, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	//将DB连接放入一个全局变量中
	db[key] = dbinit
}

//检查MySql实例的连接是否还在活跃时间范围内
func (this *MySql) checkActive() {
	var now int64 = time.Now().Unix()
	if this.tx != nil {
		//如果存在事务会话，则不再进行连接检查
		this.activetime = now
		return
	}

	//从MySql的wait_timeout变量中定位waittimeout
	if this.waittimeout == 0 {
		rows, err := this.db.Query("SHOW VARIABLES LIKE 'wait_timeout'")
		if err != nil {
			log.Fatalln(err.Error())
		}
		defer rows.Close()
		result, err := this.fetch(rows)
		if err != nil {
			log.Fatalln(err.Error())
		}
		if !Empty(result) {
			timeout, err := strconv.Atoi(result[0]["Value"])
			if err != nil {
				log.Fatalln(err.Error())
			}
			this.waittimeout = int64(timeout)
		}
	}

	if now-this.activetime > this.waittimeout-2 {
		//此时认为数据库连接已经超时了，重新进行一次连接
		connect(this.dbschema, this.optype)
		var key string = BuildKeyMd5(this.dbschema, this.optype)
		this.db = db[key]
	}

	//设置当前时间为最新活跃点
	this.activetime = now
}

//根据所提供的sql语句获取数据列表
func (this *MySql) GetAll(querysql string, args ...interface{}) ([]map[string]string, error) {
	this.checkSQL(querysql)
	this.checkActive()
	var rows *sql.Rows
	var err error
	if this.tx != nil {
		rows, err = this.tx.Query(querysql, args...)
	} else {
		rows, err = this.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return this.fetch(rows)
}

//根据所提供的sql语句获取数据列表
func (this *MySql) GetAllArray(querysql string, args ...interface{}) ([]map[int]string, error) {
	this.checkSQL(querysql)
	this.checkActive()
	var rows *sql.Rows
	var err error
	if this.tx != nil {
		rows, err = this.tx.Query(querysql, args...)
	} else {
		rows, err = this.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return this.fetchArray(rows)
}

//根据SQL获取一行数据
func (this *MySql) GetRow(querysql string, args ...interface{}) (map[string]string, error) {
	this.checkSQL(querysql)
	if !strings.Contains(strings.ToLower(querysql), "limit") {
		querysql += " LIMIT 1"
	}
	this.checkActive()
	var rows *sql.Rows
	var err error
	if this.tx != nil {
		rows, err = this.tx.Query(querysql, args...)
	} else {
		rows, err = this.db.Query(querysql, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := this.fetch(rows)
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
func (this *MySql) GetRowArray(querysql string, args ...interface{}) (map[int]string, error) {
	this.checkSQL(querysql)
	if !strings.Contains(strings.ToLower(querysql), "limit") {
		querysql += " LIMIT 1"
	}
	this.checkActive()
	var rows *sql.Rows
	var err error
	if this.tx != nil {
		rows, err = this.tx.Query(querysql, args...)
	} else {
		rows, err = this.db.Query(querysql, args...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := this.fetchArray(rows)
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
func (this *MySql) GetOne(sql string, args ...interface{}) (string, error) {
	result, err := this.GetAllArray(sql, args...)
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
func (this *MySql) GetList(table string, args ...interface{}) ([]map[string]string, error) {
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
	querysql, arguments := this.buildSql(table, fields, conditions, orderby, limit)
	return this.GetAll(querysql, arguments...)
}

//根据表名、指定字段、条件获取一条数据记录
//参数1: 表名
//参数2: 字段列表[]string
//参数3: 查询条件map[string]string
//参数4: 排序条件string，格式如："Id DESC"
func (this *MySql) GetDictionary(table string, args ...interface{}) (map[string]string, error) {
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
	querysql, arguments := this.buildSql(table, fields, conditions, orderby, "1")
	return this.GetRow(querysql, arguments...)
}

func (this *MySql) buildSql(table string, fields []string, conditions map[string]interface{}, orderby, limit string) (string, []interface{}) {
	//拼接field部分
	var fieldstr string = "*"
	if !Empty(fields) {
		fieldstr = strings.Join(fields, ", ")
	}

	//order by
	if !Empty(orderby) {
		orderby = " ORDER BY " + orderby
	}

	//定位where子句部分
	where, arguments := this.buildWhere(conditions)

	//limit
	if !Empty(limit) {
		limit = " LIMIT " + limit
	}
	querysql := "SELECT " + fieldstr + " FROM " + table + where + orderby + limit
	this.checkSQL(querysql)
	return querysql, arguments
}

//通过条件的k-v形式获取SQL中的where子句部分
//返回包括两部分: 1、where子句拼装并包括?占位符；2、?占位符参数列表
func (this *MySql) buildWhere(conditions map[string]interface{}) (where string, args []interface{}) {
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
func (this *MySql) fetch(rows *sql.Rows) ([]map[string]string, error) {
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
func (this *MySql) fetchArray(rows *sql.Rows) ([]map[int]string, error) {
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
func (this *MySql) Insert(table string, data map[string]interface{}) (int64, error) {
	columns, err := this.GetTableColumns(table)
	if err != nil {
		return 0, err
	}
	for key, _ := range data {
		if _, ok := columns[key]; !ok {
			delete(data, key)
		}
	}
	if Empty(data) {
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
	return this.InsertExec(sql, args...)
}

func (this *MySql) Update(table string, data map[string]interface{}, conditions map[string]interface{}) (int64, error) {
	columns, err := this.GetTableColumns(table)
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
	if Empty(data) {
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
	where, arguments := this.buildWhere(conditions)
	if !Empty(arguments) {
		args = append(args, arguments...)
	}

	var sql string = "UPDATE " + table + " SET " + strings.Join(fields, ", ") + where
	this.checkSQL(sql)

	return this.UDExec(sql, args...)
}

//删除数据库记录，如果删除成功，则返回影响的记录条数
func (this *MySql) Delete(table string, conditions map[string]interface{}) (int64, error) {
	where, args := this.buildWhere(conditions)
	var sql string = "DELETE FROM " + table + where
	this.checkSQL(sql)
	return this.UDExec(sql, args...)
}

//执行一条写入的SQL语句，如果成功则返回上次写入的主键ID
func (this *MySql) InsertExec(execsql string, args ...interface{}) (int64, error) {
	this.checkSQL(execsql)
	this.checkActive()
	var result sql.Result
	var err error
	if this.tx != nil {
		result, err = this.tx.Exec(execsql, args...)
	} else {
		result, err = this.db.Exec(execsql, args...)
	}

	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

//执行更新或删除的SQL语句，如果成功则返回影响的记录条数
func (this *MySql) UDExec(execsql string, args ...interface{}) (int64, error) {
	this.checkSQL(execsql)
	this.checkActive()
	var result sql.Result
	var err error
	if this.tx != nil {
		result, err = this.tx.Exec(execsql, args...)
	} else {
		result, err = this.db.Exec(execsql, args...)
	}

	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

//获取一个表的列信息，字段名称为key，值为字段信息k-v
func (this *MySql) GetTableColumns(table string) (map[string]map[string]string, error) {
	list, err := this.GetAll("DESC " + table)
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
func (this *MySql) checkSQL(sql string) {
	if this.optype == "Slave" {
		sql = strings.TrimSpace(sql)
		exp := regexp.MustCompile(`^(?i:insert|update|delete|alter|truncate|drop)`)
		if exp.MatchString(sql) {
			panic("insert|update|delete|alter|truncate|drop operation is not allowed in slave.")
		}
	}
}

//开始一个事务，开始一个事务和提交、回滚事务必须一一对应
func (this *MySql) Begin() {
	if this.txIndex == 0 {
		var err error
		this.tx, err = this.db.Begin()
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
	this.txIndex++
}

//提交一个事务
func (this *MySql) Commit() {
	if this.txIndex == 1 {
		if err := this.tx.Commit(); err != nil {
			log.Fatalln(err.Error())
		}
		this.tx = nil
	}
	this.txIndex--
}

//事务回滚
func (this *MySql) Rollback() {
	if err := this.tx.Rollback(); err != nil {
		log.Fatalln(err.Error())
	}
	this.txIndex = 0
	this.tx = nil
}
