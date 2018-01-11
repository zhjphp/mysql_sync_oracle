package models

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
)

type mysqlConf struct {
	dbType    string
	user      string
	pwd       string
	host      string
	port      string
	collation string
	timeout   string
	protocol  string
	Schema    string
	LogData   *DoLog
}

type mysqlTable struct {
	Db          *sql.DB
	Schema      string
	FieldsStr   string
	FieldsCount int
	FieldsSlice []string
	Ct          [][]*sql.ColumnType
	FieldsRows  *sql.Rows
	Row         *sql.Row
	Rows        *sql.Rows
	FieldsMap   []map[string]interface{}
	LogData     *DoLog
}

type oracleConf struct {
	dbType  string
	user    string
	pwd     string
	host    string
	port    string
	sid     string
	LogData *DoLog
}

type oracleTable struct {
	Db               *sql.DB
	FieldsValueSlice []interface{}
	LogData          *DoLog
}

type DoLog struct {
	Action  string
	TbName  string
	PkName  string
	PkValue string
}

func Sync(action string, tbName string, pkName string, pkValue string) string {
	time.Sleep(4 * 1000 * time.Millisecond)

	var logData *DoLog
	logData = new(DoLog)
	logData.Action = action
	logData.TbName = tbName
	logData.PkName = pkName
	logData.PkValue = pkValue

	var result string
	switch action {
	case "insert":
		//链接mysql
		mysTb := mysCon(logData)
		//读取一行
		mysTb.GetRows(tbName, pkName, pkValue)
		//链接oracle
		oraTb := oraCon(logData)
		//插入oracle
		result = oraTb.InsertRow(tbName, pkName, pkValue, mysTb)
		break
	case "update":
		//链接mysql
		mysTb := mysCon(logData)
		//读取一行
		mysTb.GetRows(tbName, pkName, pkValue)
		//链接oracle
		oraTb := oraCon(logData)
		//更新oracle
		result = oraTb.UpdateRow(tbName, pkName, pkValue, mysTb)
		break
	case "delete":
		//链接oracle
		oraTb := oraCon(logData)
		//删除oracle
		result = oraTb.DeleteRow(tbName, pkName, pkValue)
		break
	default:
		//fmt.Println("act error")
		result = "action error"
	}

	return result
}

func mysCon(logData *DoLog) *mysqlTable {
	//localhost mysql数据库配置
	var mysConf *mysqlConf
	mysConf = new(mysqlConf)

	mysConf.dbType = ""
	mysConf.protocol = ""
	mysConf.host = ""
	mysConf.port = ""
	mysConf.user = ""
	mysConf.collation = ""
	mysConf.timeout = ""
	mysConf.pwd = ""
	mysConf.Schema = ""
	mysConf.LogData = logData

	var mysTb *mysqlTable
	mysTb = new(mysqlTable)
	mysTb.Schema = mysConf.Schema
	mysTb.Db = mysConf.OpenDbMysql()

	mysTb.LogData = logData

	return mysTb
}

func oraCon(logData *DoLog) *oracleTable {
	//oracle数据库配置
	var oraConf *oracleConf
	oraConf = new(oracleConf)
	oraConf.dbType = ""
	oraConf.host = ""
	oraConf.port = ""
	oraConf.user = ""
	oraConf.pwd = ""
	oraConf.sid = ""
	oraConf.LogData = logData

	//链接oracle数据库
	var oraTb *oracleTable
	oraTb = new(oracleTable)
	oraTb.Db = oraConf.OpenDbOracle()

	oraTb.LogData = logData

	return oraTb
}

//删除 oracle 中的一行数据
func (tb *oracleTable) DeleteRow(tbName string, pkName string, pkValue string) string {
	var err error
	query := fmt.Sprintf(" DELETE FROM %s WHERE %s = :pkValue ", tbName, pkName)
	//执行 INSERT
	var stmt *sql.Stmt
	stmt, err = tb.Db.Prepare(query)
	if err != nil {
		tb.LogData.LogToFile("1", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	defer stmt.Close()
	var result sql.Result //执行结果
	result, err = stmt.Exec(pkValue)
	if err != nil {
		tb.LogData.LogToFile("2", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//响应行数
	var rowsAffected int64
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		tb.LogData.LogToFile("3", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//	fmt.Println(rowsAffected)
	if rowsAffected != 1 {
		//fmt.Println("error,DELETE," + tbName + "," + pkName + "," + pkValue)
		tb.LogData.LogToFile("4", err.Error())
		return "error"
	} else {
		//fmt.Println("success")
		return "success"
	}
}

//插入 oracle 中的一行数据
func (tb *oracleTable) InsertRow(tbName string, pkName string, pkValue string, mysTb *mysqlTable) string {
	var columnNames string
	var columnValues string
	var err error
	//拼装 INSERT 语句
	query := fmt.Sprintf(" INSERT INTO %s ", tbName)
	for i, _ := range mysTb.FieldsSlice {
		columnNames += mysTb.FieldsSlice[i]
		columnValues += ":" + mysTb.FieldsSlice[i]
		if (i + 1) < mysTb.FieldsCount {
			columnNames += ", "
			columnValues += ", "
		}
	}
	columnNames = "(" + columnNames + ")"
	columnValues = "(" + columnValues + ")"
	query += columnNames + " VALUES " + columnValues
	//	fmt.Println(query)
	//执行 INSERT
	var stmt *sql.Stmt
	stmt, err = tb.Db.Prepare(query)
	if err != nil {
		tb.LogData.LogToFile("5", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	defer stmt.Close()
	var result sql.Result //执行结果
	//创建 Exec 时用的 slice
	tb.MakeOracleFieldsValueSlice(mysTb)
	result, err = stmt.Exec(tb.FieldsValueSlice...)
	if err != nil {
		tb.LogData.LogToFile("6", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//响应行数
	var rowsAffected int64
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		tb.LogData.LogToFile("7", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//	fmt.Println(rowsAffected)
	if rowsAffected != 1 {
		//fmt.Println("error,INSERT," + tbName + "," + pkName + "," + pkValue)
		tb.LogData.LogToFile("8", err.Error())
		return "error"
	} else {
		//fmt.Println("success")
		return "success"
	}
}

//更新 oracle 中的一行数据
func (tb *oracleTable) UpdateRow(tbName string, pkName string, pkValue string, mysTb *mysqlTable) string {
	var set string
	var err error
	//拼装 UPDATE 语句
	for i, _ := range mysTb.FieldsSlice {
		k := mysTb.FieldsSlice[i]
		v := ":" + k
		set += k + " = " + v
		if (i + 1) < mysTb.FieldsCount {
			set += ", "
		}
	}
	query := fmt.Sprintf(" UPDATE %s SET ", tbName)
	where := fmt.Sprintf(" WHERE %s = :%s ", pkName, pkName)
	query += set
	query += where
	//	fmt.Println(query)
	//执行 UPDATE
	var stmt *sql.Stmt
	stmt, err = tb.Db.Prepare(query)
	if err != nil {
		tb.LogData.LogToFile("9", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	defer stmt.Close()
	//创建 Exec 时用的 slice
	tb.MakeOracleFieldsValueSlice(mysTb)
	tb.FieldsValueSlice = append(tb.FieldsValueSlice, pkValue)
	var result sql.Result //执行结果
	result, err = stmt.Exec(tb.FieldsValueSlice...)
	if err != nil {
		tb.LogData.LogToFile("10", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//响应行数
	var rowsAffected int64
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		tb.LogData.LogToFile("11", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}
	//	fmt.Println(rowsAffected)
	if rowsAffected != 1 {
		//fmt.Println("error,UPDATE," + tbName + "," + pkName + "," + pkValue)
		tb.LogData.LogToFile("12", err.Error())
		return "error"
	} else {
		//fmt.Println("success")
		return "success"
	}
}

//创建 oracle 要处理的 row 中值的slice
func (tb *oracleTable) MakeOracleFieldsValueSlice(mysTb *mysqlTable) {
	tb.FieldsValueSlice = make([]interface{}, mysTb.FieldsCount)
	for i, _ := range mysTb.FieldsSlice {
		tb.FieldsValueSlice[i] = mysTb.FieldsMap[0][mysTb.FieldsSlice[i]]
	}
}

//打开数据库
func (conf *oracleConf) OpenDbOracle() *sql.DB {
	var err error
	var db *sql.DB
	dsn := fmt.Sprintf("%s:%s@%s:%s/%s", conf.user, conf.pwd, conf.host, conf.port, conf.sid)
	db, err = sql.Open(conf.dbType, dsn)
	if err != nil {
		conf.LogData.LogToFile("14", err.Error())
		log.Println(err)
		//		log.Fatal(err.Error())
	}
	return db
}

//获取目标行数据
func (tb *mysqlTable) GetRows(tbName string, pkName string, pkValue string) {
	//执行查询语句
	var err error
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? ", tbName, pkName)
	tb.Rows, err = tb.Db.Query(query, pkValue)
	if err != nil {
		tb.LogData.LogToFile("15", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}

	//取出每列的名称slice
	tb.FieldsSlice, err = tb.Rows.Columns()
	//fmt.Println(tb.FieldsSlice)
	if err != nil {
		tb.LogData.LogToFile("16", err.Error())
		log.Println(err)
		//		log.Fatal(err)
	}

	//获取总列数
	tb.FieldsCount = len(tb.FieldsSlice)

	//创建[]interface{}接收数据库行字段
	var refs []sql.RawBytes
	refs = make([]sql.RawBytes, tb.FieldsCount)
	var args []interface{}
	args = make([]interface{}, tb.FieldsCount)
	for i, _ := range args {
		args[i] = &refs[i]
	}

	//初始化map
	tb.FieldsMap = make([]map[string]interface{}, 0)
	tb.Ct = make([][]*sql.ColumnType, 0)

	var c int = 0

	//遍历rows每行数据
	for tb.Rows.Next() {
		//读取一行的列类型
		var ct []*sql.ColumnType
		ct, err = tb.Rows.ColumnTypes()
		if err != nil {
			tb.LogData.LogToFile("17", err.Error())
			log.Println(err)
			//			log.Fatal(err)
		}
		tb.Ct = append(tb.Ct, ct)

		//创建数据库字段键值对map
		var m map[string]interface{}
		m = make(map[string]interface{}, tb.FieldsCount)

		//创建列数据
		err = tb.Rows.Scan(args...)
		if err != nil {
			tb.LogData.LogToFile("18", err.Error())
			log.Println(err)
			//			log.Fatal(err)
		}
		for i, _ := range refs {
			//			fmt.Println(i, "~", tb.FieldsSlice[i], " -> ", refs[i], " => ", ct[i].ScanType(), "~~>", ct[i].DatabaseTypeName())
			m[tb.FieldsSlice[i]] = tb.Conversion(refs[i], ct[i])
		}

		//		for i, _ := range m {
		//			fmt.Println(i, " -> ", m[i])
		//		}

		tb.FieldsMap = append(tb.FieldsMap, m)
		//		fmt.Println(tb.FieldsMap)

		c++
	}
	if c == 0 {
		tb.LogData.LogToFile("19", "Mysql GetRows() is zero")
		log.Println("Mysql GetRows() is zero")
		//		log.Fatal("Mysql GetRows() is zero")
	}
}

//转换 sql.RawBytes 到字段相应类型，并判断空值情况
func (tb *mysqlTable) Conversion(ref sql.RawBytes, ct *sql.ColumnType) interface{} {
	var v string
	v = string(reflect.ValueOf(ref).Bytes())
	var x interface{}
	x = v
	//兼容oracle中空字符串写入 not null 类型字段的问题
	if v == "" {
		switch ct.ScanType().Name() {
		case "int8":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "int16":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "int32":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "uint8":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "uint16":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "uint32":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "NullInt64":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Int()
			}
			break
		case "NullFloat64":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = reflect.Zero(reflect.TypeOf(ref)).Float()
			}
			break
		case "RawBytes":
			switch ct.DatabaseTypeName() {
			case "VARCHAR":
				if r, _ := ct.Nullable(); r {
					x = nil
				} else {
					x = " "
				}
				break
			case "TEXT":
				if r, _ := ct.Nullable(); r {
					x = nil
				} else {
					x = " "
				}
				break
			case "BLOB":
				if r, _ := ct.Nullable(); r {
					x = nil
				} else {
					x = " "
				}
				break
			case "DECIMAL":
				if r, _ := ct.Nullable(); r {
					x = nil
				} else {
					x = reflect.Zero(reflect.TypeOf(ref)).Float()
				}
			default:
				tb.LogData.LogToFile("21", "Conversion RawBytes switch ct.DatabaseTypeName() default")
				break
			}
			break
		case "NullString":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = " "
			}
			break
		case "NullBool":
			if r, _ := ct.Nullable(); r {
				x = nil
			} else {
				x = " "
			}
			break
		default:
			tb.LogData.LogToFile("22", "Conversion switch ct.ScanType().Name()")
			break
		}
		//		fmt.Println(x)
	}

	return x
}

//打开数据库
func (conf *mysqlConf) OpenDbMysql() *sql.DB {
	var err error
	var db *sql.DB
	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s?timeout=%s&collation=%s", conf.user, conf.pwd, conf.protocol, conf.host, conf.port, conf.Schema, conf.timeout, conf.collation)
	db, err = sql.Open(conf.dbType, dsn)
	if err != nil {
		conf.LogData.LogToFile("20", err.Error())
		log.Println(err)
		//		log.Fatal(err.Error())
	}
	return db
}

//记录错误日志
func (logData *DoLog) LogToFile(code string, content string) {
	path := "/tmp/m2o/" + time.Now().Format("200601") + "/"
	fileName := strconv.FormatInt(int64(time.Now().Day()), 10) + ".log"

	content = "[" + time.Now().Format("2006-01-02 15:04:05") + "]:\n" + "error:" + code + "," + logData.Action + "," + logData.TbName + "," + logData.PkName + "," + logData.PkValue + "\n" + content + "\n"

	err := os.MkdirAll(path, 0755)
	if err != nil {
		log.Println(err)
		//		log.Fatal(err)
	}
	f, err := os.OpenFile(path+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		//		log.Fatal(err)
	}
	if _, err := f.Write([]byte(content)); err != nil {
		log.Println(err)
		//		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Println(err)
		//		log.Fatal(err)
	}
}
