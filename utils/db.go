package utils

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"time"
)

type connect struct {
	HOST      string
	PORT      string
	DATABASE  string
	USERNAME  string
	PASSWORD  string
	CHARSET   string
	PARSETIME string
	SID       string
	Loc       string
}

func CYMysql() (baseDB *sql.DB, err error) {
	mysql := connect{
		HOST:      "sh-cdb-4lnrzka6.sql.tencentcdb.com",
		PORT:      "59794",
		DATABASE:  "HAJXC",
		USERNAME:  "hadj",
		PASSWORD:  "0Ln%1XKLqMClGiKF",
		CHARSET:   "utf8mb4",
		PARSETIME: "True",
		Loc: "Asia%2FShanghai",
	}
	driver := mysql.USERNAME + ":" + mysql.PASSWORD + "@" + "tcp(" + mysql.HOST + ":" + mysql.PORT + ")/" + mysql.DATABASE + "?charset=" + mysql.CHARSET
	if mysql.Loc != "" {
		driver += "&loc=" + mysql.Loc
	}
	if mysql.PARSETIME != "" {
		driver += "&parseTime=" + mysql.PARSETIME
	}
	db, err := sql.Open("mysql", driver)
	if err != nil {
		fmt.Printf("connect DB failed, err:%v\n", err)
		return
	}
	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Second * 10)
	return db, nil
}

func Oracle() (baseDB *sql.DB, err error) {
	oracle := connect{
		HOST:     "192.168.0.7",
		PORT:     "1521",
		USERNAME: "dbusrwx",
		PASSWORD: "dbusrwx#321",
		SID:      "HAJXC",
	}
	db, err := sql.Open("godror", `user="`+oracle.USERNAME+`" password="`+oracle.PASSWORD+`" connectString="`+oracle.HOST+`:`+oracle.PORT+`/`+oracle.SID+`"`)
	if err != nil {
		fmt.Printf("connect DB failed, err:%v\n", err)
		return
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Second * 10)
	return db, nil
}
