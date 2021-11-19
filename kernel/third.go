package kernel

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync-data/utils"
)

var thirdDB *sql.DB

type Third struct {

}

func (third *Third) ConnectDB() error {
	db, err := utils.Oracle()
	if err != nil {
		fmt.Println("获取数据库实例错误:" + err.Error())
		return err
	}
	thirdDB = db
	return nil
}

func (third *Third) CloseDB() {
	thirdDB.Close()
}

func (third *Third) GetStocks(startTime int64) []ThirdStock {
	var (
		stocks []ThirdStock
		err    error
	)

	sqlStr := "select SHOPID, SHOPNAME, GOODSID, GOODSNAME, BARCODE, STOCKQTY, PRICE, LASTUPTIME from VIEW_SSKC t"
	if startTime != 0 {
		sqlStr += fmt.Sprintf(" where LASTUPTIME > %s", strconv.FormatInt(startTime, 10))
	}

	rows, err := thirdDB.Query(sqlStr)
	if err != nil {
		fmt.Println(err.Error())
		return stocks
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	// 这里表示一行所有列的值，用[]byte表示
	vals := make([][]byte, len(cols))
	// 这里表示一行填充数据
	scans := make([]interface{}, len(cols))
	// 这里scans引用vals，把数据填充到[]byte里
	for k := range vals {
		scans[k] = &vals[k]
	}

	for rows.Next() {
		// 填充数据
		err = rows.Scan(scans...)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		row := make(map[string]string)
		for k, v := range vals {
			key := cols[k]
			row[key] = string(v)
		}

		stock := &ThirdStock{}

		err = utils.Mapping(row, reflect.ValueOf(stock))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		stocks = append(stocks, *stock)
	}

	return stocks
}
