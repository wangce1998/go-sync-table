package kernel

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync-data/utils"
	"time"
)

var cyDB *sql.DB

type ChangYi struct {
}

func (cy *ChangYi) ConnectDB() error {
	db, err := utils.CYMysql()
	if err != nil {
		fmt.Println("获取畅移数据库实例错误:" + err.Error())

		return err
	}
	cyDB = db

	return nil
}

func (cy *ChangYi) CloseDB() {
	cyDB.Close()
}

func (cy *ChangYi) GetLastRow() (Stock, error) {
	var (
		stock Stock
		err   error
	)
	sqlStr := "SELECT last_up_time FROM sskc ORDER BY last_up_time DESC LIMIT 1"
	res, err := cy.GetData(sqlStr)
	if err != nil {
		return stock, err
	}
	if len(res) > 0 {
		stock = res[0]
	}

	return stock, err
}

func (cy *ChangYi) GetData(sqlStr string) ([]Stock, error) {
	rows, err := cyDB.Query(sqlStr)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	defer rows.Close()

	return cy.Handle(rows), nil
}

func (cy *ChangYi) Update(id int64, stock Stock) error {
	stmt, err := cyDB.Prepare("update sskc set shop_name = ?, goods_name = ?, bar_code = ?, stock = ?, price = ?, last_up_time = ?, updated_at = ? where id = ?")
	if err != nil {
		fmt.Println("更新sql创建错误:" + err.Error())
		return err
	}
	stock.UpdatedAt = time.Now().Unix()
	_, err = stmt.Exec(
		stock.ShopName,
		stock.GoodsName,
		stock.BarCode,
		stock.Stock,
		stock.Price,
		stock.LastUpTime,
		stock.UpdatedAt,
		id,
	)
	defer stmt.Close()
	if err != nil {
		fmt.Println("更新错误:" + err.Error())
		return err
	}

	return nil
}
func (cy *ChangYi) BatchAdd(stocks []Stock) int64 {
	if len(stocks) == 0 {
		return 0
	}
	sqlStr := "insert into sskc (shop_id, shop_name, goods_id, goods_name, bar_code, stock, price, last_up_time, created_at, updated_at) values "

	t := time.Now().Unix()
	for _, stock := range stocks {
		stock.CreatedAt = t
		stock.UpdatedAt = t

		sqlStr += fmt.Sprintf(
			"(%s, '%s', '%s', '%s', '%s', %s, '%s', %s, %s, %s),",
			strconv.FormatInt(stock.ShopID, 10),
			strings.ReplaceAll(stock.ShopName, "'", "\\'"),
			stock.GoodsID,
			strings.ReplaceAll(stock.GoodsName, "'", "\\'"),
			strings.ReplaceAll(stock.BarCode, "'", "\\'"),
			stock.Stock,
			stock.Price,
			strconv.FormatInt(stock.LastUpTime, 10),
			strconv.FormatInt(stock.CreatedAt, 10),
			strconv.FormatInt(stock.UpdatedAt, 10),
		)
	}
	sqlStr = sqlStr[:len(sqlStr)-1]

	stmt, err := cyDB.Exec(sqlStr)
	if err != nil {
		fmt.Println("批量新增错误:" + err.Error())

		var d1 = []byte(sqlStr)
		_ = ioutil.WriteFile("./err-"+strconv.FormatInt(time.Now().Unix(), 10)+".sql", d1, 0666)

		return 0
	}
	num, err := stmt.RowsAffected()
	if err != nil {
		fmt.Println("批量新增获取插入数错误:" + err.Error())
		return 0
	}

	return num
}

func (cy *ChangYi) GetDataByGoodsID(shopID int64, goodsID string) (Stock, error) {
	var (
		stock Stock
		err   error
	)
	res, err := cy.GetData("select id from sskc where shop_id = " + strconv.FormatInt(shopID, 10) + " and goods_id = " + goodsID)
	if err != nil {
		return stock, err
	}
	if len(res) > 0 {
		stock = res[0]
	}

	return stock, err
}

func (cy *ChangYi) Handle(rows *sql.Rows) []Stock {
	var err error
	var stocks []Stock
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

		stock := &Stock{}
		err = utils.Mapping(row, reflect.ValueOf(stock))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		stocks = append(stocks, *stock)
	}

	return stocks
}
