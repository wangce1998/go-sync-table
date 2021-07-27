package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync-data/utils"
	"time"
)

var thirdDB *sql.DB
var cyDB *sql.DB

func main() {
	var (
		err error
	)
	thirdDB, err = utils.Oracle()
	if err != nil {
		fmt.Println("获取数据库实例错误", err.Error())
		return
	}
	cyDB, err = utils.CYMysql()
	if err != nil {
		fmt.Println("获取畅移数据库实例错误", err.Error())
		return
	}

	for {
		fmt.Println("开始同步库存关系 datetime", utils.DateTime())
		handle()
		time.Sleep(time.Second * 5)
	}
}

func handle() {
	thirdStocks := getThirdStocks()

	for _, thirdStock := range thirdStocks {
		stock, err := CYGetDataByGoodsID(thirdStock.ShopID, thirdStock.GoodsID)
		if err != nil {
			fmt.Println("根据商品ID查询畅移数据错误", err.Error())
			continue
		}
		if stock.ID != 0 {
			CYUpdate(stock.ID, Stock{
				ShopName:   thirdStock.ShopName,
				GoodsName:  thirdStock.GoodsName,
				BarCode:    thirdStock.BarCode,
				Stock:      thirdStock.StockQty,
				Price:      thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})
			fmt.Printf("更新数据成功 goods_id:%v stock:%v \n", thirdStock.GoodsID, thirdStock.StockQty)
		} else {
			id := CYCreate(Stock{
				ShopID:     thirdStock.ShopID,
				ShopName:   thirdStock.ShopName,
				GoodsID:    thirdStock.GoodsID,
				GoodsName:  thirdStock.GoodsName,
				BarCode:    thirdStock.BarCode,
				Stock:      thirdStock.StockQty,
				Price:      thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})

			fmt.Printf("新增数据成功 id:%v \n", id)
		}
	}
}

func getThirdStocks() []ThirdStock {
	var (
		stocks []ThirdStock
		err    error
	)

	rows, err := thirdDB.Query("select SHOPID, SHOPNAME, GOODSID, GOODSNAME, BARCODE, STOCKQTY, PRICE, LASTUPTIME from VIEW_SSKC t")
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

		stock.LastUpTime = utils.Format(stock.LastUpTime)
		stocks = append(stocks, *stock)
	}

	return stocks
}

func CYUpdate(id int64, stock Stock) {
	stmt, err := cyDB.Prepare("update sskc set shop_name = ?, goods_name = ?, bar_code = ?, stock = ?, price = ?, last_up_time = ?, updated_at = ? where id = ?")
	if err != nil {
		fmt.Println("更新sql创建错误", err.Error())
		return
	}
	stock.LastUpTime = 0
	stock.UpdatedAt = time.Now().Unix()
	_, err = stmt.Exec(stock.ShopName, stock.GoodsName, stock.BarCode, stock.Stock, stock.Price, stock.LastUpTime, stock.UpdatedAt, id)
	if err != nil {
		fmt.Println("更新错误", err.Error())
		return
	}
}

func CYCreate(stock Stock) int64 {
	stmt, err := cyDB.Prepare("insert into sskc (shop_id, shop_name, goods_id, goods_name, bar_code, stock, price, last_up_time, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		fmt.Println("新增sql创建错误", err.Error())
		return 0
	}
	stock.LastUpTime = 0
	tt := time.Now().Unix()
	stock.CreatedAt = tt
	stock.UpdatedAt = tt

	fmt.Println(stock)

	exec, err := stmt.Exec(stock.ShopID, stock.ShopName, stock.GoodsID, stock.GoodsName, stock.BarCode, stock.Stock, stock.Price, stock.LastUpTime, stock.CreatedAt, stock.UpdatedAt)
	if err != nil {
		fmt.Println("新增错误", err.Error())
		return 0
	}
	id, _ := exec.LastInsertId()

	return id
}

func CYGetDataByGoodsID(shopID int64, goodsID string) (Stock, error) {
	var (
		stock Stock
		err   error
	)
	res, err := getCYData("select id from sskc where shop_id = " + strconv.FormatInt(shopID, 10) + " and goods_id = " + goodsID)
	if err != nil {
		return stock, err
	}
	if len(res) > 0 {
		stock = res[0]
	}

	return stock, err
}

func getCYData(sqlStr string) ([]Stock, error) {
	rows, err := cyDB.Query(sqlStr)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	defer rows.Close()

	return CYHandle(rows), nil
}

func CYHandle(rows *sql.Rows) []Stock {
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