package main

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync-data/kernel"
	"sync-data/utils"
	"time"
)

func main() {
	var (
		startTime      int64
		runTime        int64
		lastUpdateTime int64
	)

	for {
		fmt.Println("开始同步库存关系:" + utils.DateTime())

		startTime = time.Now().Unix()
		thirdStocks := kernel.GetThirdStocks(lastUpdateTime)

		var wg sync.WaitGroup

		kernel.ConnectDB()

		size := 1000
		total := len(thirdStocks)
		chunks := int(math.Ceil(float64(len(thirdStocks) / size)))
		for i := 0; i <= chunks; i++ {
			end := (i + 1) * size
			if end > total {
				end = total
			}
			item := thirdStocks[i*size : end]
			wg.Add(1)
			go func() {
				handle(item)

				defer wg.Done()
			}()
		}
		wg.Wait()

		kernel.CloseDB()

		t := time.Now().Unix()
		lastUpdateTime = t

		runTime = t - startTime
		fmt.Println("运行耗时:" + strconv.FormatInt(runTime, 10) + "秒")

		time.Sleep(time.Second * 60)
	}
}

func handle(thirdStocks []kernel.ThirdStock) {
	var creates []kernel.Stock
	for _, thirdStock := range thirdStocks {
		stock, err := kernel.CYGetDataByGoodsID(thirdStock.ShopID, thirdStock.GoodsID)
		if err != nil {
			fmt.Println("根据商品ID查询畅移数据错误:" + err.Error())
			continue
		}
		if stock.ID != 0 {
			kernel.CYUpdate(stock.ID, kernel.Stock{
				ShopName:   thirdStock.ShopName,
				GoodsName:  thirdStock.GoodsName,
				BarCode:    thirdStock.BarCode,
				Stock:      thirdStock.StockQty,
				Price:      thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})
		} else {
			creates = append(creates, kernel.Stock{
				ShopID:    thirdStock.ShopID,
				ShopName:  thirdStock.ShopName,
				GoodsID:   thirdStock.GoodsID,
				GoodsName: thirdStock.GoodsName,
				BarCode:   thirdStock.BarCode,
				Stock:     thirdStock.StockQty,
				Price:     thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})
		}
	}
	kernel.CYBatchCreate(creates)
}
