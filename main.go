package main

import (
	"encoding/json"
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

		kernel.ConnectDB()
		thirdStocks := kernel.GetThirdStocks(lastUpdateTime)

		var (
			addNum    int64
			updateNum int64
			wg        sync.WaitGroup
		)

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
				an, un := handle(item)
				addNum += an
				updateNum += un

				defer wg.Done()
			}()
		}
		wg.Wait()

		kernel.CloseDB()

		t := time.Now().Unix()
		lastUpdateTime = t
		runTime = t - startTime

		fmt.Println("新增数:" + strconv.FormatInt(addNum, 10) + " 更新数:" + strconv.FormatInt(updateNum, 10))
		fmt.Println("运行耗时:" + strconv.FormatInt(runTime, 10) + "秒")

		time.Sleep(time.Second * 60)
	}
}

func handle(thirdStocks []kernel.ThirdStock) (int64, int64) {
	var (
		addNum    int64
		updateNum int64
		creates   []kernel.Stock
	)

	for _, thirdStock := range thirdStocks {
		stock, err := kernel.CYGetDataByGoodsID(thirdStock.ShopID, thirdStock.GoodsID)
		if err != nil {
			fmt.Println("根据商品ID查询畅移数据错误:" + err.Error())
			continue
		}
		if stock.ID != 0 {
			err = kernel.CYUpdate(stock.ID, kernel.Stock{
				ShopName:   thirdStock.ShopName,
				GoodsName:  thirdStock.GoodsName,
				BarCode:    thirdStock.BarCode,
				Stock:      thirdStock.StockQty,
				Price:      thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})
			if err != nil {
				b, _ := json.Marshal(thirdStock)

				fmt.Println("更新错误:" + err.Error() + ",原始数据:" + string(b))
				continue
			}
			updateNum++
		} else {
			creates = append(creates, kernel.Stock{
				ShopID:     thirdStock.ShopID,
				ShopName:   thirdStock.ShopName,
				GoodsID:    thirdStock.GoodsID,
				GoodsName:  thirdStock.GoodsName,
				BarCode:    thirdStock.BarCode,
				Stock:      thirdStock.StockQty,
				Price:      thirdStock.Price,
				LastUpTime: utils.FormatTime(thirdStock.LastUpTime),
			})
		}
	}
	addNum = kernel.CYBatchCreate(creates)

	return addNum, updateNum
}
