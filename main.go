package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync-data/kernel"
	"time"
)

func main() {
	var (
		startTime      int64
		runTime        int64
		lastUpdateTime int64
		wg             sync.WaitGroup
	)

	kernel.ConnectDB()

	for {
		startTime = time.Now().Unix()
		lastUpdateTime = 186561508

		thirdStocks := kernel.GetThirdStocks(lastUpdateTime)
		var (
			addNum    int64
			updateNum int64
		)
		if len(thirdStocks) > 0 {
			fmt.Println("开始同步,数量:" + strconv.FormatInt(int64(len(thirdStocks)), 10))

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
					add, update := handle(item)
					addNum += add
					updateNum += update

					fmt.Printf("单协程总处理数:%v,结果新增数:%v,更新数:%v\n", len(item), add, update)

					defer wg.Done()
				}()
			}
			wg.Wait()
		} else {
			fmt.Println("无增量数据,无需同步")
		}

		// kernel.CloseDB()

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
				LastUpTime: thirdStock.LastUpTime,
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
				LastUpTime: thirdStock.LastUpTime,
			})
		}
	}
	addNum = kernel.CYBatchCreate(creates)

	return addNum, updateNum
}
