package kernel

type ThirdStock struct {
	ShopID     int64  `json:"shop_id" column:"SHOPID"`
	ShopName   string `json:"shop_name" column:"SHOPNAME"`
	GoodsID    string `json:"goods_id" column:"GOODSID"`
	GoodsName  string `json:"goods_name" column:"GOODSNAME"`
	BarCode    string `json:"bar_code" column:"BARCODE"`
	StockQty   string `json:"stock_qty" column:"STOCKQTY"`
	Price      string `json:"price" column:"PRICE"`
	LastUpTime int64  `json:"last_up_time" column:"LASTUPTIME"`
}

type Stock struct {
	ID         int64  `json:"id" column:"id"`
	ShopID     int64  `json:"shop_id" column:"shop_id"`
	ShopName   string `json:"shop_name" column:"shop_name"`
	GoodsID    string `json:"goods_id" column:"goods_id"`
	GoodsName  string `json:"goods_name" column:"goods_name"`
	BarCode    string `json:"bar_code" column:"bar_code"`
	Stock      string `json:"stock" column:"stock"`
	Price      string `json:"price" column:"price"`
	LastUpTime int64  `json:"last_up_time" column:"last_up_time"`
	CreatedAt  int64  `json:"created_at" column:"created_at"`
	UpdatedAt  int64  `json:"updated_at" column:"updated_at"`
}
