package types

type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

type Order struct {
	Price       int64
	Quantity    int64
	OrderID     string
	Side        Side
	UserID      string
	Fills       []Fill
	ExecutedQty int64
}

type Fill struct {
	Price         int64  `json:"price"`
	Quantity      int64  `json:"qty"`
	TradeId       int64  `json:"tradeId"`
	OtherUserId   string `json:"otherUserId"`
	MarketOrderId string `json:"marketOrderId"`
}

type UserBalance map[string]*Balance

type Balance struct {
	Available int64
	Locked    int64
}

type OrderbookSnapshot struct {
	BaseAsset    string
	QuoteAsset   string
	Bids         []Order
	Asks         []Order
	LastTradeId  int64
	CurrentPrice int64
}

const BaseCurrency = "INR"
