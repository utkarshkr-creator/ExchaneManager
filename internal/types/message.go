package types

import "encoding/json"

type MessageFromApi struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

const (
	CREATE_ORDER    = "CREATE_ORDER"
	GET_BALANCE     = "GET_BALANCE"
	CANCEL_ORDER    = "CANCEL_ORDER"
	GET_PRICE       = "GET_PRICE"
	ON_RAMP         = "ON_RAMP"
	GET_DEPTH       = "GET_DEPTH"
	GET_OPEN_ORDERS = "GET_OPEN_ORDERS"
)

type MessageFromApiData interface {
	CREATE_ORDER_DATA() *CreateOrderData
	GET_BALANCE_DATA() *GetBalanceData
	CANCEL_ORDER_DATA() *CancelOrderData
	GET_PRICE_DATA() *GetPriceData
	ON_RAMP_DATA() *OnRampData
	GET_DEPTH_DATA() *GetDepthData
	GET_OPEN_ORDERS_DATA() *GetOpenOrdersData
}

type CreateOrderData struct {
	Market   string `json:"market"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
	Side     string `json:"side"`
	UserId   string `json:"userId"`
}

func (c *CreateOrderData) CREATE_ORDER_DATA() *CreateOrderData {
	return c
}

type GetBalanceData struct {
	UserId     string `json:"userId"`
	QuoteAsset string `json:"quoteAsset"`
}

func (g *GetBalanceData) GET_BALANCE_DATA() *GetBalanceData {
	return g
}

type CancelOrderData struct {
	OrderId string `json:"orderId"`
	Market  string `json:"market"`
}

func (c *CancelOrderData) CANCEL_ORDER_DATA() *CancelOrderData {
	return c
}

type GetPriceData struct {
	QuoteAsset string `json:"quoteAsset"`
}

func (g *GetPriceData) GET_PRICE_DATA() *GetPriceData {
	return g
}

type OnRampData struct {
	Amount string `json:"amount"`
	UserId string `json:"userId"`
	TxnId  string `json:"txnId"`
}

func (o *OnRampData) ON_RAMP_DATA() *OnRampData {
	return o
}

type GetDepthData struct {
	Market string `json:"market"`
}

func (g *GetDepthData) GET_DEPTH_DATA() *GetDepthData {
	return g
}

type GetOpenOrdersData struct {
	UserId string `json:"userId"`
	Market string `json:"market"`
}

func (g *GetOpenOrdersData) GET_OPEN_ORDERS_DATA() *GetOpenOrdersData {
	return g
}

// TO API
type MessageToApi struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"payload"`
}

type MessageToApiData interface {
	ORDER_PLACED() *OrderPlacedMessage
	ORDER_CANCELLED() *OrderCancelledMessage
	OPEN_ORDERS() *OpenOrdersMessage
	GET_PRICE() *GetPriceMessage
	DEPTH() *DepthMessage
	GET_BALANCE() *GetBalanceMessage
}

type OrderPlacedMessage struct {
	OrderId     string  `json:"orderId"`
	ExecutedQty float64 `json:"executedQty"`
	Fills       []Fill  `json:"fills"`
}

func (o *OrderPlacedMessage) ORDER_PLACED() *OrderPlacedMessage {
	return o
}

type DepthMessage struct {
	Bids [][2]string `json:"bids"`
	Asks [][2]string `json:"asks"`
}

func (d *DepthMessage) DEPTH() *DepthMessage {
	return d
}

type GetBalanceMessage struct {
	UserBalance string `json:"userBalance"`
}

func (g *GetBalanceMessage) GET_BALANCE() *GetBalanceMessage {
	return g
}

type OrderCancelledMessage struct {
	OrderId      string `json:"orderId"`
	ExecutedQty  int64  `json:"executedQty"`
	RemainingQty int64  `json:"remainingQty"`
}

func (o *OrderCancelledMessage) ORDER_CANCELLED() *OrderCancelledMessage {
	return o
}

type OpenOrdersMessage struct {
	Orders []Order `json:"orders"`
}

func (o *OpenOrdersMessage) OPEN_ORDERS() *OpenOrdersMessage {
	return o
}

type GetPriceMessage struct {
	Price string `json:"price"`
}

func (g *GetPriceMessage) GET_PRICE() *GetPriceMessage {
	return g
}

// DB Messages — pushed to Redis list "db_processor"
const (
	TRADE_ADDED  = "TRADE_ADDED"
	ORDER_UPDATE = "ORDER_UPDATE"
)

type DbMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type TradeAddedData struct {
	ID            string `json:"id"`
	IsBuyerMaker  bool   `json:"isBuyerMaker"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	QuoteQuantity string `json:"quoteQuantity"`
	Timestamp     int64  `json:"timestamp"`
	Market        string `json:"market"`
}

type OrderUpdateData struct {
	OrderId     string `json:"orderId"`
	ExecutedQty int64  `json:"executedQty"`
	Market      string `json:"market,omitempty"`
	Price       string `json:"price,omitempty"`
	Quantity    string `json:"quantity,omitempty"`
	Side        string `json:"side,omitempty"`
}

// WS Messages — published via Redis pub/sub to WebSocket server
type WsMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}
