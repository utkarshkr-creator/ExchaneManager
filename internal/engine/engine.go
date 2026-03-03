package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"exchangeManager/internal/orderbook"
	redismgr "exchangeManager/internal/redis"
	"exchangeManager/internal/types"
)

const BaseCurrency = types.BaseCurrency

type Engine struct {
	Orderbooks []*orderbook.Orderbook       `json:"orderbooks"`
	PriceList  map[string]int64             `json:"priceList"`
	Balances   map[string]types.UserBalance `json:"balances"`
}

func saveSnapshot(engine *Engine) {
	data, err := json.MarshalIndent(engine, "", "  ")
	if err != nil {
		fmt.Println("Failed to marshal snapshot:", err)
		return
	}
	if err := os.WriteFile("./snapshot.json", data, 0644); err != nil {
		fmt.Println("Failed to write snapshot:", err)
	}
}

func NewEngine() (*Engine, error) {
	var engine *Engine

	if os.Getenv("WITH_SNAPSHOT") == "true" {
		data, err := os.ReadFile("./snapshot.json")
		if err != nil {
			fmt.Println("No snapshot found:", err)
		} else {
			if err := json.Unmarshal(data, &engine); err != nil {
				fmt.Println("Failed to parse snapshot:", err)
			}
		}
	}

	if engine == nil {
		engine = &Engine{
			Orderbooks: []*orderbook.Orderbook{orderbook.NewOrderbook("TATA", "INR", nil, nil, 0, 138)},
			PriceList:  map[string]int64{"TATA": 138},
			Balances:   make(map[string]types.UserBalance),
		}
		engine.setBaseBalances()
	}

	// Periodic snapshot saving in the background.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			saveSnapshot(engine)
		}
	}()

	return engine, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (e *Engine) addOrderbook(ob *orderbook.Orderbook) {
	e.Orderbooks = append(e.Orderbooks, ob)
}

func (e *Engine) getOrderbook(market string) *orderbook.Orderbook {
	for _, ob := range e.Orderbooks {
		if ob.Ticker() == market {
			return ob
		}
	}
	return nil
}

func (e *Engine) getBalance(userId string, asset string) string {
	ub, ok := e.Balances[userId]
	if !ok {
		return "0"
	}
	b, ok := ub[asset]
	if !ok || b == nil {
		return "0"
	}
	return strconv.FormatInt(b.Available, 10)
}

func (e *Engine) getPrice(asset string) int64 {
	return e.PriceList[asset]
}

func generateOrderId() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 13)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Process — main message router (matches TS Engine.process)
// ---------------------------------------------------------------------------

func (e *Engine) Process(message types.MessageFromApi, clientId string) {
	ctx := context.Background()
	rm := redismgr.GetInstance()

	switch message.Type {
	case types.GET_BALANCE:
		var data types.GetBalanceData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing GET_BALANCE:", err)
			return
		}
		balance := e.getBalance(data.UserId, data.QuoteAsset)
		payload, _ := json.Marshal(types.GetBalanceMessage{UserBalance: balance})
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "GET_BALANCE", Data: payload})

	case types.GET_PRICE:
		var data types.GetPriceData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing GET_PRICE:", err)
			return
		}
		price := e.getPrice(data.QuoteAsset)
		payload, _ := json.Marshal(types.GetPriceMessage{Price: strconv.FormatInt(price, 10)})
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "GET_PRICE", Data: payload})

	case types.CREATE_ORDER:
		var data types.CreateOrderData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing CREATE_ORDER:", err)
			return
		}
		result, err := e.createOrder(data.Market, data.Price, data.Quantity, data.Side, data.UserId)
		if err != nil {
			fmt.Println("Error in order placing:", err)
			payload, _ := json.Marshal(types.OrderCancelledMessage{OrderId: "", ExecutedQty: 0, RemainingQty: 0})
			rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "ORDER_CANCELLED", Data: payload})
			return
		}
		payload, _ := json.Marshal(types.OrderPlacedMessage{
			OrderId:     result.OrderId,
			ExecutedQty: float64(result.ExecutedQty),
			Fills:       result.Fills,
		})
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "ORDER_PLACED", Data: payload})

	case types.CANCEL_ORDER:
		var data types.CancelOrderData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing CANCEL_ORDER:", err)
			return
		}
		e.cancelOrder(ctx, data.OrderId, data.Market)
		payload, _ := json.Marshal(types.OrderCancelledMessage{OrderId: data.OrderId, ExecutedQty: 0, RemainingQty: 0})
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "ORDER_CANCELLED", Data: payload})

	case types.GET_OPEN_ORDERS:
		var data types.GetOpenOrdersData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing GET_OPEN_ORDERS:", err)
			return
		}
		ob := e.getOrderbook(data.Market)
		if ob == nil {
			fmt.Println("No orderbook found for", data.Market)
			return
		}
		orders := ob.GetOpenOrders(data.UserId)
		payload, _ := json.Marshal(types.OpenOrdersMessage{Orders: orders})
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "OPEN_ORDERS", Data: payload})

	case types.ON_RAMP:
		var data types.OnRampData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing ON_RAMP:", err)
			return
		}
		amount, _ := strconv.ParseInt(data.Amount, 10, 64)
		e.onRamp(data.UserId, amount)

	case types.GET_DEPTH:
		var data types.GetDepthData
		if err := json.Unmarshal(message.Data, &data); err != nil {
			fmt.Println("Error parsing GET_DEPTH:", err)
			return
		}
		ob := e.getOrderbook(data.Market)
		if ob == nil {
			payload, _ := json.Marshal(types.DepthMessage{Bids: [][2]string{}, Asks: [][2]string{}})
			rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "DEPTH", Data: payload})
			return
		}
		depth := ob.GetDepth()
		payload, _ := json.Marshal(depth)
		rm.SendToApi(ctx, clientId, types.MessageToApi{Type: "DEPTH", Data: payload})
	}
}

// ---------------------------------------------------------------------------
// Order creation result
// ---------------------------------------------------------------------------

type createOrderResult struct {
	ExecutedQty int64
	Fills       []types.Fill
	OrderId     string
}

// ---------------------------------------------------------------------------
// createOrder — matches TS Engine.createOrder
// ---------------------------------------------------------------------------

func (e *Engine) createOrder(market, priceStr, quantityStr, side, userId string) (*createOrderResult, error) {
	ob := e.getOrderbook(market)
	if ob == nil {
		return nil, types.ErrInvalidMarket
	}

	baseAsset := strings.Split(market, "_")[0]
	quoteAsset := strings.Split(market, "_")[1]

	price, _ := strconv.ParseInt(priceStr, 10, 64)
	quantity, _ := strconv.ParseInt(quantityStr, 10, 64)

	if err := e.checkAndLockFunds(baseAsset, quoteAsset, side, userId, price, quantity); err != nil {
		return nil, err
	}

	order := types.Order{
		Price:    price,
		Quantity: quantity,
		OrderID:  generateOrderId(),
		Side:     types.Side(side),
		UserID:   userId,
	}

	result := ob.AddOrder(order)
	fills := result.Fills
	executedQty := result.ExecutedQty

	ctx := context.Background()

	e.updateBalance(userId, baseAsset, quoteAsset, side, fills)
	e.createDbTrades(ctx, fills, market, userId)
	e.updateDbOrders(ctx, result, executedQty, fills, market)
	e.publishWsDepthUpdates(ctx, fills, priceStr, side, market)
	if len(fills) > 0 {
		lastPrice := strconv.FormatInt(fills[len(fills)-1].Price, 10)
		e.publishWsPriceUpdates(ctx, market, lastPrice)
	}
	e.publishWsTrades(ctx, fills, userId, market)

	return &createOrderResult{
		ExecutedQty: executedQty,
		Fills:       fills,
		OrderId:     result.OrderID,
	}, nil
}

// ---------------------------------------------------------------------------
// cancelOrder — matches TS Engine (inside CANCEL_ORDER case)
// ---------------------------------------------------------------------------

func (e *Engine) cancelOrder(ctx context.Context, orderId string, market string) {
	ob := e.getOrderbook(market)
	if ob == nil {
		fmt.Println("No orderbook found for", market)
		return
	}

	parts := strings.Split(market, "_")
	if len(parts) != 2 {
		fmt.Println("Invalid market format:", market)
		return
	}
	baseAsset := parts[0]
	quoteAsset := parts[1]

	// Try to find the order in asks or bids.
	var order *types.Order
	for i := range ob.Asks {
		if ob.Asks[i].OrderID == orderId {
			order = &ob.Asks[i]
			break
		}
	}
	if order == nil {
		for i := range ob.Bids {
			if ob.Bids[i].OrderID == orderId {
				order = &ob.Bids[i]
				break
			}
		}
	}
	if order == nil {
		fmt.Println("No order found:", orderId)
		return
	}

	if order.Side == types.SideBuy {
		price, found := ob.CancelBid(orderId)
		leftQuantity := (order.Quantity - order.ExecutedQty) * order.Price
		e.ensureBalance(order.UserID, quoteAsset)
		e.Balances[order.UserID][quoteAsset].Available += leftQuantity
		e.Balances[order.UserID][quoteAsset].Locked -= leftQuantity
		if found {
			e.sendUpdatedDepthAt(ctx, strconv.FormatInt(price, 10), market)
		}
	} else {
		price, found := ob.CancelAsk(orderId)
		leftQuantity := order.Quantity - order.ExecutedQty
		e.ensureBalance(order.UserID, baseAsset)
		e.Balances[order.UserID][baseAsset].Available += leftQuantity
		e.Balances[order.UserID][baseAsset].Locked -= leftQuantity
		if found {
			e.sendUpdatedDepthAt(ctx, strconv.FormatInt(price, 10), market)
		}
	}
}

// ---------------------------------------------------------------------------
// Balance management
// ---------------------------------------------------------------------------

func (e *Engine) ensureBalance(userId string, asset string) {
	if _, ok := e.Balances[userId]; !ok {
		e.Balances[userId] = make(types.UserBalance)
	}
	if e.Balances[userId][asset] == nil {
		e.Balances[userId][asset] = &types.Balance{}
	}
}

func (e *Engine) onRamp(userId string, amount int64) {
	e.ensureBalance(userId, BaseCurrency)
	e.Balances[userId][BaseCurrency].Available += amount
}

func (e *Engine) checkAndLockFunds(baseAsset, quoteAsset, side string, userId string, price, quantity int64) error {
	totalPrice := quantity * price
	if side == "buy" {
		e.ensureBalance(userId, quoteAsset)
		if e.Balances[userId][quoteAsset].Available < totalPrice {
			return types.ErrInsufficientFunds
		}
		e.Balances[userId][quoteAsset].Available -= totalPrice
		e.Balances[userId][quoteAsset].Locked += totalPrice
	} else {
		e.ensureBalance(userId, baseAsset)
		if e.Balances[userId][baseAsset].Available < quantity {
			return types.ErrInsufficientFunds
		}
		e.Balances[userId][baseAsset].Available -= quantity
		e.Balances[userId][baseAsset].Locked += quantity
	}
	return nil
}

func (e *Engine) updateBalance(userId, baseAsset, quoteAsset, side string, fills []types.Fill) {
	if side == "buy" {
		for _, fill := range fills {
			totalValue := fill.Quantity * fill.Price
			// Seller gets quote currency
			e.ensureBalance(fill.OtherUserId, quoteAsset)
			e.Balances[fill.OtherUserId][quoteAsset].Available += totalValue
			// Buyer's locked quote currency decreases
			e.ensureBalance(userId, quoteAsset)
			e.Balances[userId][quoteAsset].Locked -= totalValue
			// Seller's locked base asset decreases
			e.ensureBalance(fill.OtherUserId, baseAsset)
			e.Balances[fill.OtherUserId][baseAsset].Locked -= fill.Quantity
			// Buyer gets base asset
			e.ensureBalance(userId, baseAsset)
			e.Balances[userId][baseAsset].Available += fill.Quantity
		}
	} else {
		for _, fill := range fills {
			totalValue := fill.Quantity * fill.Price
			// Buyer's locked quote decreases
			e.ensureBalance(fill.OtherUserId, quoteAsset)
			e.Balances[fill.OtherUserId][quoteAsset].Locked -= totalValue
			// Seller gets quote
			e.ensureBalance(userId, quoteAsset)
			e.Balances[userId][quoteAsset].Available += totalValue
			// Buyer gets base
			e.ensureBalance(fill.OtherUserId, baseAsset)
			e.Balances[fill.OtherUserId][baseAsset].Available += fill.Quantity
			// Seller's locked base decreases
			e.ensureBalance(userId, baseAsset)
			e.Balances[userId][baseAsset].Locked -= fill.Quantity
		}
	}
}

func (e *Engine) setBaseBalances() {
	users := []string{"1", "2", "3", "6", "7"}
	for _, uid := range users {
		e.Balances[uid] = types.UserBalance{
			BaseCurrency: {Available: 1000000000000000, Locked: 0},
			"TATA":       {Available: 100000000, Locked: 0},
		}
	}
	// User "1" gets extra base currency
	e.Balances["1"][BaseCurrency].Available = 1000000000000000
}

// ---------------------------------------------------------------------------
// Redis DB pushes — matches TS createDbTrades, updateDbOrders
// ---------------------------------------------------------------------------

func (e *Engine) createDbTrades(ctx context.Context, fills []types.Fill, market, userId string) {
	rm := redismgr.GetInstance()
	for _, fill := range fills {
		data, _ := json.Marshal(types.TradeAddedData{
			Market:        market,
			ID:            strconv.FormatInt(fill.TradeId, 10),
			IsBuyerMaker:  fill.OtherUserId == userId,
			Price:         strconv.FormatInt(fill.Price, 10),
			Quantity:      strconv.FormatInt(fill.Quantity, 10),
			QuoteQuantity: strconv.FormatInt(fill.Quantity*fill.Price, 10),
			Timestamp:     time.Now().UnixMilli(),
		})
		rm.PushMessage(ctx, types.DbMessage{Type: types.TRADE_ADDED, Data: data})
	}
}

func (e *Engine) updateDbOrders(ctx context.Context, order types.Order, executedQty int64, fills []types.Fill, market string) {
	rm := redismgr.GetInstance()

	// Push the taker order update.
	data, _ := json.Marshal(types.OrderUpdateData{
		OrderId:     order.OrderID,
		ExecutedQty: executedQty,
		Market:      market,
		Price:       strconv.FormatInt(order.Price, 10),
		Quantity:    strconv.FormatInt(order.Quantity, 10),
		Side:        string(order.Side),
	})
	rm.PushMessage(ctx, types.DbMessage{Type: types.ORDER_UPDATE, Data: data})

	// Push each maker (filled) order update.
	for _, fill := range fills {
		fdata, _ := json.Marshal(types.OrderUpdateData{
			OrderId:     fill.MarketOrderId,
			ExecutedQty: fill.Quantity,
		})
		rm.PushMessage(ctx, types.DbMessage{Type: types.ORDER_UPDATE, Data: fdata})
	}
}

// ---------------------------------------------------------------------------
// WebSocket publishing — matches TS publishWsTrades, publishWsDepthUpdates,
// publishWsPriceUpdates, sendUpdatedDepthAt
// ---------------------------------------------------------------------------

func (e *Engine) publishWsTrades(ctx context.Context, fills []types.Fill, userId, market string) {
	rm := redismgr.GetInstance()
	channel := fmt.Sprintf("trade@%s", market)
	for _, fill := range fills {
		data, _ := json.Marshal(map[string]interface{}{
			"e": "trade",
			"t": fill.TradeId,
			"m": fill.OtherUserId == userId,
			"p": strconv.FormatInt(fill.Price, 10),
			"q": strconv.FormatInt(fill.Quantity, 10),
			"s": market,
		})
		rm.PublishMessage(ctx, channel, types.WsMessage{Stream: channel, Data: data})
	}
}

func (e *Engine) publishWsDepthUpdates(ctx context.Context, fills []types.Fill, priceStr, side, market string) {
	ob := e.getOrderbook(market)
	if ob == nil {
		return
	}
	depth := ob.GetDepth()
	channel := fmt.Sprintf("depth@%s", market)
	rm := redismgr.GetInstance()

	if side == "buy" {
		// Updated asks: for each fill price, find current depth or "0"
		updatedAsks := make([][2]string, 0, len(fills))
		for _, f := range fills {
			p := strconv.FormatInt(f.Price, 10)
			qty := "0"
			for _, a := range depth.Asks {
				if a[0] == p {
					qty = a[1]
					break
				}
			}
			updatedAsks = append(updatedAsks, [2]string{p, qty})
		}
		// Updated bid at the order price
		var updatedBids [][2]string
		for _, b := range depth.Bids {
			if b[0] == priceStr {
				updatedBids = append(updatedBids, b)
				break
			}
		}
		data, _ := json.Marshal(map[string]interface{}{
			"a": updatedAsks,
			"b": updatedBids,
			"e": "depth",
		})
		rm.PublishMessage(ctx, channel, types.WsMessage{Stream: channel, Data: data})
	} else {
		// Updated bids: for each fill price, find current depth or "0"
		updatedBids := make([][2]string, 0, len(fills))
		for _, f := range fills {
			p := strconv.FormatInt(f.Price, 10)
			qty := "0"
			for _, b := range depth.Bids {
				if b[0] == p {
					qty = b[1]
					break
				}
			}
			updatedBids = append(updatedBids, [2]string{p, qty})
		}
		// Updated ask at the order price
		var updatedAsks [][2]string
		for _, a := range depth.Asks {
			if a[0] == priceStr {
				updatedAsks = append(updatedAsks, a)
				break
			}
		}
		data, _ := json.Marshal(map[string]interface{}{
			"a": updatedAsks,
			"b": updatedBids,
			"e": "depth",
		})
		rm.PublishMessage(ctx, channel, types.WsMessage{Stream: channel, Data: data})
	}
}

func (e *Engine) sendUpdatedDepthAt(ctx context.Context, priceStr, market string) {
	ob := e.getOrderbook(market)
	if ob == nil {
		return
	}
	depth := ob.GetDepth()
	channel := fmt.Sprintf("depth@%s", market)
	rm := redismgr.GetInstance()

	var updatedBids [][2]string
	for _, b := range depth.Bids {
		if b[0] == priceStr {
			updatedBids = append(updatedBids, b)
		}
	}
	var updatedAsks [][2]string
	for _, a := range depth.Asks {
		if a[0] == priceStr {
			updatedAsks = append(updatedAsks, a)
		}
	}

	if len(updatedAsks) == 0 {
		updatedAsks = [][2]string{{priceStr, "0"}}
	}
	if len(updatedBids) == 0 {
		updatedBids = [][2]string{{priceStr, "0"}}
	}

	data, _ := json.Marshal(map[string]interface{}{
		"a": updatedAsks,
		"b": updatedBids,
		"e": "depth",
	})
	rm.PublishMessage(ctx, channel, types.WsMessage{Stream: channel, Data: data})
}

func (e *Engine) publishWsPriceUpdates(ctx context.Context, market, price string) {
	channel := fmt.Sprintf("ticker@%s", market)
	rm := redismgr.GetInstance()
	data, _ := json.Marshal(map[string]interface{}{
		"e": "ticker",
		"c": price,
		"s": market,
	})
	rm.PublishMessage(ctx, channel, types.WsMessage{Stream: channel, Data: data})
}
