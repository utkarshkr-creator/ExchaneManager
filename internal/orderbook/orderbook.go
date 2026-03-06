package orderbook

import (
	"fmt"

	"exchangeManager/internal/types"
)

type Orderbook struct {
	Bids         []types.Order
	Asks         []types.Order
	QuoteAsset   string
	BaseAsset    string
	LastTradeId  int64
	CurrentPrice int64
	Tasks        chan func() `json:"-"`
}

func NewOrderbook(baseAsset string, quoteAsset string, bids []types.Order, asks []types.Order, lastTradeId int64, currentPrice int64) *Orderbook {
	ob := &Orderbook{
		Bids:         bids,
		Asks:         asks,
		BaseAsset:    baseAsset,
		LastTradeId:  lastTradeId,
		QuoteAsset:   quoteAsset,
		CurrentPrice: currentPrice,
		Tasks:        make(chan func(), 1000), // Buffered channel for tasks
	}
	go ob.Start()
	return ob
}

func (ob *Orderbook) Start() {
	for task := range ob.Tasks {
		task()
	}
}

func (ob *Orderbook) Ticker() string {
	return ob.BaseAsset + "_" + ob.QuoteAsset
}

func (ob *Orderbook) GetSnapshot() types.OrderbookSnapshot {
	return types.OrderbookSnapshot{
		BaseAsset:    ob.BaseAsset,
		QuoteAsset:   ob.QuoteAsset,
		Bids:         ob.Bids,
		Asks:         ob.Asks,
		LastTradeId:  ob.LastTradeId,
		CurrentPrice: ob.CurrentPrice,
	}
}

func (ob *Orderbook) AddOrder(order types.Order) types.Order {
	if order.Side == types.SideBuy {
		executedQty, fills := ob.matchBid(order)
		order.Fills = fills
		order.ExecutedQty = executedQty
		// If not fully filled, insert remaining into bids (descending price)
		if executedQty < order.Quantity {
			remaining := order
			remaining.Quantity = order.Quantity // keep original qty (filled tracked separately)
			idx := len(ob.Bids)
			for i := 0; i < len(ob.Bids); i++ {
				if ob.Bids[i].Price < order.Price {
					idx = i
					break
				}
			}
			// Insert at idx
			ob.Bids = append(ob.Bids, types.Order{})
			copy(ob.Bids[idx+1:], ob.Bids[idx:])
			ob.Bids[idx] = remaining
		}
	} else if order.Side == types.SideSell {
		executedQty, fills := ob.matchAsk(order)
		order.Fills = fills
		order.ExecutedQty = executedQty
		// If not fully filled, insert remaining into asks (ascending price)
		if executedQty < order.Quantity {
			remaining := order
			remaining.Quantity = order.Quantity
			idx := len(ob.Asks)
			for i := 0; i < len(ob.Asks); i++ {
				if ob.Asks[i].Price > order.Price {
					idx = i
					break
				}
			}
			ob.Asks = append(ob.Asks, types.Order{})
			copy(ob.Asks[idx+1:], ob.Asks[idx:])
			ob.Asks[idx] = remaining
		}
	}
	return order
}

func (ob *Orderbook) matchBid(order types.Order) (int64, []types.Fill) {
	var fills []types.Fill
	var executedQty int64

	for i := 0; i < len(ob.Asks) && executedQty < order.Quantity; i++ {
		ask := &ob.Asks[i]
		if ask.UserID != order.UserID && ask.Price <= order.Price {
			available := ask.Quantity - ask.ExecutedQty
			remaining := order.Quantity - executedQty
			filledQty := min64(remaining, available)

			executedQty += filledQty
			ask.ExecutedQty += filledQty

			fills = append(fills, types.Fill{
				Price:         ask.Price,
				Quantity:      filledQty,
				TradeId:       ob.LastTradeId,
				OtherUserId:   ask.UserID,
				MarketOrderId: ask.OrderID,
			})
			ob.LastTradeId++
		}
	}

	// Remove fully filled asks
	cleaned := ob.Asks[:0]
	for _, ask := range ob.Asks {
		if ask.ExecutedQty < ask.Quantity {
			cleaned = append(cleaned, ask)
		}
	}
	ob.Asks = cleaned

	return executedQty, fills
}

func (ob *Orderbook) matchAsk(order types.Order) (int64, []types.Fill) {
	var fills []types.Fill
	var executedQty int64

	for i := 0; i < len(ob.Bids) && executedQty < order.Quantity; i++ {
		bid := &ob.Bids[i]
		if bid.UserID != order.UserID && bid.Price >= order.Price {
			available := bid.Quantity - bid.ExecutedQty
			remaining := order.Quantity - executedQty
			filledQty := min64(remaining, available)

			executedQty += filledQty
			bid.ExecutedQty += filledQty

			fills = append(fills, types.Fill{
				Price:         bid.Price,
				Quantity:      filledQty,
				TradeId:       ob.LastTradeId,
				OtherUserId:   bid.UserID,
				MarketOrderId: bid.OrderID,
			})
			ob.LastTradeId++
		}
	}

	// Remove fully filled bids
	cleaned := ob.Bids[:0]
	for _, bid := range ob.Bids {
		if bid.ExecutedQty < bid.Quantity {
			cleaned = append(cleaned, bid)
		}
	}
	ob.Bids = cleaned

	return executedQty, fills
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (ob *Orderbook) GetDepth() types.DepthMessage {
	// Aggregate quantities at each price level (matching TS getDepth)
	bidsObj := make(map[int64]int64)
	for _, bid := range ob.Bids {
		bidsObj[bid.Price] += bid.Quantity
	}
	asksObj := make(map[int64]int64)
	for _, ask := range ob.Asks {
		asksObj[ask.Price] += ask.Quantity
	}

	bids := make([][2]string, 0, len(bidsObj))
	for price, qty := range bidsObj {
		bids = append(bids, [2]string{fmt.Sprintf("%d", price), fmt.Sprintf("%d", qty)})
	}

	asks := make([][2]string, 0, len(asksObj))
	for price, qty := range asksObj {
		asks = append(asks, [2]string{fmt.Sprintf("%d", price), fmt.Sprintf("%d", qty)})
	}

	return types.DepthMessage{
		Bids: bids,
		Asks: asks,
	}
}

func (ob *Orderbook) GetPrice() int64 {
	return ob.CurrentPrice
}

func (ob *Orderbook) GetOpenOrders(userId string) []types.Order {
	var orders []types.Order
	for _, ask := range ob.Asks {
		if ask.UserID == userId {
			orders = append(orders, ask)
		}
	}
	for _, bid := range ob.Bids {
		if bid.UserID == userId {
			orders = append(orders, bid)
		}
	}
	return orders
}

func (ob *Orderbook) CancelBid(orderId string) (int64, bool) {
	for i, bid := range ob.Bids {
		if bid.OrderID == orderId {
			price := bid.Price
			ob.Bids = append(ob.Bids[:i], ob.Bids[i+1:]...)
			return price, true
		}
	}
	return 0, false
}

func (ob *Orderbook) CancelAsk(orderId string) (int64, bool) {
	for i, ask := range ob.Asks {
		if ask.OrderID == orderId {
			price := ask.Price
			ob.Asks = append(ob.Asks[:i], ob.Asks[i+1:]...)
			return price, true
		}
	}
	return 0, false
}
