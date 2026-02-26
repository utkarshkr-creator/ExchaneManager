package orderbook

import (
	"encoding/json"
	"testing"

	"exchangeManager/internal/types"
)

// Helper to create a fresh orderbook
func newOB() *Orderbook {
	return &Orderbook{
		Bids:         []types.Order{},
		Asks:         []types.Order{},
		BaseAsset:    "TATA",
		QuoteAsset:   "INR",
		LastTradeId:  0,
		CurrentPrice: 0,
	}
}

// --- Matching Tests ---

func TestEmptyOrderbookNoFills(t *testing.T) {
	ob := newOB()
	result := ob.AddOrder(types.Order{
		OrderID: "1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if len(result.Fills) != 0 {
		t.Errorf("expected 0 fills, got %d", len(result.Fills))
	}
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty, got %d", result.ExecutedQty)
	}
	// Order should be inserted into bids
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid in book, got %d", len(ob.Bids))
	}
}

func TestBidMatchesSingleAsk(t *testing.T) {
	ob := newOB()
	// Place a sell order first
	ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 100, Quantity: 10, Side: types.SideSell,
	})
	// Now place a matching buy order
	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if result.ExecutedQty != 10 {
		t.Errorf("expected executedQty=10, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(result.Fills))
	}
	if result.Fills[0].Quantity != 10 {
		t.Errorf("expected fill qty=10, got %d", result.Fills[0].Quantity)
	}
	if result.Fills[0].OtherUserId != "u2" {
		t.Errorf("expected otherUserId=u2, got %s", result.Fills[0].OtherUserId)
	}
	// Ask should be removed from book
	if len(ob.Asks) != 0 {
		t.Errorf("expected 0 asks after full match, got %d", len(ob.Asks))
	}
	// Fully filled buy should NOT be in book
	if len(ob.Bids) != 0 {
		t.Errorf("expected 0 bids (fully filled), got %d", len(ob.Bids))
	}
}

func TestBidPartialFill(t *testing.T) {
	ob := newOB()
	// Ask for 5
	ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 100, Quantity: 5, Side: types.SideSell,
	})
	// Bid for 10 — should fill 5, leave 5 in book
	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if result.ExecutedQty != 5 {
		t.Errorf("expected executedQty=5, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(result.Fills))
	}
	// Ask should be consumed
	if len(ob.Asks) != 0 {
		t.Errorf("expected 0 asks, got %d", len(ob.Asks))
	}
	// Remaining buy should be in bids
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid in book, got %d", len(ob.Bids))
	}
	if ob.Bids[0].OrderID != "b1" {
		t.Errorf("expected bid order b1, got %s", ob.Bids[0].OrderID)
	}
}

func TestAskMatchesSingleBid(t *testing.T) {
	ob := newOB()
	// Place a buy order first
	ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	// Now place a matching sell order
	result := ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 100, Quantity: 10, Side: types.SideSell,
	})
	if result.ExecutedQty != 10 {
		t.Errorf("expected executedQty=10, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(result.Fills))
	}
	// Bid should be removed
	if len(ob.Bids) != 0 {
		t.Errorf("expected 0 bids after full match, got %d", len(ob.Bids))
	}
}

func TestMultipleFills(t *testing.T) {
	ob := newOB()
	// Place multiple small sell orders
	ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 99, Quantity: 3, Side: types.SideSell,
	})
	ob.AddOrder(types.Order{
		OrderID: "s2", UserID: "u3", Price: 100, Quantity: 4, Side: types.SideSell,
	})
	ob.AddOrder(types.Order{
		OrderID: "s3", UserID: "u4", Price: 101, Quantity: 5, Side: types.SideSell,
	})

	// Buy 10 at price 100 — should match s1 (3) + s2 (4) = 7
	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if result.ExecutedQty != 7 {
		t.Errorf("expected executedQty=7, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 2 {
		t.Fatalf("expected 2 fills, got %d", len(result.Fills))
	}
	if result.Fills[0].Quantity != 3 {
		t.Errorf("fill[0] qty expected 3, got %d", result.Fills[0].Quantity)
	}
	if result.Fills[1].Quantity != 4 {
		t.Errorf("fill[1] qty expected 4, got %d", result.Fills[1].Quantity)
	}
	// s3 at 101 should remain
	if len(ob.Asks) != 1 {
		t.Fatalf("expected 1 ask remaining, got %d", len(ob.Asks))
	}
	if ob.Asks[0].OrderID != "s3" {
		t.Errorf("remaining ask should be s3, got %s", ob.Asks[0].OrderID)
	}
	// Partially filled buy should be in bids
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid in book, got %d", len(ob.Bids))
	}
}

func TestNoSelfTrade(t *testing.T) {
	ob := newOB()
	// Same user on both sides
	ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideSell,
	})
	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty for self-trade, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 0 {
		t.Errorf("expected 0 fills for self-trade, got %d", len(result.Fills))
	}
}

func TestPriceFilterBid(t *testing.T) {
	ob := newOB()
	// Ask at 200 — too expensive
	ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 200, Quantity: 10, Side: types.SideSell,
	})
	// Bid at 100 — should not match
	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty, got %d", result.ExecutedQty)
	}
	if len(ob.Asks) != 1 {
		t.Errorf("ask should remain, got %d asks", len(ob.Asks))
	}
	if len(ob.Bids) != 1 {
		t.Errorf("bid should be inserted, got %d bids", len(ob.Bids))
	}
}

func TestPriceFilterAsk(t *testing.T) {
	ob := newOB()
	// Bid at 50 — too low
	ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 50, Quantity: 10, Side: types.SideBuy,
	})
	// Ask at 100 — should not match
	result := ob.AddOrder(types.Order{
		OrderID: "s1", UserID: "u2", Price: 100, Quantity: 10, Side: types.SideSell,
	})
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty, got %d", result.ExecutedQty)
	}
	if len(ob.Bids) != 1 {
		t.Errorf("bid should remain, got %d bids", len(ob.Bids))
	}
	if len(ob.Asks) != 1 {
		t.Errorf("ask should be inserted, got %d asks", len(ob.Asks))
	}
}

func TestBidsInsertedDescending(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "1", UserID: "u1", Price: 100, Quantity: 1, Side: types.SideBuy})
	ob.AddOrder(types.Order{OrderID: "2", UserID: "u1", Price: 200, Quantity: 1, Side: types.SideBuy})
	ob.AddOrder(types.Order{OrderID: "3", UserID: "u1", Price: 150, Quantity: 1, Side: types.SideBuy})

	if len(ob.Bids) != 3 {
		t.Fatalf("expected 3 bids, got %d", len(ob.Bids))
	}
	// Should be sorted descending: 200, 150, 100
	if ob.Bids[0].Price != 200 || ob.Bids[1].Price != 150 || ob.Bids[2].Price != 100 {
		t.Errorf("bids not sorted descending: %d, %d, %d",
			ob.Bids[0].Price, ob.Bids[1].Price, ob.Bids[2].Price)
	}
}

func TestAsksInsertedAscending(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "1", UserID: "u1", Price: 200, Quantity: 1, Side: types.SideSell})
	ob.AddOrder(types.Order{OrderID: "2", UserID: "u1", Price: 100, Quantity: 1, Side: types.SideSell})
	ob.AddOrder(types.Order{OrderID: "3", UserID: "u1", Price: 150, Quantity: 1, Side: types.SideSell})

	if len(ob.Asks) != 3 {
		t.Fatalf("expected 3 asks, got %d", len(ob.Asks))
	}
	// Should be sorted ascending: 100, 150, 200
	if ob.Asks[0].Price != 100 || ob.Asks[1].Price != 150 || ob.Asks[2].Price != 200 {
		t.Errorf("asks not sorted ascending: %d, %d, %d",
			ob.Asks[0].Price, ob.Asks[1].Price, ob.Asks[2].Price)
	}
}

func TestTradeIdIncrementsAcrossFills(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "s1", UserID: "u2", Price: 100, Quantity: 5, Side: types.SideSell})
	ob.AddOrder(types.Order{OrderID: "s2", UserID: "u3", Price: 100, Quantity: 5, Side: types.SideSell})

	result := ob.AddOrder(types.Order{
		OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy,
	})
	if len(result.Fills) != 2 {
		t.Fatalf("expected 2 fills, got %d", len(result.Fills))
	}
	if result.Fills[0].TradeId != 0 {
		t.Errorf("first fill tradeId expected 0, got %d", result.Fills[0].TradeId)
	}
	if result.Fills[1].TradeId != 1 {
		t.Errorf("second fill tradeId expected 1, got %d", result.Fills[1].TradeId)
	}
	if ob.LastTradeId != 2 {
		t.Errorf("expected LastTradeId=2, got %d", ob.LastTradeId)
	}
}

// --- JSON API Compatibility Tests ---

func TestDepthJSONShape(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "b1", UserID: "u1", Price: 100, Quantity: 5, Side: types.SideBuy})
	ob.AddOrder(types.Order{OrderID: "s1", UserID: "u2", Price: 200, Quantity: 3, Side: types.SideSell})

	depth := ob.GetDepth()
	data, err := json.Marshal(depth)
	if err != nil {
		t.Fatalf("failed to marshal depth: %v", err)
	}

	// Parse back into generic structure to verify shape
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal depth JSON: %v", err)
	}

	// Must have "bids" and "asks" keys
	bids, ok := parsed["bids"].([]interface{})
	if !ok {
		t.Fatal("depth JSON missing 'bids' array")
	}
	asks, ok := parsed["asks"].([]interface{})
	if !ok {
		t.Fatal("depth JSON missing 'asks' array")
	}

	// Each entry must be [string, string]
	if len(bids) != 1 {
		t.Fatalf("expected 1 bid entry, got %d", len(bids))
	}
	bidEntry, ok := bids[0].([]interface{})
	if !ok || len(bidEntry) != 2 {
		t.Fatal("bid entry should be a 2-element array")
	}
	if _, ok := bidEntry[0].(string); !ok {
		t.Error("bid price should be string")
	}
	if _, ok := bidEntry[1].(string); !ok {
		t.Error("bid quantity should be string")
	}

	if len(asks) != 1 {
		t.Fatalf("expected 1 ask entry, got %d", len(asks))
	}
	askEntry, ok := asks[0].([]interface{})
	if !ok || len(askEntry) != 2 {
		t.Fatal("ask entry should be a 2-element array")
	}

	// Verify actual values
	if bidEntry[0] != "100" || bidEntry[1] != "5" {
		t.Errorf("bid entry expected [\"100\",\"5\"], got %v", bidEntry)
	}
	if askEntry[0] != "200" || askEntry[1] != "3" {
		t.Errorf("ask entry expected [\"200\",\"3\"], got %v", askEntry)
	}
}

func TestOrderPlacedJSONShape(t *testing.T) {
	// Simulate the OrderPlacedMessage that gets sent to the API
	msg := types.OrderPlacedMessage{
		OrderId:     "b1",
		ExecutedQty: 10,
		Fills: []types.Fill{
			{Price: 100, Quantity: 5, TradeId: 0, OtherUserId: "u2", MarketOrderId: "s1"},
			{Price: 100, Quantity: 5, TradeId: 1, OtherUserId: "u3", MarketOrderId: "s2"},
		},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal OrderPlacedMessage: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify top-level keys match TS: orderId, executedQty, fills
	if _, ok := parsed["orderId"]; !ok {
		t.Error("missing 'orderId' key")
	}
	if _, ok := parsed["executedQty"]; !ok {
		t.Error("missing 'executedQty' key")
	}
	fills, ok := parsed["fills"].([]interface{})
	if !ok {
		t.Fatal("missing 'fills' array")
	}
	if len(fills) != 2 {
		t.Fatalf("expected 2 fills, got %d", len(fills))
	}

	// Verify fill keys
	fill0 := fills[0].(map[string]interface{})
	for _, key := range []string{"price", "qty", "tradeId", "otherUserId", "marketOrderId"} {
		if _, ok := fill0[key]; !ok {
			t.Errorf("fill missing key: %s", key)
		}
	}
}

func TestMessageToApiJSONShape(t *testing.T) {
	// Simulate wrapping a depth response as MessageToApi
	depth := types.DepthMessage{
		Bids: [][2]string{{"100", "5"}},
		Asks: [][2]string{{"200", "3"}},
	}
	depthJSON, _ := json.Marshal(depth)

	msg := types.MessageToApi{
		Type: "DEPTH",
		Data: depthJSON,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal MessageToApi: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if parsed["type"] != "DEPTH" {
		t.Errorf("expected type=DEPTH, got %v", parsed["type"])
	}
	if _, ok := parsed["data"]; !ok {
		t.Error("missing 'data' key in MessageToApi")
	}
}

func TestCancelBid(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy})
	ob.AddOrder(types.Order{OrderID: "b2", UserID: "u1", Price: 200, Quantity: 5, Side: types.SideBuy})

	price, found := ob.CancelBid("b1")
	if !found {
		t.Fatal("expected to find bid b1")
	}
	if price != 100 {
		t.Errorf("expected cancelled price=100, got %d", price)
	}
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid remaining, got %d", len(ob.Bids))
	}
	if ob.Bids[0].OrderID != "b2" {
		t.Errorf("remaining bid should be b2, got %s", ob.Bids[0].OrderID)
	}
}

func TestCancelAsk(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "s1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideSell})

	price, found := ob.CancelAsk("s1")
	if !found {
		t.Fatal("expected to find ask s1")
	}
	if price != 100 {
		t.Errorf("expected cancelled price=100, got %d", price)
	}
	if len(ob.Asks) != 0 {
		t.Errorf("expected 0 asks after cancel, got %d", len(ob.Asks))
	}
}

func TestGetOpenOrders(t *testing.T) {
	ob := newOB()
	ob.AddOrder(types.Order{OrderID: "b1", UserID: "u1", Price: 100, Quantity: 10, Side: types.SideBuy})
	ob.AddOrder(types.Order{OrderID: "s1", UserID: "u1", Price: 200, Quantity: 5, Side: types.SideSell})
	ob.AddOrder(types.Order{OrderID: "b2", UserID: "u2", Price: 90, Quantity: 3, Side: types.SideBuy})

	orders := ob.GetOpenOrders("u1")
	if len(orders) != 2 {
		t.Fatalf("expected 2 open orders for u1, got %d", len(orders))
	}
}
