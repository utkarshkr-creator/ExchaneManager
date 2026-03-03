package engine

import (
	"context"
	"testing"

	"exchangeManager/internal/orderbook"
	"exchangeManager/internal/types"
)

// newTestEngine creates a minimal engine for testing without snapshots or
// background goroutines. It sets up one TATA_INR orderbook.
func newTestEngine() *Engine {
	return &Engine{
		Orderbooks: []*orderbook.Orderbook{
			orderbook.NewOrderbook("TATA", "INR", nil, nil, 0, 138),
		},
		PriceList: map[string]int64{"TATA": 138},
		Balances:  make(map[string]types.UserBalance),
	}
}

// ---------------------------------------------------------------------------
// ensureBalance
// ---------------------------------------------------------------------------

func TestEnsureBalanceCreatesEntries(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "INR")

	if _, ok := e.Balances["u1"]; !ok {
		t.Fatal("expected user u1 to exist in Balances")
	}
	b := e.Balances["u1"]["INR"]
	if b == nil {
		t.Fatal("expected INR balance to be initialized")
	}
	if b.Available != 0 || b.Locked != 0 {
		t.Errorf("expected zero balance, got available=%d locked=%d", b.Available, b.Locked)
	}
}

func TestEnsureBalanceIdempotent(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "INR")
	e.Balances["u1"]["INR"].Available = 500

	// Call again — should not reset the balance.
	e.ensureBalance("u1", "INR")
	if e.Balances["u1"]["INR"].Available != 500 {
		t.Errorf("ensureBalance reset existing balance, got %d", e.Balances["u1"]["INR"].Available)
	}
}

// ---------------------------------------------------------------------------
// onRamp
// ---------------------------------------------------------------------------

func TestOnRampNewUser(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 10000)

	bal := e.Balances["u1"][BaseCurrency]
	if bal == nil || bal.Available != 10000 {
		t.Fatalf("expected 10000 available after onRamp, got %v", bal)
	}
}

func TestOnRampExistingUser(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 5000)
	e.onRamp("u1", 3000)

	if e.Balances["u1"][BaseCurrency].Available != 8000 {
		t.Errorf("expected 8000 after two onRamps, got %d", e.Balances["u1"][BaseCurrency].Available)
	}
}

// ---------------------------------------------------------------------------
// getBalance
// ---------------------------------------------------------------------------

func TestGetBalanceReturnsZeroForUnknownUser(t *testing.T) {
	e := newTestEngine()
	if got := e.getBalance("unknown", "INR"); got != "0" {
		t.Errorf("expected '0', got '%s'", got)
	}
}

func TestGetBalanceReturnsCorrectValue(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 42000)
	if got := e.getBalance("u1", BaseCurrency); got != "42000" {
		t.Errorf("expected '42000', got '%s'", got)
	}
}

func TestGetBalanceReturnsZeroForUnknownAsset(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 100)
	if got := e.getBalance("u1", "BTC"); got != "0" {
		t.Errorf("expected '0' for unknown asset, got '%s'", got)
	}
}

// ---------------------------------------------------------------------------
// getPrice
// ---------------------------------------------------------------------------

func TestGetPrice(t *testing.T) {
	e := newTestEngine()
	if got := e.getPrice("TATA"); got != 138 {
		t.Errorf("expected 138, got %d", got)
	}
	if got := e.getPrice("UNKNOWN"); got != 0 {
		t.Errorf("expected 0 for unknown asset, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// setBaseBalances
// ---------------------------------------------------------------------------

func TestSetBaseBalances(t *testing.T) {
	e := newTestEngine()
	e.setBaseBalances()

	expectedUsers := []string{"1", "2", "3", "6", "7"}
	for _, uid := range expectedUsers {
		ub, ok := e.Balances[uid]
		if !ok {
			t.Errorf("expected user %s to have balance", uid)
			continue
		}
		inr := ub[BaseCurrency]
		if inr == nil {
			t.Errorf("user %s: missing INR balance", uid)
			continue
		}
		if inr.Available <= 0 {
			t.Errorf("user %s: expected positive INR available, got %d", uid, inr.Available)
		}
		tata := ub["TATA"]
		if tata == nil || tata.Available != 100000000 {
			t.Errorf("user %s: expected TATA available=100000000, got %v", uid, tata)
		}
	}
}

// ---------------------------------------------------------------------------
// checkAndLockFunds
// ---------------------------------------------------------------------------

func TestCheckAndLockFundsBuySuccess(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 10000) // 10000 INR

	err := e.checkAndLockFunds("TATA", "INR", "buy", "u1", 100, 50) // 100*50 = 5000
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if e.Balances["u1"]["INR"].Available != 5000 {
		t.Errorf("expected 5000 available after lock, got %d", e.Balances["u1"]["INR"].Available)
	}
	if e.Balances["u1"]["INR"].Locked != 5000 {
		t.Errorf("expected 5000 locked, got %d", e.Balances["u1"]["INR"].Locked)
	}
}

func TestCheckAndLockFundsBuyInsufficientFunds(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 100) // Only 100 INR

	err := e.checkAndLockFunds("TATA", "INR", "buy", "u1", 100, 50) // needs 5000
	if err != types.ErrInsufficientFunds {
		t.Errorf("expected ErrInsufficientFunds, got %v", err)
	}
	// Balance should be untouched.
	if e.Balances["u1"]["INR"].Available != 100 {
		t.Errorf("expected balance unchanged at 100, got %d", e.Balances["u1"]["INR"].Available)
	}
}

func TestCheckAndLockFundsSellSuccess(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "TATA")
	e.Balances["u1"]["TATA"].Available = 500

	err := e.checkAndLockFunds("TATA", "INR", "sell", "u1", 100, 200)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if e.Balances["u1"]["TATA"].Available != 300 {
		t.Errorf("expected 300 available, got %d", e.Balances["u1"]["TATA"].Available)
	}
	if e.Balances["u1"]["TATA"].Locked != 200 {
		t.Errorf("expected 200 locked, got %d", e.Balances["u1"]["TATA"].Locked)
	}
}

func TestCheckAndLockFundsSellInsufficientFunds(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "TATA")
	e.Balances["u1"]["TATA"].Available = 10

	err := e.checkAndLockFunds("TATA", "INR", "sell", "u1", 100, 50) // needs 50
	if err != types.ErrInsufficientFunds {
		t.Errorf("expected ErrInsufficientFunds, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// addOrderbook / getOrderbook
// ---------------------------------------------------------------------------

func TestAddAndGetOrderbook(t *testing.T) {
	e := newTestEngine()
	if ob := e.getOrderbook("TATA_INR"); ob == nil {
		t.Fatal("expected TATA_INR orderbook to exist")
	}
	if ob := e.getOrderbook("BTC_USD"); ob != nil {
		t.Fatal("expected nil for non-existent market")
	}

	e.addOrderbook(orderbook.NewOrderbook("BTC", "USD", nil, nil, 0, 50000))
	if ob := e.getOrderbook("BTC_USD"); ob == nil {
		t.Fatal("expected BTC_USD orderbook after addOrderbook")
	}
}

// ---------------------------------------------------------------------------
// createOrder (core ordering logic, bypasses Redis publish)
// Note: createOrder calls Redis internally. These tests will use the
// singleton RedisManager which connects to localhost:6379. The tests still
// validate the orderbook and balance logic even if Redis isn't running
// (publish errors are logged but don't fail the operation).
// ---------------------------------------------------------------------------

func TestCreateOrderInvalidMarket(t *testing.T) {
	e := newTestEngine()
	_, err := e.createOrder("FAKE_MKT", "100", "10", "buy", "u1")
	if err != types.ErrInvalidMarket {
		t.Errorf("expected ErrInvalidMarket, got %v", err)
	}
}

func TestCreateOrderBuyInsufficientFunds(t *testing.T) {
	e := newTestEngine()
	// No balance for u1.
	_, err := e.createOrder("TATA_INR", "100", "10", "buy", "u1") // needs 1000 INR
	if err != types.ErrInsufficientFunds {
		t.Errorf("expected ErrInsufficientFunds, got %v", err)
	}
}

func TestCreateOrderBuyNoMatch(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 100000)

	result, err := e.createOrder("TATA_INR", "100", "10", "buy", "u1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty (no asks), got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 0 {
		t.Errorf("expected 0 fills, got %d", len(result.Fills))
	}
	if result.OrderId == "" {
		t.Error("expected order ID to be assigned")
	}

	// Funds should be locked.
	if e.Balances["u1"]["INR"].Available != 99000 { // 100000 - 100*10
		t.Errorf("expected 99000 available, got %d", e.Balances["u1"]["INR"].Available)
	}
	if e.Balances["u1"]["INR"].Locked != 1000 {
		t.Errorf("expected 1000 locked, got %d", e.Balances["u1"]["INR"].Locked)
	}

	// Order should be in the orderbook.
	ob := e.getOrderbook("TATA_INR")
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid in orderbook, got %d", len(ob.Bids))
	}
}

func TestCreateOrderSellNoMatch(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "TATA")
	e.Balances["u1"]["TATA"].Available = 1000

	result, err := e.createOrder("TATA_INR", "200", "10", "sell", "u1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ExecutedQty != 0 {
		t.Errorf("expected 0 executedQty (no bids), got %d", result.ExecutedQty)
	}

	// TATA should be locked.
	if e.Balances["u1"]["TATA"].Available != 990 {
		t.Errorf("expected 990 available, got %d", e.Balances["u1"]["TATA"].Available)
	}
	if e.Balances["u1"]["TATA"].Locked != 10 {
		t.Errorf("expected 10 locked, got %d", e.Balances["u1"]["TATA"].Locked)
	}
}

func TestCreateOrderBuySellMatch(t *testing.T) {
	e := newTestEngine()

	// Seller: has TATA
	e.ensureBalance("seller", "TATA")
	e.Balances["seller"]["TATA"].Available = 1000
	e.ensureBalance("seller", "INR")

	// Buyer: has INR
	e.onRamp("buyer", 100000)
	e.ensureBalance("buyer", "TATA")

	// Seller places ask at 150.
	_, err := e.createOrder("TATA_INR", "150", "10", "sell", "seller")
	if err != nil {
		t.Fatalf("seller order failed: %v", err)
	}
	// Seller: 990 TATA available, 10 locked.
	if e.Balances["seller"]["TATA"].Available != 990 {
		t.Errorf("seller TATA available: expected 990, got %d", e.Balances["seller"]["TATA"].Available)
	}
	if e.Balances["seller"]["TATA"].Locked != 10 {
		t.Errorf("seller TATA locked: expected 10, got %d", e.Balances["seller"]["TATA"].Locked)
	}

	// Buyer places bid at 150 for 10 — should fully match.
	result, err := e.createOrder("TATA_INR", "150", "10", "buy", "buyer")
	if err != nil {
		t.Fatalf("buyer order failed: %v", err)
	}
	if result.ExecutedQty != 10 {
		t.Errorf("expected executedQty=10, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(result.Fills))
	}
	if result.Fills[0].Quantity != 10 {
		t.Errorf("expected fill qty=10, got %d", result.Fills[0].Quantity)
	}

	// After match:
	// Buyer: INR locked should be 0 (was locked then unlocked via updateBalance)
	//        TATA available should be 10 (received from fill)
	if e.Balances["buyer"]["TATA"].Available != 10 {
		t.Errorf("buyer TATA available: expected 10, got %d", e.Balances["buyer"]["TATA"].Available)
	}
	if e.Balances["buyer"]["INR"].Available != 98500 { // 100000 - 1500
		t.Errorf("buyer INR available: expected 98500, got %d", e.Balances["buyer"]["INR"].Available)
	}

	// Seller: INR available should increase by 1500 (10 * 150)
	//         TATA locked should be 0
	if e.Balances["seller"]["INR"].Available != 1500 {
		t.Errorf("seller INR available: expected 1500, got %d", e.Balances["seller"]["INR"].Available)
	}
	if e.Balances["seller"]["TATA"].Locked != 0 {
		t.Errorf("seller TATA locked: expected 0, got %d", e.Balances["seller"]["TATA"].Locked)
	}

	// Orderbook should be empty.
	ob := e.getOrderbook("TATA_INR")
	if len(ob.Bids) != 0 {
		t.Errorf("expected 0 bids after full match, got %d", len(ob.Bids))
	}
	if len(ob.Asks) != 0 {
		t.Errorf("expected 0 asks after full match, got %d", len(ob.Asks))
	}
}

func TestCreateOrderPartialMatch(t *testing.T) {
	e := newTestEngine()

	// Seller has 5 TATA.
	e.ensureBalance("seller", "TATA")
	e.Balances["seller"]["TATA"].Available = 100

	// Buyer has INR.
	e.onRamp("buyer", 100000)

	// Ask for 5 at 100.
	_, err := e.createOrder("TATA_INR", "100", "5", "sell", "seller")
	if err != nil {
		t.Fatalf("sell order failed: %v", err)
	}

	// Bid for 10 at 100 — only 5 can fill.
	result, err := e.createOrder("TATA_INR", "100", "10", "buy", "buyer")
	if err != nil {
		t.Fatalf("buy order failed: %v", err)
	}
	if result.ExecutedQty != 5 {
		t.Errorf("expected executedQty=5, got %d", result.ExecutedQty)
	}
	if len(result.Fills) != 1 {
		t.Errorf("expected 1 fill, got %d", len(result.Fills))
	}

	// Remaining 5 should be in bids.
	ob := e.getOrderbook("TATA_INR")
	if len(ob.Bids) != 1 {
		t.Fatalf("expected 1 bid remaining, got %d", len(ob.Bids))
	}
}

// ---------------------------------------------------------------------------
// cancelOrder
// ---------------------------------------------------------------------------

func TestCancelBuyOrder(t *testing.T) {
	e := newTestEngine()
	e.onRamp("u1", 100000)

	// Place a buy order.
	result, err := e.createOrder("TATA_INR", "100", "10", "buy", "u1")
	if err != nil {
		t.Fatalf("order creation failed: %v", err)
	}

	// Verify funds are locked.
	if e.Balances["u1"]["INR"].Locked != 1000 {
		t.Errorf("expected 1000 locked, got %d", e.Balances["u1"]["INR"].Locked)
	}

	// Cancel the order.
	e.cancelOrder(context.Background(), result.OrderId, "TATA_INR")

	// Funds should be unlocked.
	if e.Balances["u1"]["INR"].Locked != 0 {
		t.Errorf("expected 0 locked after cancel, got %d", e.Balances["u1"]["INR"].Locked)
	}
	if e.Balances["u1"]["INR"].Available != 100000 {
		t.Errorf("expected 100000 available after cancel, got %d", e.Balances["u1"]["INR"].Available)
	}

	// Orderbook should be empty.
	ob := e.getOrderbook("TATA_INR")
	if len(ob.Bids) != 0 {
		t.Errorf("expected 0 bids after cancel, got %d", len(ob.Bids))
	}
}

func TestCancelSellOrder(t *testing.T) {
	e := newTestEngine()
	e.ensureBalance("u1", "TATA")
	e.Balances["u1"]["TATA"].Available = 1000

	result, err := e.createOrder("TATA_INR", "200", "10", "sell", "u1")
	if err != nil {
		t.Fatalf("order creation failed: %v", err)
	}

	// Verify TATA locked.
	if e.Balances["u1"]["TATA"].Locked != 10 {
		t.Errorf("expected 10 locked, got %d", e.Balances["u1"]["TATA"].Locked)
	}

	e.cancelOrder(context.Background(), result.OrderId, "TATA_INR")

	if e.Balances["u1"]["TATA"].Locked != 0 {
		t.Errorf("expected 0 locked after cancel, got %d", e.Balances["u1"]["TATA"].Locked)
	}
	if e.Balances["u1"]["TATA"].Available != 1000 {
		t.Errorf("expected 1000 available after cancel, got %d", e.Balances["u1"]["TATA"].Available)
	}

	ob := e.getOrderbook("TATA_INR")
	if len(ob.Asks) != 0 {
		t.Errorf("expected 0 asks after cancel, got %d", len(ob.Asks))
	}
}

func TestCancelNonExistentOrder(t *testing.T) {
	e := newTestEngine()
	// Should not panic.
	e.cancelOrder(context.Background(), "nonexistent", "TATA_INR")
}

// ---------------------------------------------------------------------------
// updateBalance (tested indirectly via createOrder match, but also directly)
// ---------------------------------------------------------------------------

func TestUpdateBalanceBuySide(t *testing.T) {
	e := newTestEngine()
	// Setup: buyer has 10000 INR locked, seller has 100 TATA locked.
	e.ensureBalance("buyer", "INR")
	e.Balances["buyer"]["INR"].Locked = 10000
	e.ensureBalance("seller", "TATA")
	e.Balances["seller"]["TATA"].Locked = 100

	fills := []types.Fill{
		{Price: 100, Quantity: 50, OtherUserId: "seller"},
	}
	e.updateBalance("buyer", "TATA", "INR", "buy", fills)

	// Buyer: INR locked -5000, TATA available +50
	if e.Balances["buyer"]["INR"].Locked != 5000 {
		t.Errorf("buyer INR locked: expected 5000, got %d", e.Balances["buyer"]["INR"].Locked)
	}
	if e.Balances["buyer"]["TATA"].Available != 50 {
		t.Errorf("buyer TATA available: expected 50, got %d", e.Balances["buyer"]["TATA"].Available)
	}
	// Seller: INR available +5000, TATA locked -50
	if e.Balances["seller"]["INR"].Available != 5000 {
		t.Errorf("seller INR available: expected 5000, got %d", e.Balances["seller"]["INR"].Available)
	}
	if e.Balances["seller"]["TATA"].Locked != 50 {
		t.Errorf("seller TATA locked: expected 50, got %d", e.Balances["seller"]["TATA"].Locked)
	}
}

func TestUpdateBalanceSellSide(t *testing.T) {
	e := newTestEngine()
	// Setup: seller has 100 TATA locked, buyer has 10000 INR locked.
	e.ensureBalance("seller", "TATA")
	e.Balances["seller"]["TATA"].Locked = 100
	e.ensureBalance("buyer", "INR")
	e.Balances["buyer"]["INR"].Locked = 10000

	fills := []types.Fill{
		{Price: 100, Quantity: 50, OtherUserId: "buyer"},
	}
	e.updateBalance("seller", "TATA", "INR", "sell", fills)

	// Seller: TATA locked -50, INR available +5000
	if e.Balances["seller"]["TATA"].Locked != 50 {
		t.Errorf("seller TATA locked: expected 50, got %d", e.Balances["seller"]["TATA"].Locked)
	}
	if e.Balances["seller"]["INR"].Available != 5000 {
		t.Errorf("seller INR available: expected 5000, got %d", e.Balances["seller"]["INR"].Available)
	}
	// Buyer: INR locked -5000, TATA available +50
	if e.Balances["buyer"]["INR"].Locked != 5000 {
		t.Errorf("buyer INR locked: expected 5000, got %d", e.Balances["buyer"]["INR"].Locked)
	}
	if e.Balances["buyer"]["TATA"].Available != 50 {
		t.Errorf("buyer TATA available: expected 50, got %d", e.Balances["buyer"]["TATA"].Available)
	}
}

// ---------------------------------------------------------------------------
// generateOrderId
// ---------------------------------------------------------------------------

func TestGenerateOrderIdLength(t *testing.T) {
	id := generateOrderId()
	if len(id) != 13 {
		t.Errorf("expected 13-char order ID, got %d: %s", len(id), id)
	}
}

func TestGenerateOrderIdUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := generateOrderId()
		if seen[id] {
			t.Fatalf("duplicate order ID generated: %s", id)
		}
		seen[id] = true
	}
}
