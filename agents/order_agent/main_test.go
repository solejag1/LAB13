package main

import (
	"testing"
)

func TestValidateOrder_ValidPayload_ReturnsOutput(t *testing.T) {
	payload := map[string]interface{}{
		"order_id":     "test-123",
		"table_number": float64(5),
		"items": []interface{}{
			map[string]interface{}{"name": "Борщ", "price": 350.0, "quantity": float64(2)},
			map[string]interface{}{"name": "Чай", "price": 120.0, "quantity": float64(1)},
		},
	}

	output, err := validateOrder(payload)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output["status"] != "validated" {
		t.Errorf("expected status=validated, got %v", output["status"])
	}
	total, ok := output["total"].(float64)
	if !ok {
		t.Fatal("total not float64")
	}
	// 350*2 + 120*1 = 820
	if total != 820.0 {
		t.Errorf("expected total=820.0, got %v", total)
	}
}

func TestValidateOrder_MissingOrderID_ReturnsError(t *testing.T) {
	payload := map[string]interface{}{
		"table_number": float64(3),
		"items":        []interface{}{map[string]interface{}{"name": "Стейк", "price": 850.0}},
	}

	_, err := validateOrder(payload)

	if err == nil {
		t.Fatal("expected error for missing order_id")
	}
}

func TestValidateOrder_EmptyItems_ReturnsError(t *testing.T) {
	payload := map[string]interface{}{
		"order_id":     "test-456",
		"table_number": float64(2),
		"items":        []interface{}{},
	}

	_, err := validateOrder(payload)

	if err == nil {
		t.Fatal("expected error for empty items")
	}
}

func TestValidateOrder_InvalidTableNumber_ReturnsError(t *testing.T) {
	payload := map[string]interface{}{
		"order_id": "test-789",
		"items":    []interface{}{map[string]interface{}{"name": "Суп", "price": 300.0}},
	}

	_, err := validateOrder(payload)

	if err == nil {
		t.Fatal("expected error for missing table_number")
	}
}

func TestValidateOrder_TotalCalculation_MultipeItems(t *testing.T) {
	payload := map[string]interface{}{
		"order_id":     "test-total",
		"table_number": float64(1),
		"items": []interface{}{
			map[string]interface{}{"name": "A", "price": 100.0, "quantity": float64(3)},
			map[string]interface{}{"name": "B", "price": 200.0, "quantity": float64(2)},
		},
	}

	output, err := validateOrder(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	total := output["total"].(float64)
	// 100*3 + 200*2 = 700
	if total != 700.0 {
		t.Errorf("expected 700.0, got %v", total)
	}
}

func TestValidateOrder_SingleItemDefaultQty_CountsOne(t *testing.T) {
	payload := map[string]interface{}{
		"order_id":     "test-qty",
		"table_number": float64(4),
		"items": []interface{}{
			map[string]interface{}{"name": "Пицца", "price": 500.0},
		},
	}

	output, err := validateOrder(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	total := output["total"].(float64)
	if total != 500.0 {
		t.Errorf("expected 500.0, got %v", total)
	}
}

func TestValidateOrder_OrderIDPresentInOutput(t *testing.T) {
	orderID := "check-passthrough"
	payload := map[string]interface{}{
		"order_id":     orderID,
		"table_number": float64(7),
		"items":        []interface{}{map[string]interface{}{"name": "X", "price": 50.0}},
	}

	output, err := validateOrder(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output["order_id"] != orderID {
		t.Errorf("order_id not passed through: got %v", output["order_id"])
	}
}

func TestValidateOrder_TableNumberPresentInOutput(t *testing.T) {
	payload := map[string]interface{}{
		"order_id":     "tbl-check",
		"table_number": float64(9),
		"items":        []interface{}{map[string]interface{}{"name": "Y", "price": 100.0}},
	}

	output, err := validateOrder(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if output["table_number"] != float64(9) {
		t.Errorf("table_number not correct in output: %v", output["table_number"])
	}
}
