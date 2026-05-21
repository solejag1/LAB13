package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Task represents an incoming task from the orchestrator.
type Task struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Payload    map[string]interface{} `json:"payload"`
	TraceID    string                 `json:"trace_id"`
	RetryCount int                    `json:"retry_count"`
}

// TaskResult is sent back to the orchestrator.
type TaskResult struct {
	TaskID  string                 `json:"task_id"`
	Success bool                   `json:"success"`
	Output  map[string]interface{} `json:"output"`
	Error   string                 `json:"error"`
	AgentID string                 `json:"agent_id"`
	TraceID string                 `json:"trace_id"`
}

var (
	agentID   = getenv("AGENT_ID", "order-agent-1")
	natsURL   = getenv("NATS_URL", "nats://localhost:4222")
	otlpEndpt = getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	processed int64
	logger    = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpt),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("order-agent"),
			attribute.String("agent.id", agentID),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

// validateOrder checks required fields and business rules.
func validateOrder(payload map[string]interface{}) (map[string]interface{}, error) {
	orderID, ok := payload["order_id"].(string)
	if !ok || orderID == "" {
		return nil, fmt.Errorf("missing order_id")
	}
	items, ok := payload["items"].([]interface{})
	if !ok || len(items) == 0 {
		return nil, fmt.Errorf("order must have at least one item")
	}
	tableNum, ok := payload["table_number"].(float64)
	if !ok || tableNum <= 0 {
		return nil, fmt.Errorf("invalid table_number")
	}

	// Calculate total
	total := 0.0
	for _, raw := range items {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		price, _ := item["price"].(float64)
		qty, _ := item["quantity"].(float64)
		if qty == 0 {
			qty = 1
		}
		total += price * qty
	}

	logger.Info("order validated",
		"order_id", orderID,
		"items_count", len(items),
		"total", total,
	)

	return map[string]interface{}{
		"order_id":     orderID,
		"table_number": tableNum,
		"items":        items,
		"total":        total,
		"validated_at": time.Now().UTC().Format(time.RFC3339),
		"status":       "validated",
	}, nil
}

func handleTask(nc *nats.Conn, msg *nats.Msg) {
	processed++
	start := time.Now()

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		logger.Error("unmarshal task", "error", err)
		return
	}

	if task.Type != "validate_order" {
		logger.Debug("skipping task type", "type", task.Type)
		return
	}

	tracer := otel.Tracer("order-agent")
	ctx, span := tracer.Start(context.Background(), "validate_order")
	defer span.End()
	span.SetAttributes(attribute.String("task.id", task.ID))
	_ = ctx

	logger.Info("processing task",
		"task_id", task.ID,
		"agent", agentID,
		"retry", task.RetryCount,
	)

	output, err := validateOrder(task.Payload)
	result := TaskResult{
		TaskID:  task.ID,
		AgentID: agentID,
		TraceID: task.TraceID,
	}
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		logger.Error("validation failed", "task_id", task.ID, "error", err)
	} else {
		result.Success = true
		result.Output = output
	}

	data, _ := json.Marshal(result)
	if pubErr := nc.Publish("tasks.completed", data); pubErr != nil {
		logger.Error("publish result", "error", pubErr)
	}

	logger.Info("task done",
		"task_id", task.ID,
		"success", result.Success,
		"elapsed_ms", time.Since(start).Milliseconds(),
		"processed_total", processed,
	)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tp, err := initTracer(ctx)
	if err != nil {
		logger.Warn("tracer init failed (continuing without tracing)", "error", err)
	} else {
		defer func() { _ = tp.Shutdown(context.Background()) }()
	}

	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		logger.Error("nats connect", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	sub, err := nc.QueueSubscribe("tasks.process", "order-agents", func(msg *nats.Msg) {
		handleTask(nc, msg)
	})
	if err != nil {
		logger.Error("subscribe", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	logger.Info("order-agent started", "agent_id", agentID, "nats", natsURL)
	<-ctx.Done()
	logger.Info("order-agent shutting down", "processed", processed)
}
