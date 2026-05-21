package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Task struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Payload    map[string]interface{} `json:"payload"`
	TraceID    string                 `json:"trace_id"`
	RetryCount int                    `json:"retry_count"`
}

type TaskResult struct {
	TaskID  string                 `json:"task_id"`
	Success bool                   `json:"success"`
	Output  map[string]interface{} `json:"output"`
	Error   string                 `json:"error"`
	AgentID string                 `json:"agent_id"`
	TraceID string                 `json:"trace_id"`
}

type Bid struct {
	TaskID   string  `json:"task_id"`
	AgentID  string  `json:"agent_id"`
	Cost     float64 `json:"cost"`
	TaskType string  `json:"task_type"`
}

var (
	agentID   = getenv("AGENT_ID", "delivery-agent-1")
	natsURL   = getenv("NATS_URL", "nats://localhost:4222")
	redisURL  = getenv("REDIS_URL", "redis://localhost:6379")
	otlpEndpt = getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	processed atomic.Int64
	active    atomic.Int64
	logger    = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rdb       *redis.Client
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initRedis() error {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("redis url: %w", err)
	}
	rdb = redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rdb.Ping(ctx).Err()
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
			semconv.ServiceName("delivery-agent"),
			attribute.String("agent.id", agentID),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

func currentCost() float64 {
	return 1.0 + float64(active.Load())*3.0
}

func deliverOrder(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	orderID, ok := payload["order_id"].(string)
	if !ok || orderID == "" {
		return nil, fmt.Errorf("missing order_id")
	}
	tableNum, ok := payload["table_number"].(float64)
	if !ok || tableNum <= 0 {
		return nil, fmt.Errorf("missing or invalid table_number")
	}

	time.Sleep(300 * time.Millisecond)
	deliveredAt := time.Now().UTC().Format(time.RFC3339)

	if rdb != nil {
		orderKey := fmt.Sprintf("order:status:%s", orderID)
		if err := rdb.HSet(ctx, orderKey,
			"status", "delivered",
			"delivered_at", deliveredAt,
			"delivered_by", agentID,
		).Err(); err != nil {
			logger.Warn("redis update status", "error", err)
		}
		rdb.Expire(ctx, orderKey, 24*time.Hour)

		eventData, _ := json.Marshal(map[string]interface{}{
			"event":        "order.delivered",
			"order_id":     orderID,
			"table_number": tableNum,
			"delivered_at": deliveredAt,
		})
		if err := rdb.Publish(ctx, "restaurant:events", eventData).Err(); err != nil {
			logger.Warn("redis publish event", "error", err)
		}
	}

	logger.Info("order delivered", "order_id", orderID, "table_number", tableNum, "agent", agentID)

	return map[string]interface{}{
		"order_id":     orderID,
		"table_number": tableNum,
		"status":       "delivered",
		"delivered_by": agentID,
		"delivered_at": deliveredAt,
	}, nil
}

func handleTask(nc *nats.Conn, msg *nats.Msg) {
	processed.Add(1)
	active.Add(1)
	defer active.Add(-1)
	start := time.Now()

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		logger.Error("unmarshal task", "error", err)
		return
	}
	if task.Type != "deliver_order" {
		return
	}

	tracer := otel.Tracer("delivery-agent")
	ctx, span := tracer.Start(context.Background(), "deliver_order")
	defer span.End()
	span.SetAttributes(attribute.String("task.id", task.ID))

	logger.Info("processing task", "task_id", task.ID, "agent", agentID, "retry", task.RetryCount)

	output, err := deliverOrder(ctx, task.Payload)
	result := TaskResult{TaskID: task.ID, AgentID: agentID, TraceID: task.TraceID}
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		logger.Error("delivery failed", "task_id", task.ID, "error", err)
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
		"processed_total", processed.Load(),
	)
}

func handleBid(nc *nats.Conn, msg *nats.Msg) {
	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		return
	}
	if task.Type != "deliver_order" {
		return
	}
	bid := Bid{TaskID: task.ID, AgentID: agentID, Cost: currentCost(), TaskType: task.Type}
	data, _ := json.Marshal(bid)
	if err := nc.Publish("tasks.bids", data); err != nil {
		logger.Error("publish bid", "error", err)
	}
	logger.Debug("bid submitted", "task_id", task.ID, "cost", bid.Cost, "active", active.Load())
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := initRedis(); err != nil {
		logger.Warn("redis init failed", "error", err)
	}

	tp, err := initTracer(ctx)
	if err != nil {
		logger.Warn("tracer init failed", "error", err)
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

	bidSub, err := nc.Subscribe("tasks.bid", func(msg *nats.Msg) { handleBid(nc, msg) })
	if err != nil {
		logger.Error("subscribe bid", "error", err)
		os.Exit(1)
	}
	defer bidSub.Unsubscribe()

	directSub, err := nc.Subscribe("tasks.direct."+agentID, func(msg *nats.Msg) { handleTask(nc, msg) })
	if err != nil {
		logger.Error("subscribe direct", "error", err)
		os.Exit(1)
	}
	defer directSub.Unsubscribe()

	broadSub, err := nc.QueueSubscribe("tasks.process", "delivery-agents", func(msg *nats.Msg) { handleTask(nc, msg) })
	if err != nil {
		logger.Error("subscribe broadcast", "error", err)
		os.Exit(1)
	}
	defer broadSub.Unsubscribe()

	logger.Info("delivery-agent started", "agent_id", agentID, "nats", natsURL)
	<-ctx.Done()
	logger.Info("delivery-agent shutting down", "processed", processed.Load())
}
