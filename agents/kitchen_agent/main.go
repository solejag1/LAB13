package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
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
	agentID   = getenv("AGENT_ID", "kitchen-agent-1")
	natsURL   = getenv("NATS_URL", "nats://localhost:4222")
	redisURL  = getenv("REDIS_URL", "redis://localhost:6379")
	otlpEndpt = getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	processed atomic.Int64
	active    atomic.Int64 // currently processing tasks — used for bid cost
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
			semconv.ServiceName("kitchen-agent"),
			attribute.String("agent.id", agentID),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

// currentCost returns bid cost: idle agent bids low, busy agent bids high.
func currentCost() float64 {
	return 1.0 + float64(active.Load())*2.0
}

func cookDish(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	orderID, ok := payload["order_id"].(string)
	if !ok || orderID == "" {
		return nil, fmt.Errorf("missing order_id")
	}
	items, _ := payload["items"].([]interface{})

	// Redis state — safe nil check
	if rdb != nil {
		stateKey := fmt.Sprintf("kitchen:order:%s", orderID)
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, stateKey,
			"order_id", orderID,
			"status", "cooking",
			"agent_id", agentID,
			"started_at", time.Now().UTC().Format(time.RFC3339),
			"items_count", strconv.Itoa(len(items)),
		)
		pipe.Expire(ctx, stateKey, 24*time.Hour)
		pipe.Incr(ctx, fmt.Sprintf("kitchen:agent:%s:processed", agentID))
		if _, err := pipe.Exec(ctx); err != nil {
			logger.Warn("redis pipeline", "error", err)
		}
	}

	cookTime := time.Duration(len(items)) * 200 * time.Millisecond
	if cookTime > 2*time.Second {
		cookTime = 2 * time.Second
	}
	time.Sleep(cookTime)

	if rdb != nil {
		if err := rdb.HSet(ctx, fmt.Sprintf("kitchen:order:%s", orderID),
			"status", "ready",
			"ready_at", time.Now().UTC().Format(time.RFC3339),
		).Err(); err != nil {
			logger.Warn("redis update ready", "error", err)
		}
	}

	logger.Info("dishes cooked", "order_id", orderID, "items_count", len(items), "cook_time_ms", cookTime.Milliseconds())

	return map[string]interface{}{
		"order_id":  orderID,
		"items":     items,
		"status":    "ready",
		"cooked_by": agentID,
		"ready_at":  time.Now().UTC().Format(time.RFC3339),
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
	if task.Type != "cook_dish" {
		return
	}

	tracer := otel.Tracer("kitchen-agent")
	ctx, span := tracer.Start(context.Background(), "cook_dish")
	defer span.End()
	span.SetAttributes(attribute.String("task.id", task.ID))

	logger.Info("processing task", "task_id", task.ID, "agent", agentID, "retry", task.RetryCount)

	output, err := cookDish(ctx, task.Payload)
	result := TaskResult{TaskID: task.ID, AgentID: agentID, TraceID: task.TraceID}
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		logger.Error("cook failed", "task_id", task.ID, "error", err)
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
	if task.Type != "cook_dish" {
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
		logger.Warn("redis init failed (stateless mode)", "error", err)
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

	// Auction: respond to bid requests
	bidSub, err := nc.Subscribe("tasks.bid", func(msg *nats.Msg) {
		handleBid(nc, msg)
	})
	if err != nil {
		logger.Error("subscribe bid", "error", err)
		os.Exit(1)
	}
	defer bidSub.Unsubscribe()

	// Direct: receive tasks won at auction
	directSub, err := nc.Subscribe("tasks.direct."+agentID, func(msg *nats.Msg) {
		handleTask(nc, msg)
	})
	if err != nil {
		logger.Error("subscribe direct", "error", err)
		os.Exit(1)
	}
	defer directSub.Unsubscribe()

	// Fallback broadcast
	broadSub, err := nc.QueueSubscribe("tasks.process", "kitchen-agents", func(msg *nats.Msg) {
		handleTask(nc, msg)
	})
	if err != nil {
		logger.Error("subscribe broadcast", "error", err)
		os.Exit(1)
	}
	defer broadSub.Unsubscribe()

	logger.Info("kitchen-agent started", "agent_id", agentID, "nats", natsURL)
	<-ctx.Done()
	logger.Info("kitchen-agent shutting down", "processed", processed.Load())
}
