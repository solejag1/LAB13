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

const maxTables = 20

var (
	agentID   = getenv("AGENT_ID", "table-agent-1")
	natsURL   = getenv("NATS_URL", "nats://localhost:4222")
	redisURL  = getenv("REDIS_URL", "redis://localhost:6379")
	otlpEndpt = getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	processed atomic.Int64
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
			semconv.ServiceName("table-agent"),
			attribute.String("agent.id", agentID),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}

func currentCost() float64 {
	return 1.0 + float64(processed.Load())*0.05
}

func assignTable(ctx context.Context, payload map[string]interface{}) (map[string]interface{}, error) {
	orderID, ok := payload["order_id"].(string)
	if !ok || orderID == "" {
		return nil, fmt.Errorf("missing order_id")
	}

	tableNum := 0
	if tn, ok := payload["table_number"].(float64); ok {
		tableNum = int(tn)
	}

	if rdb == nil {
		// stateless fallback
		if tableNum == 0 {
			tableNum = 1
		}
		return map[string]interface{}{
			"order_id":     orderID,
			"table_number": tableNum,
			"assigned_at":  time.Now().UTC().Format(time.RFC3339),
			"status":       "assigned",
		}, nil
	}

	if tableNum == 0 {
		for i := 1; i <= maxTables; i++ {
			exists, err := rdb.Exists(ctx, fmt.Sprintf("table:occupied:%d", i)).Result()
			if err != nil || exists == 0 {
				tableNum = i
				break
			}
		}
		if tableNum == 0 {
			return nil, fmt.Errorf("no free tables available")
		}
	}

	tableKey := fmt.Sprintf("table:occupied:%d", tableNum)
	orderKey := fmt.Sprintf("table:order:%d", tableNum)
	pipe := rdb.Pipeline()
	pipe.Set(ctx, tableKey, orderID, 4*time.Hour)
	pipe.HSet(ctx, orderKey,
		"order_id", orderID,
		"table_number", tableNum,
		"assigned_at", time.Now().UTC().Format(time.RFC3339),
		"agent_id", agentID,
	)
	pipe.Expire(ctx, orderKey, 4*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis assign: %w", err)
	}

	logger.Info("table assigned", "order_id", orderID, "table_number", tableNum)

	return map[string]interface{}{
		"order_id":     orderID,
		"table_number": tableNum,
		"assigned_at":  time.Now().UTC().Format(time.RFC3339),
		"status":       "assigned",
	}, nil
}

func handleTask(nc *nats.Conn, msg *nats.Msg) {
	processed.Add(1)
	start := time.Now()

	var task Task
	if err := json.Unmarshal(msg.Data, &task); err != nil {
		logger.Error("unmarshal task", "error", err)
		return
	}
	if task.Type != "assign_table" {
		return
	}

	tracer := otel.Tracer("table-agent")
	ctx, span := tracer.Start(context.Background(), "assign_table")
	defer span.End()
	span.SetAttributes(attribute.String("task.id", task.ID))

	logger.Info("processing task", "task_id", task.ID, "agent", agentID, "retry", task.RetryCount)

	output, err := assignTable(ctx, task.Payload)
	result := TaskResult{TaskID: task.ID, AgentID: agentID, TraceID: task.TraceID}
	if err != nil {
		result.Success = false
		result.Error = err.Error()
		logger.Error("table assign failed", "task_id", task.ID, "error", err)
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
	if task.Type != "assign_table" {
		return
	}
	bid := Bid{TaskID: task.ID, AgentID: agentID, Cost: currentCost(), TaskType: task.Type}
	data, _ := json.Marshal(bid)
	if err := nc.Publish("tasks.bids", data); err != nil {
		logger.Error("publish bid", "error", err)
	}
	logger.Debug("bid submitted", "task_id", task.ID, "cost", bid.Cost)
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

	broadSub, err := nc.QueueSubscribe("tasks.process", "table-agents", func(msg *nats.Msg) { handleTask(nc, msg) })
	if err != nil {
		logger.Error("subscribe broadcast", "error", err)
		os.Exit(1)
	}
	defer broadSub.Unsubscribe()

	logger.Info("table-agent started", "agent_id", agentID, "nats", natsURL)
	<-ctx.Done()
	logger.Info("table-agent shutting down", "processed", processed.Load())
}
