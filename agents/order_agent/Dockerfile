FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o order_agent .

FROM alpine:3.19
RUN adduser -D -u 1001 appuser
WORKDIR /app
COPY --from=builder /app/order_agent .
USER appuser
CMD ["./order_agent"]
