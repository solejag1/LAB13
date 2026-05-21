FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o table_agent .

FROM alpine:3.19
RUN adduser -D -u 1001 appuser
WORKDIR /app
COPY --from=builder /app/table_agent .
USER appuser
CMD ["./table_agent"]
