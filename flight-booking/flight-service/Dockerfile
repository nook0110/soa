FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git protobuf-dev protobuf

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

COPY proto/ ./proto/
RUN mkdir -p proto/flight
RUN protoc --go_out=. --go_opt=module=flight-service \
    --go-grpc_out=. --go-grpc_opt=module=flight-service \
    proto/flight_service.proto

COPY main.go .
COPY migrations ./migrations
RUN CGO_ENABLED=0 GOOS=linux go build -o flight-service .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/flight-service .
COPY --from=builder /app/migrations ./migrations

EXPOSE 50051

CMD ["./flight-service"]
