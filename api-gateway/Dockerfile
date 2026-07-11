FROM golang:1.25-alpine AS builder

# Install C dependencies for librdkafka
RUN apk add --no-cache \
    gcc \
    musl-dev \
    librdkafka-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /app/api-gateway ./main.go

FROM alpine:3.19

RUN apk --no-cache add \
    ca-certificates \
    librdkafka

WORKDIR /root/

COPY --from=builder /app/api-gateway .

EXPOSE 8080

CMD ["./api-gateway"]