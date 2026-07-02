FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app

COPY go.mod ./
RUN go mod download 2>/dev/null || true

COPY . .
RUN go mod tidy

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o whatsmeow-api .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates sqlite-libs tzdata

WORKDIR /app

COPY --from=builder /app/whatsmeow-api .

RUN mkdir -p /app/data

ENV DB_PATH="file:/app/data/whatsapp.db?_foreign_keys=on"
ENV PORT=8080

EXPOSE 8080

CMD ["./whatsmeow-api"]
