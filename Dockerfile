FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev git ca-certificates

WORKDIR /app

COPY . .
RUN go get go.mau.fi/whatsmeow@latest && \
    go get github.com/mattn/go-sqlite3@latest && \
    go get github.com/skip2/go-qrcode@latest && \
    go get google.golang.org/protobuf@latest && \
    go mod tidy

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
