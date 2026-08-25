FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev git ca-certificates

WORKDIR /app

COPY . .
ENV GOTOOLCHAIN=auto
RUN go get go.mau.fi/whatsmeow@latest && \
    go get github.com/mattn/go-sqlite3@latest && \
    go get github.com/skip2/go-qrcode@latest && \
    go get google.golang.org/protobuf@latest && \
    go mod tidy && \
    go mod download

RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -o whatsmeow-api .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates sqlite-libs tzdata

WORKDIR /app

COPY --from=builder /app/whatsmeow-api .

RUN mkdir -p /app/data

ENV DB_PATH="file:/app/data/whatsapp.db?_foreign_keys=on&mode=rwc"

EXPOSE 8080

CMD ["./whatsmeow-api"]
