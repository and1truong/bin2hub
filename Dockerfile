FROM golang:1.17.6-alpine3.16

WORKDIR   /build
COPY    . /build/

RUN apk add --no-cache git
RUN go mod vendor
RUN go build -o /app cmd/main.go

# ---------------------
# Reset
# ---------------------
FROM alpine:3.15

COPY config.json ./
COPY --from=0 /app /app

RUN chmod +x /app
RUN apk add --no-cache ca-certificates

CMD ["/app"]
