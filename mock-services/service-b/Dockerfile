FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /service-b .

FROM alpine:3.19
COPY --from=builder /service-b /service-b
EXPOSE 8082
ENTRYPOINT ["/service-b"]
