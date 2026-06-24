FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /service-a .

FROM alpine:3.19
COPY --from=builder /service-a /service-a
EXPOSE 8081
ENTRYPOINT ["/service-a"]
