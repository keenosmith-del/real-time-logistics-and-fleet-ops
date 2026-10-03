FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /fleet ./cmd/server
FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && adduser -D fleet
WORKDIR /app
COPY --from=build /fleet /app/fleet
COPY migrations /app/migrations
USER fleet
EXPOSE 8080
CMD ["/app/fleet"]
