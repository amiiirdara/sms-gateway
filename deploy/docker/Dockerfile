# Generic multi-stage Dockerfile shared by every cmd/ binary. docker-compose
# passes --build-arg SERVICE=<name> to select which one gets built.
ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/service ./cmd/${SERVICE}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/service /service
ENTRYPOINT ["/service"]
