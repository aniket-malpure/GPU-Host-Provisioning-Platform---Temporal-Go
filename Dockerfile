# syntax=docker/dockerfile:1
# Build after running `make tidy` so go.sum exists.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -tags kafka -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -tags kafka -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot AS worker
COPY --from=build /out/worker /worker
ENTRYPOINT ["/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build /out/api /api
EXPOSE 8080
ENTRYPOINT ["/api"]
