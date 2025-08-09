FROM golang:1.24.6-bookworm AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /worker ./cmd/worker
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /reaper ./cmd/reaper
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /backfiller ./cmd/backfiller

FROM gcr.io/distroless/static:nonroot
COPY --from=builder --chmod=0755 /worker /worker
COPY --from=builder --chmod=0755 /reaper /reaper
COPY --from=builder --chmod=0755 /backfiller /backfiller
USER nonroot:nonroot
