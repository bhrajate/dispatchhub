# ---- Build stage ----
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown
ARG COMPONENT=scheduler

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags "-s -w \
      -X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.Version=${VERSION} \
      -X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.GitCommit=${GIT_COMMIT} \
      -X github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version.BuildDate=${BUILD_DATE}" \
    -o /bin/dispatchhub-${COMPONENT} ./cmd/${COMPONENT}

# ---- Runtime stage ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S dispatchhub && adduser -S dispatchhub -G dispatchhub

ARG COMPONENT=scheduler

COPY --from=builder /bin/dispatchhub-${COMPONENT} /usr/local/bin/dispatchhub
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

USER dispatchhub

EXPOSE 8080 9090 9091

ENTRYPOINT ["dispatchhub"]
