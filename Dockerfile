# One parameterized Dockerfile builds any gohex service:
#   docker build --build-arg SERVICE=ordering .
# The whole workspace is copied so go.work resolves the local modules.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
ARG SERVICE
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/app ./services/${SERVICE}/cmd/${SERVICE}

FROM alpine:3.20
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
