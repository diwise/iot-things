FROM --platform=$BUILDPLATFORM docker.io/golang:1.25 AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

WORKDIR /app/cmd/iot-things

ARG TARGETARCH
ARG TARGETOS
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build

FROM registry.access.redhat.com/ubi9/ubi-minimal

WORKDIR /opt/diwise

COPY --from=builder --chown=1001 /app/assets/config/config.yaml /opt/diwise/config/config.yaml
COPY --from=builder --chown=1001 /app/cmd/iot-things/iot-things /opt/diwise

RUN chown 1001 /opt/diwise

EXPOSE 8080
USER 1001

ENTRYPOINT ["/opt/diwise/iot-things"]