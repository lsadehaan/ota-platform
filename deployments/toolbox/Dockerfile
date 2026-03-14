FROM golang:1.26-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash git curl librdkafka-dev pkg-config gcc libc6-dev && rm -rf /var/lib/apt/lists/*
ENV CGO_ENABLED=1
WORKDIR /workspace
ENTRYPOINT ["bash", "-c"]
CMD ["sleep infinity"]
