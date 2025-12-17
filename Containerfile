FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY bin/bq-runner-linux-aarch64 /app/bq-runner

RUN useradd -r -s /bin/false bqrunner && \
    chown -R bqrunner:bqrunner /app

USER bqrunner

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

ENTRYPOINT ["/app/bq-runner"]
CMD ["--mode", "mock", "--port", "3000"]
