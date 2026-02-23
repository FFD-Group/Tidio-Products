# ─────────────────────────────────────────────────────────────────────────────
# Tidio Product Sync – container image
#
# Uses supercronic instead of the system cron daemon so that job output goes
# directly to the container's stdout/stderr (visible via `podman logs`).
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# supercronic: a container-friendly, daemonless cron runner
ARG SUPERCRONIC_VERSION=0.2.33
# Change to arm64 if your Hetzner node is ARM-based (rare – most are amd64)
ARG SUPERCRONIC_ARCH=amd64

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL \
    "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-${SUPERCRONIC_ARCH}" \
    -o /usr/local/bin/supercronic \
 && chmod +x /usr/local/bin/supercronic \
 && apt-get purge -y curl \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Directory for the rotating log file (mount a volume here for persistence)
RUN mkdir -p /app/logs

# Crontab consumed by supercronic
COPY crontab /app/crontab

# supercronic runs in the foreground and forwards all job output to stdout/stderr
CMD ["supercronic", "/app/crontab"]
