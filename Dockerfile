# ─────────────────────────────────────────────────────────────────────────────
# Tidio Product Sync – container image
#
# Uses a pure-Python scheduler (entrypoint.py) instead of an external cron
# binary, so there are no shell or third-party runtime dependencies.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY entrypoint.py .

# Directory for the persistent log file (mount a volume here for persistence)
RUN mkdir -p /app/logs

CMD ["/usr/local/bin/python", "/app/entrypoint.py"]
