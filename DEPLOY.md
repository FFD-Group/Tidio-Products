# Tidio Product Sync – Deployment Guide

Deploys the sync script as a Podman container on the Hetzner VPS. The container
runs a pure-Python scheduler (`entrypoint.py`) to fire:

- **Incremental sync** every 2 hours (00:00, 04:00, 06:00 … 22:00)
- **Full catalog sync** daily at 02:00 (replaces the 02:00 incremental slot)

---

## Prerequisites

- Hetzner VPS with SSH access
- Podman installed (`podman --version`)
- If Docker is also installed and running, stop it first:

```bash
sudo systemctl stop docker
sudo systemctl disable docker   # prevent it auto-starting on reboot
```

- The project repository cloned on the VPS (or files copied via `scp`/`rsync`)

---

## 1 · Prepare the working directories on the VPS

All files live under your admin user's home directory — no `sudo` needed for
day-to-day work, and no root-owned directories to fight with:

```bash
mkdir -p ~/tidio-sync/src ~/tidio-sync/logs
chmod 700 ~/tidio-sync          # other OS users cannot browse the folder
```

---

## 2 · Prepare the `.env` file on the VPS

Create `~/tidio-sync/.env` and populate it with **all** the variables the
script requires, plus the two new ones added for this deployment:

```dotenv
# ── Tidio ────────────────────────────────────────────────────────────
TIDIO_CLIENT_ID=
TIDIO_CLIENT_SECRET=
TIDIO_ACCEPT_API_VERSION=
TIDIO_MAX_REQ_PER_MIN=

# ── Magento ──────────────────────────────────────────────────────────
WEB_API_DOMAIN=
WEB_DOMAIN=
WEB_AUTH_HEADER_VALUE=
WEB_SECRET_NAME=
WEB_SECRET_PASS=
MAG_PRODUCTS_API_ENDPOINT=
MAG_CATEGORIES_API_ENDPOINT=
MAG_PRICES_API_ENDPOINT=
MAG_ATTRIBUTE_API_ENDPOINT=
MAG_STORE_ID=
MAG_WEBSITE_ID=
MAG_BRAND_ATTRIBUTE_CODE=
UPDATE_AGE_MINS=130        # must cover the 2 h window + a safety margin
EXCLUDED_FEATURES=[]
COLLECTIONS_PARENT_CATEGORY=collections
OUTPUT_FILE=/app/output.json

# ── Zoho ─────────────────────────────────────────────────────────────
Z_CLIENT_ID=
Z_CLIENT_SECRET=
Z_SCOPE=
Z_REFRESH_TOKEN=
Z_REGION=eu                # or com
Z_WD_MANIFEST_FOLDER_ID=
Z_WD_ROOT_FOLDER_NAME=
Z_WD_ROOT_FOLDER_ID=

# ── Notifications ─────────────────────────────────────────────────────
# Paste the webhook URL from your Zoho Flow trigger (see §5 below)
ZOHO_FLOW_WEBHOOK_URL=https://flow.zoho.eu/...
# Set to true if you also want a Cliq ping when an incremental finds 0 updates
NOTIFY_ON_EMPTY=false

# ── Logging ──────────────────────────────────────────────────────────
LOG_FILE=/app/logs/tidio_products.log
```

Restrict the file so only your user can read it:

```bash
touch ~/tidio-sync/.env
chmod 600 ~/tidio-sync/.env
# fill in values – e.g. nano ~/tidio-sync/.env
```

---

## 3 · Copy / pull the project files onto the VPS

Option A – git pull (recommended):

```bash
git clone git@github.com:<your-org>/<repo>.git ~/tidio-sync/src
# or, inside an existing clone:
cd ~/tidio-sync/src && git pull
```

Option B – rsync from your local machine:

```bash
rsync -av --exclude='.env' --exclude='*.log' \
  ./  user@your-vps:~/tidio-sync/src/
```

---

## 4 · Build the Podman image

SSH into the VPS and run:

```bash
cd ~/tidio-sync/src
podman build -t tidio-sync:latest .
```

> **ARM node?** The image uses the official `python:3.12-slim` base which is
> multi-arch, so no build flags are needed for ARM.

---

## 5 · Run the container

```bash
podman run -d \
  --name tidio-sync \
  --restart=unless-stopped \
  --env-file ~/tidio-sync/.env \
  -v ~/tidio-sync/logs:/app/logs:Z \
  tidio-sync:latest
```

Flag notes:

| Flag | Purpose |
|---|---|
| `--restart=unless-stopped` | Auto-restarts after VPS reboot or crash |
| `--env-file` | Injects secrets from the host file – never baked into the image |
| `-v … :Z` | Mounts the log directory; `:Z` sets the correct SELinux label |

Check it started cleanly:

```bash
podman ps
podman logs -f tidio-sync
```

---

## 6 · Set up the Zoho Flow webhook & Cliq notification

1. In **Zoho Flow**, create a new flow with trigger type **Webhook**.
2. Copy the generated webhook URL and paste it into `~/tidio-sync/.env`
   as `ZOHO_FLOW_WEBHOOK_URL`.
3. Add an action step: **Zoho Cliq → Send Message** (or **Post to Channel**).
4. Map the webhook payload fields to your message body. The script POSTs this
   JSON structure every time:

```json
{
  "status":          "success | failure | no_updates",
  "sync_type":       "incremental | full",
  "products_synced": 142,
  "failed_batches":  [],
  "resume_command":  null,
  "timestamp":       "2026-02-23T02:00:01+00:00"
}
```

Suggested Cliq message templates (use Flow's merge fields):

**Success:**
```
✅ Tidio ${sync_type} sync complete – ${products_synced} products synced at ${timestamp}
```

**Failure:**
```
❌ Tidio ${sync_type} sync FAILED at ${timestamp}
Failed batches: ${failed_batches}
To resume: ${resume_command}
```

**No updates (incremental only, if NOTIFY_ON_EMPTY=true):**
```
ℹ️ Tidio incremental sync – no product updates found at ${timestamp}
```

After saving the Flow, restart the container so it picks up the new env var:

```bash
podman restart tidio-sync
```

---

## 7 · Auto-start on VPS reboot (systemd user unit)

Podman's `--restart=unless-stopped` handles crashes, but a systemd unit ensures
the container starts after a clean reboot:

```bash
# Generate the unit file automatically
podman generate systemd --name tidio-sync --files --new
sudo mv container-tidio-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now container-tidio-sync
```

Verify:

```bash
sudo systemctl status container-tidio-sync
```

---

## 8 · Updating the application

```bash
cd ~/tidio-sync/src
git pull
podman build -t tidio-sync:latest .
podman stop tidio-sync
podman rm tidio-sync
podman run -d \
  --name tidio-sync \
  --restart=unless-stopped \
  --env-file ~/tidio-sync/.env \
  -v ~/tidio-sync/logs:/app/logs:Z \
  tidio-sync:latest
```

---

## 9 · Running a manual resume after a failed sync

If a Cliq notification tells you to resume using a WorkDrive manifest file ID:

```bash
podman exec tidio-sync python /app/app.py --resume <WORKDRIVE_FILE_ID>
```

To trigger a one-off full sync immediately (e.g. after a data correction):

```bash
podman exec tidio-sync python /app/app.py --full
```

---

## 10 · Viewing logs

Live container output (supercronic + Python logging):

```bash
podman logs -f tidio-sync
```

Persistent log file (mounted from host):

```bash
tail -f ~/tidio-sync/logs/tidio_products.log
```
