# XCPro Server Deploy Guide

## Important

This document describes the **current real deployment model**.

At the moment, production is **not automatically deployed from GitHub**.

That means:
- editing and pushing code to GitHub does **not** change production by itself
- production must be updated explicitly on the server

## Production host

- Host: `api.xcpro.com.au`
- Server path: `/opt/xcpro`

## Current production structure

```text
/opt/xcpro
  docker-compose.yml
  .env
  /app
    Dockerfile
    main.py
    requirements.txt
    alembic.ini
    /alembic
```

## Services

Production currently runs these containers:
- `xcpro-api`
- `xcpro-db`
- `xcpro-redis`

Caddy runs on the host and proxies HTTPS traffic to:

```text
127.0.0.1:8000
```

## Environment file

Production keeps real runtime secrets in:

```text
/opt/xcpro/.env
```

This file is not committed to Git.

The repo should only contain:

```text
.env.example
```

Example format only:

```dotenv
POSTGRES_DB=xcpro
POSTGRES_PASSWORD=change-me
DATABASE_URL=postgresql://postgres:change-me@db:5432/xcpro
XCPRO_RUNTIME_ENV=prod
XCPRO_GOOGLE_SERVER_CLIENT_IDS=your-google-server-client-id
XCPRO_FIREBASE_AUTH_PROJECT_ID=your-firebase-auth-project-id
XCPRO_FIREBASE_AUTH_SERVICE_ACCOUNT_JSON_PATH=/run/secrets/firebase-auth-service-account.json
XCPRO_PRIVATE_FOLLOW_BEARER_SECRET=generated-secret
XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET=generated-secret
XCPRO_FCM_PROJECT_ID=your-firebase-project-id
XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH=/run/secrets/fcm-service-account.json
XCPRO_LIVE_READ_RATE_LIMIT_WINDOW_SECONDS=60
XCPRO_LIVE_READ_RATE_LIMIT_GLOBAL=0
XCPRO_LIVE_READ_RATE_LIMIT_PER_USER=0
XCPRO_LIVE_READ_RATE_LIMIT_PER_IP=0
XCPRO_LIVE_READ_RATE_LIMIT_PER_SESSION=0
XCPRO_PURETRACK_APP_KEY=your-puretrack-app-key
XCPRO_PURETRACK_INSERT_KEY=your-puretrack-insert-key-if-publishing
XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET=generated-secret
XCPRO_PURETRACK_API_BASE_URL=https://puretrack.io
XCPRO_PURETRACK_TIMEOUT_SECONDS=10
XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED=false
```

Do not commit the real production values.

The live-read rate-limit defaults above intentionally leave rate limiting
disabled (`0`) while metrics are observed. Set nonzero values only after a
traffic-based decision; Android already honors `429 Retry-After` responses.

## Live deployment contract

Every server-backed feature that depends on environment values, external
provider credentials, feature flags, service-account files, or deployment-time
configuration must satisfy this contract before Android or another client can
claim the live feature is ready.

Required repo-side evidence:

- `.env.example` lists every required variable by name with placeholder values
  only.
- `docker-compose.yml` projects every required variable into the service that
  reads it.
- This deploy guide documents the feature-specific live values, secret
  handling, safe verification command, and recreate/restart scope.
- Feature contract docs name any required provider credentials, server-only
  tokens, route availability, degraded-state behavior, and live parity
  requirement.
- Tests or local checks prove code behavior for missing/unconfigured values
  where the feature has server route logic.

Required production evidence:

- Real secret values exist only in `/opt/xcpro/.env`, host secret files, or a
  future approved secret store. They must not be committed to Git.
- Compose config validates successfully on the production host before applying
  changes.
- The running container receives the values after deploy. Checking
  `/opt/xcpro/.env` alone is not enough.
- Any changed environment projection is followed by the smallest safe service
  recreate, normally:

```bash
cd /opt/xcpro
docker compose config
docker compose up -d --no-deps --force-recreate api
```

- Verification commands print only presence or aggregate status, never raw
  secrets, tokens, passwords, service-account JSON, provider sessions, or
  bearer tokens.
- Public route smoke tests verify the expected auth boundary. For protected
  routes, unauthenticated requests should still fail closed.
- Feature-specific smoke tests prove the live user-visible state after
  deployment.
- Rollback is available through the pre-change backup and the previous
  Compose/env/app state.

For any new or changed server-backed feature, do not treat a local route
implementation, a passing unit test, or a pushed commit as live readiness until
the repo-side and production evidence above are complete.

## PureTrack provider and Insert configuration

PureTrack integration is server-owned in production. Android calls XCPro_Server;
it must not hold PureTrack app keys, Insert keys, provider tokens, or direct
PureTrack production URLs.

This section is the PureTrack-specific application of the live deployment
contract above.

Production PureTrack routes can exist and still report "PureTrack backend
unavailable" to Android if the API container does not receive the required
runtime environment. Both of these must be true before live PureTrack
connection testing is meaningful:

- `/opt/xcpro/.env` contains the required PureTrack values outside Git.
- `/opt/xcpro/docker-compose.yml` projects those values into the `api` service
  environment.

Required production variables:

```dotenv
XCPRO_PURETRACK_APP_KEY=your-puretrack-app-key
XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET=generated-secret
XCPRO_PURETRACK_API_BASE_URL=https://puretrack.io
XCPRO_PURETRACK_TIMEOUT_SECONDS=10
```

`XCPRO_PURETRACK_INSERT_KEY` is also required before outbound Insert publishing
can send points upstream. It is not required to fix the PureTrack
login/connect "backend unavailable" symptom.

Do not reuse `XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET` as the PureTrack provider
session encryption secret.

After changing PureTrack env or Compose projection, validate Compose and
recreate only the API container so the running process receives the new
environment:

```bash
cd /opt/xcpro
docker compose config
docker compose up -d --no-deps --force-recreate api
```

Verify secret presence without printing values:

```bash
docker compose exec -T api python - <<'PY'
import os
for name in [
    "XCPRO_PURETRACK_APP_KEY",
    "XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET",
    "XCPRO_PURETRACK_API_BASE_URL",
    "XCPRO_PURETRACK_TIMEOUT_SECONDS",
    "XCPRO_PURETRACK_INSERT_KEY",
]:
    print(f"{name}={'SET' if os.getenv(name) else 'MISSING'}")
PY
```

For PureTrack login/connect validation, the app key, provider-session
encryption secret, API base URL, and timeout must report `SET`. The Insert key
may remain `MISSING` only while outbound Insert publishing is not being
validated.

## Firebase Auth identity configuration

Firebase Auth is the V1 account identity provider for XCPro session exchange.
It is identity only. It must not own entitlement authority, Google Play
verification, billing records, LiveFollow permission, or paid-access decisions.
It is separate from FCM, which is only the private-follow push transport.

The Firebase Auth service account JSON should live outside Git on the
production host:

```text
/opt/xcpro/secrets/firebase-auth-service-account.json
```

The API container receives it read-only at:

```text
/run/secrets/firebase-auth-service-account.json
```

Production `.env` must provide:

```dotenv
XCPRO_FIREBASE_AUTH_PROJECT_ID=your-firebase-auth-project-id
XCPRO_FIREBASE_AUTH_SERVICE_ACCOUNT_JSON_PATH=/run/secrets/firebase-auth-service-account.json
```

`XCPRO_FIREBASE_AUTH_PROJECT_ID` is required by staging/prod preflight. The
service-account path is optional at runtime so deployments that provide
Firebase Admin SDK default credentials can omit it, but the Compose deployment
mounts the explicit JSON path above.

This config checkpoint does not add
`POST /api/v2/auth/firebase/exchange` yet, and it does not remove
`POST /api/v2/auth/google/exchange`.

## Private-follow notification delivery

Private-follow push notifications are delivered by `XCPro_Server` through
Firebase Cloud Messaging. For this notification path, Firebase is only the push
transport:
- do not add Firebase-hosted backend logic
- do not use paid Firebase services for this path
- do not log raw FCM device tokens
- do not send live-now notifications from this private-follow worker

The server-owned notification outbox currently emits only these private-follow
event types:
- `follow_request_received`
- `follow_request_accepted`
- `follow_new_follower`
- `follow_mutual`

### Required production secrets

The FCM service account JSON must live outside Git on the production host:

```text
/opt/xcpro/secrets/fcm-service-account.json
```

The API container receives it read-only at:

```text
/run/secrets/fcm-service-account.json
```

Production `.env` must provide:

```dotenv
XCPRO_FCM_PROJECT_ID=your-firebase-project-id
XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH=/run/secrets/fcm-service-account.json
XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET=generated-secret
```

Do not commit the service account JSON or paste its contents into docs, logs,
test fixtures, or shell history.

### Manual delivery command

After the API container is running, deliver queued private-follow notification
outbox events with:

```bash
cd /opt/xcpro
docker compose exec -T api python /app/scripts/deliver_notifications.py --confirm-send --limit 50
```

The `--confirm-send` flag is intentionally required so accidental command
execution does not call FCM. The command prints aggregate counts only; it must
not print raw FCM tokens.

### Production scheduling

Production scheduling is host-owned through systemd:

```text
/etc/systemd/system/xcpro-notification-delivery.service
/etc/systemd/system/xcpro-notification-delivery.timer
```

The timer runs once per minute:

```ini
OnCalendar=*:0/1
```

The service uses `flock` to prevent overlapping delivery runs:

```bash
cd /opt/xcpro
/usr/bin/flock -n /run/xcpro-notification-delivery.lock \
  /usr/bin/docker compose exec -T api \
  python /app/scripts/deliver_notifications.py --confirm-send --limit 50
```

Inspect scheduler status and recent aggregate delivery output with:

```bash
systemctl status xcpro-notification-delivery.timer --no-pager -l
systemctl status xcpro-notification-delivery.service --no-pager -l
journalctl -u xcpro-notification-delivery.service -n 50 --no-pager
```

This scheduler calls the server-owned outbox delivery command. It does not move
delivery into Android, request handlers, Firebase Functions, or any
Firebase-hosted backend logic. Journal output must remain aggregate-only and
must not include raw FCM device tokens, access tokens, or service account JSON.

## Before changing production

Always take backups first.

Example:

```bash
mkdir -p /root/backups/xcpro-$(date +%F-%H%M%S)
BACKUP_DIR=$(ls -dt /root/backups/xcpro-* | head -n 1)

cp /etc/caddy/Caddyfile "$BACKUP_DIR/Caddyfile"
cp -a /opt/xcpro "$BACKUP_DIR/opt_xcpro"
docker exec xcpro-db pg_dumpall -U postgres > "$BACKUP_DIR/pg_dumpall.sql"
```

## Production entitlement repair scripts

`app/scripts/seed_test_entitlement.py` is the only current manual
test/operator entitlement repair writer. In production, any committed seed or
clear operation must be an explicit audited support action:

```bash
docker compose exec -T api python /app/scripts/seed_test_entitlement.py \
  <one lookup argument> \
  --confirm-manual-test \
  --confirm-production-repair \
  --operator-id <operator-tag> \
  --support-ticket <ticket-id>
```

For clears, add `--clear`. For read-only rehearsal, add `--dry-run`; dry-runs
do not write entitlement rows or billing audit rows.

Rules:

- Do not paste raw account identifiers, emails, bearer tokens, purchase tokens,
  token hashes, provider credentials, or production exports into committed
  notes, logs, docs, screenshots, or support artifacts.
- Production committed mutations must emit a billing `auditId`; retain that
  sanitized id for support correlation.
- This script is not Google Play reconciliation. Do not use it to bypass raw
  Play-token evidence requirements or to weaken stale paid-continuity
  fail-closed behavior.

## Current manual deploy pattern

### If only docker-compose.yml or `.env` changed

1. SSH to the server
2. Go to `/opt/xcpro`
3. Ensure `/opt/xcpro/.env` exists with the real production values
4. Validate Compose config
5. Apply the change if needed

Examples:

#### Compose v2
```bash
cd /opt/xcpro
docker compose config
docker compose up -d
```

#### Old Compose v1
```bash
cd /opt/xcpro
docker-compose config
docker-compose up -d
```

Note:
- the first migration from Compose v1 to Compose v2 may recreate containers
- for PureTrack env-only or API-service env-projection changes, prefer
  `docker compose up -d --no-deps --force-recreate api` after
  `docker compose config`

### If app code changed

Because the API is built from `/opt/xcpro/app`, production must have updated server-side files before rebuilding.

Typical pattern:
1. copy updated app files onto the server under `/opt/xcpro/app`
2. validate Compose config
3. build the new API image without recreating the running API container
4. run database migrations from a one-off API container
5. run any required operational backfills from a one-off API container
6. recreate the API container after migrations and backfills pass

Example:

```bash
cd /opt/xcpro
docker compose config
docker compose build api
docker compose run --rm api python -m alembic -c /app/alembic.ini upgrade head
docker compose run --rm api python /app/scripts/recount_relationship_counters.py --confirm
docker compose up -d --no-deps --force-recreate api
```

If the host is still using old compose:

```bash
cd /opt/xcpro
docker-compose config
docker-compose build api
docker-compose run --rm api python -m alembic -c /app/alembic.ini upgrade head
docker-compose run --rm api python /app/scripts/recount_relationship_counters.py --confirm
docker-compose up -d --no-deps --force-recreate api
```

Do not use `docker compose up -d --build api` for schema-changing deploys. It
can start new API code against the old database schema before migrations have
run. Build first, migrate and backfill through one-off containers, then recreate
the API.

Run the relationship-counter recount after migrations whenever deploying the
cached follower/following counter table. The command is idempotent and prints
aggregate output only.

## Database password rotation

Do not rotate the password by editing `.env` alone.

For an existing Postgres volume, a real rotation should follow this pattern:

1. back up the database and current config
2. generate a new password
3. run `ALTER ROLE postgres WITH PASSWORD '...'` inside Postgres
4. verify the new password from a separate container/client
5. update `/opt/xcpro/.env`
6. validate Compose config
7. recreate only the API container so it picks up the new `DATABASE_URL`

Example commands:

```bash
docker exec xcpro-db psql -U postgres -d xcpro -c "ALTER ROLE postgres WITH PASSWORD 'NEW_PASSWORD';"

docker run --rm --network xcpro_default -e PGPASSWORD="NEW_PASSWORD" postgres:15 \
  psql -h xcpro-db -U postgres -d xcpro -c "SELECT 1;"

cd /opt/xcpro
docker compose up -d --no-deps --force-recreate api
```

## Verification after deploy

Run these checks:

```bash
cat /etc/caddy/Caddyfile
cat /opt/xcpro/docker-compose.yml
ls -la /opt/xcpro/.env
docker inspect xcpro-api --format '{{.HostConfig.RestartPolicy.Name}}'
docker ps
curl -i http://127.0.0.1:8000/
curl -i https://api.xcpro.com.au/
docker compose run --rm api python /app/scripts/private_follow_env_preflight.py
docker compose exec -T api python - <<'PY'
import os
for name in [
    "XCPRO_PURETRACK_APP_KEY",
    "XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET",
    "XCPRO_PURETRACK_API_BASE_URL",
    "XCPRO_PURETRACK_TIMEOUT_SECONDS",
    "XCPRO_PURETRACK_INSERT_KEY",
]:
    print(f"{name}={'SET' if os.getenv(name) else 'MISSING'}")
PY
systemctl status xcpro-notification-delivery.timer --no-pager -l
journalctl -u xcpro-notification-delivery.service -n 50 --no-pager
```

Expected:
- Caddyfile points to `127.0.0.1:8000`
- API restart policy is `unless-stopped`
- `.env` exists with restricted permissions
- `xcpro-api`, `xcpro-db`, `xcpro-redis` are running
- local and public curl checks return an HTTP response
- private-follow preflight reports `ok: true`
- staging/prod preflight includes Firebase Auth config and fails if
  `XCPRO_FIREBASE_AUTH_PROJECT_ID` is missing
- PureTrack app key, provider-session encryption secret, API base URL, and
  timeout report `SET` before live PureTrack login/connect validation
- PureTrack Insert key reports `SET` before outbound Insert publishing
  validation
- PureTrack traffic cadence evidence remains `false` except during an explicit,
  short, sanitized PureTrack evidence window
- notification timer status and journal output remain aggregate-only

Do not run `deliver_notifications.py --confirm-send` as a generic deploy smoke
check. Use it only when intentionally delivering queued notification outbox
events.

### PureTrack traffic cadence evidence window

`XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED` is disabled by default. Enable it
only for a short, explicit PureTrack inbound-overlay evidence window, then
disable it again after collecting route-level timing proof.

Allowed evidence fields are limited to the sanitized
`puretrack_traffic_cadence` event marker, route path, server timestamps, HTTP
status/outcome, cache status, retry-after milliseconds, redacted user hash, and
validated package name. Do not collect or commit raw provider rows, PureTrack
provider URLs, app keys, bearer tokens, provider session material, passwords,
request or response bodies, bbox coordinates, target labels, registrations,
callsigns, models, `tracker_uid` values, or exact private locations.

Safe verification flow:

1. Set `XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED=true` in `/opt/xcpro/.env`.
2. Recreate only the API container through the normal compose path.
3. Run the phone smoke and capture only sanitized
   `puretrack_traffic_cadence` lines.
4. Set `XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED=false` and recreate the API
   container again.
5. Scrub and review any evidence before copying it into repo docs.

## Recommended next improvements

These are not fully implemented yet, but should happen next:
- create a real staging server
- standardize on Compose v2 / modern Docker packaging
- make GitHub the source of truth for deployment
- add an automated deploy process
- move toward a more mature secrets management approach
