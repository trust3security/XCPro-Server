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
XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET=generated-secret
XCPRO_FCM_PROJECT_ID=your-firebase-project-id
XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH=/run/secrets/fcm-service-account.json
```

Do not commit the real production values.

## Private-follow notification delivery

Private-follow push notifications are delivered by `XCPro_Server` through
Firebase Cloud Messaging. Firebase is only the push transport:
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

### If app code changed

Because the API is built from `/opt/xcpro/app`, production must have updated server-side files before rebuilding.

Typical pattern:
1. copy updated app files onto the server under `/opt/xcpro/app`
2. rebuild/recreate the API container

Example:

```bash
cd /opt/xcpro
docker compose up -d --build api
```

If the host is still using old compose:

```bash
cd /opt/xcpro
docker-compose up -d --build api
```

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
curl -I http://127.0.0.1:8000
curl -I https://api.xcpro.com.au
```

Expected:
- Caddyfile points to `127.0.0.1:8000`
- API restart policy is `unless-stopped`
- `.env` exists with restricted permissions
- `xcpro-api`, `xcpro-db`, `xcpro-redis` are running
- local and public curl checks return an HTTP response

## Recommended next improvements

These are not fully implemented yet, but should happen next:
- create a real staging server
- standardize on Compose v2 / modern Docker packaging
- make GitHub the source of truth for deployment
- add an automated deploy process
- move toward a more mature secrets management approach
