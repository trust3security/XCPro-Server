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

### If only docker-compose.yml changed

1. SSH to the server
2. Go to `/opt/xcpro`
3. Update `docker-compose.yml`
4. Apply the change

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

## Verification after deploy

Run these checks:

```bash
cat /etc/caddy/Caddyfile
cat /opt/xcpro/docker-compose.yml
docker inspect xcpro-api --format '{{.HostConfig.RestartPolicy.Name}}'
docker ps
curl -I http://127.0.0.1:8000
curl -I https://api.xcpro.com.au
```

Expected:
- Caddyfile points to `127.0.0.1:8000`
- API restart policy is `unless-stopped`
- `xcpro-api`, `xcpro-db`, `xcpro-redis` are running
- local and public curl checks return an HTTP response

## Recommended next improvements

These are not fully implemented yet, but should happen next:
- create a real staging server
- move secrets out of compose
- standardize on Compose v2 / modern Docker packaging
- make GitHub the source of truth for deployment
- add an automated deploy process
