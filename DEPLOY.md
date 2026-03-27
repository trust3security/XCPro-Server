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
```

Do not commit the real production values.

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
