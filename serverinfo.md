# XCPro Server Info

## Current environment

This repo currently has one confirmed live environment:

- Production host: `api.xcpro.com.au`
- Public IP: `46.225.17.44`
- OS: Ubuntu 24.04.4 LTS
- Architecture: arm64 / aarch64

There is currently **no separate staging server documented or configured**.

## Reverse proxy

Caddy runs on the host machine.

Current Caddy config:

```caddy
api.xcpro.com.au {
    reverse_proxy 127.0.0.1:8000
}
```

Notes:
- Caddy terminates HTTPS on the host
- Caddy forwards traffic to the API container via `127.0.0.1:8000`
- This was changed from `localhost:8000` after earlier reverse-proxy issues

## App runtime

Docker and Compose are used on the host.

Compose project root:

```text
/opt/xcpro
```

App build context:

```text
/opt/xcpro/app
```

Current services:
- `xcpro-api`
- `xcpro-db`
- `xcpro-redis`

## API service

The API is built from the local server files under `/opt/xcpro/app`.

Important:
- The production server is **not currently deployed automatically from GitHub**
- Pushing to GitHub does **not** update production by itself
- Production changes currently require an explicit deploy/apply step on the server

The API service currently includes:

```yaml
restart: unless-stopped
```

## Database

Postgres runs in Docker as `xcpro-db`.

Current compose uses:
- database name: `xcpro`
- password: `postgres`

This works, but should be improved later by moving secrets to environment files or a secret manager.

## Redis

Redis runs in Docker as `xcpro-redis`.

## Compose tooling

The host originally used old Compose v1:

```text
docker-compose 1.29.2
```

Compose v2 was later installed manually so the host now also supports:

```text
docker compose
```

Current note:
- the manually installed Compose plugin does not auto-update
- later cleanup should standardize Docker packaging and Compose management

## Backups taken during investigation

Backups were created under:

```text
/root/backups/
```

These included:
- Caddyfile backup
- `/opt/xcpro` copy
- PostgreSQL dump via `pg_dumpall`

## Known current limitations

- no documented staging environment
- no automated GitHub deploy pipeline
- current deployment process is manual
- credentials are still hardcoded in compose and should be cleaned up later
