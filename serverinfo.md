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

Current API runtime notes:
- API container restart policy is `unless-stopped`
- public HTTPS and local loopback checks were verified after the recent fixes
- Compose v2 is installed and available as `docker compose`

## Database

Postgres runs in Docker as `xcpro-db`.

Current runtime model:
- database name is provided through `/opt/xcpro/.env`
- database password is provided through `/opt/xcpro/.env`
- `DATABASE_URL` is provided through `/opt/xcpro/.env`

Important:
- the database password has been rotated away from the original default value
- the real production password must never be committed to Git
- the real production password should be stored in a password manager or other secure secret store

## Redis

Redis runs in Docker as `xcpro-redis`.

## Environment file

Production runtime secrets/config now live in:

```text
/opt/xcpro/.env
```

This file is not committed to Git.

The repo should contain only an example file such as:

```text
.env.example
```

## Compose tooling

The host originally used old Compose v1:

```text
docker-compose 1.29.2
```

Compose v2 was later installed manually and the host now supports:

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
- production secrets live outside Git on the server in `/opt/xcpro/.env`
- `.env.example` in this repo shows the required variable names only
- the real `.env` file must never be committed
