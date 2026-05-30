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

Private-follow notification delivery runs as a host-owned systemd timer that
invokes the server-owned command inside the API container:

```bash
docker compose exec -T api python /app/scripts/deliver_notifications.py --confirm-send --limit 50
```

Installed units:

```text
/etc/systemd/system/xcpro-notification-delivery.service
/etc/systemd/system/xcpro-notification-delivery.timer
```

The timer runs once per minute with `OnCalendar=*:0/1`. The service uses
`flock` on `/run/xcpro-notification-delivery.lock` so delivery runs do not
overlap. The FCM service account JSON stays outside Git under
`/opt/xcpro/secrets/` and is mounted read-only into the container.
The Firebase Auth service account JSON follows the same outside-Git model and
is mounted read-only into the API container when explicit Firebase Admin SDK
credentials are used.

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

Production env and secret inventory includes:
- `POSTGRES_DB`
- `POSTGRES_PASSWORD`
- `DATABASE_URL`
- `XCPRO_RUNTIME_ENV`
- `XCPRO_GOOGLE_SERVER_CLIENT_IDS` or `XCPRO_GOOGLE_SERVER_CLIENT_ID`
- `XCPRO_FIREBASE_AUTH_PROJECT_ID`
- `XCPRO_FIREBASE_AUTH_SERVICE_ACCOUNT_JSON_PATH`
- `XCPRO_PRIVATE_FOLLOW_BEARER_SECRET`
- `XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET`
- `XCPRO_GOOGLE_PLAY_PACKAGE_NAME`
- `XCPRO_GOOGLE_PLAY_SERVICE_ACCOUNT_JSON_PATH`
- `XCPRO_GOOGLE_PLAY_RTDN_OIDC_AUDIENCE`
- `XCPRO_GOOGLE_PLAY_RTDN_EXPECTED_SERVICE_ACCOUNT_EMAIL`
- `XCPRO_FCM_PROJECT_ID`
- `XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH`
- live-read rate-limit variables

The Firebase Auth service-account file path is currently:

```text
/opt/xcpro/secrets/firebase-auth-service-account.json
```

It is mounted into the API container at:

```text
/run/secrets/firebase-auth-service-account.json
```

Firebase Auth is identity only for XCPro account/session exchange. It is
separate from FCM and must not own entitlement authority, Google Play
verification, billing records, LiveFollow permission, or paid-access decisions.
The Firebase exchange endpoint is not enabled by this config checkpoint, and
the existing Google exchange endpoint remains in place.

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
- private-follow notification delivery is scheduled by a host systemd timer,
  not a dedicated Compose worker service
- production secrets live outside Git on the server in `/opt/xcpro/.env`
- `.env.example` in this repo shows the required variable names only
- the real `.env` file must never be committed
