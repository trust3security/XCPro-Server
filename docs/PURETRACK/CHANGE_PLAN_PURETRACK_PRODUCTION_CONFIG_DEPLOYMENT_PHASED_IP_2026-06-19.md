# PureTrack Production Config Deployment Phased IP

Status: planned / current
Date: 2026-06-19
Repo: `C:\Users\Asus\AndroidStudioProjects\XCPro_Server`
Branch observed: `main`

## Purpose

Fix the live PureTrack connection failure where XCPro Android can reach the
XCPro backend, but PureTrack settings report "PureTrack backend unavailable".

This IP is intentionally server/deployment-first. It does not change Android
PureTrack settings, queue publishing, foreground runtime, traffic overlays, or
PureTrack client logic unless a later verified phase explicitly requires a
separate Android IP.

## Problem Statement

Live-device testing showed that Android reaches production XCPro_Server routes:

- `GET /api/v1/puretrack/status` reached production and returned `200`.
- `POST /api/v1/puretrack/connect` reached production and returned `200`.
- Invalid bearer-token probes returned `401 unauthenticated`, proving the
  route is live and auth is enforced.

The failure is server configuration, not Android route discovery:

- Production API container had no `XCPRO_PURETRACK_*` environment variables.
- Production `/opt/xcpro/.env` only had `XCPRO_PURETRACK_API_BASE_URL` and
  `XCPRO_PURETRACK_TIMEOUT_SECONDS` when checked.
- Production `/opt/xcpro/docker-compose.yml` did not pass any
  `XCPRO_PURETRACK_*` values into the `api` service.
- Local `docker-compose.yml` has the same missing PureTrack environment
  projection.
- Local `.env.example` has no PureTrack environment placeholders.

The expected user-facing symptom is that Android maps the server's
unconfigured PureTrack status/connect state to "PureTrack backend unavailable".

## Official PureTrack References Checked

Official PureTrack docs checked on 2026-06-19:

- https://puretrack.io/help/api
- https://puretrack.io/help/api-insert

Relevant contract facts:

- PureTrack Traffic API login and traffic calls require an application API key.
- Users must be PureTrack Pro subscribers to access PureTrack data through
  third-party applications.
- PureTrack login returns an API token and Pro status.
- PureTrack Insert API requires an issued insert key for the application.
- Insert `deviceID` must be displayed/known so users can link the app source to
  their PureTrack map marker.

XCPro production policy remains:

- Android must call XCPro_Server only.
- Android must not hold PureTrack app keys, insert keys, provider tokens,
  passwords after request completion, or direct PureTrack production URLs.

## Confirmed Boundaries / Verified Facts

- `XCPro_Server` owns PureTrack app key configuration, provider login/session
  handling, provider Traffic API calls, Insert API key use, upstream PureTrack
  calls, redaction, and route-level error projection.
- Docker Compose owns projecting `.env` values into the API container.
- `/opt/xcpro/.env` owns real production secret values and is outside Git.
- `docs/PURETRACK/PURETRACK_BACKEND_PROXY_CONTRACT.md` owns the route and
  environment contract.
- `DEPLOY.md` owns the real production deployment model. It states production
  is not automatically deployed from GitHub and must be updated explicitly on
  the server under `/opt/xcpro`.
- Android PureTrack already calls `https://api.xcpro.com.au/` for the backend
  adapter path. This IP does not move Android to a local server or direct
  PureTrack endpoint.

## Explicit Decisions / Defaults Chosen

- Use "PureTrack Pro" in user-facing/provider terminology, while server models
  may retain `PREMIUM` internally to avoid confusion with XCPro
  `PlanTier.PRO`.
- Treat live production parity as separate from local server commits. A local
  code commit is not enough until `/opt/xcpro` is updated and the API container
  is recreated.
- Do not log, commit, paste, or echo real PureTrack keys or provider session
  material.
- Use presence-only checks for secrets: output `SET` / `MISSING`, never values.
- Do not change XCPro login, Firebase Auth, Google login, subscriptions,
  LiveFollow, FCM, or entitlement authority in this IP.

## Unresolved Decisions

- Real production `XCPRO_PURETRACK_APP_KEY` value must be supplied out of band
  before live PureTrack Traffic login/connect can succeed.
- Real production `XCPRO_PURETRACK_INSERT_KEY` value is required before live
  outbound Insert publishing can be enabled. It is not required to fix the
  PureTrack login/connect "backend unavailable" symptom.
- Production provider-session encryption secret value must be generated or
  supplied on the server, outside Git.
- Whether the production server should be updated manually from the local
  working tree or through a pushed repository revision remains a deployment
  operation decision.

## Out Of Scope

- Android UI changes.
- Android backend adapter changes.
- PureTrack outbound queue or foreground runtime changes.
- PureTrack traffic overlay implementation changes.
- Direct Android calls to `puretrack.io`.
- XCPro login/Firebase/Google auth changes.
- LiveFollow changes.
- Subscription entitlement policy changes.
- Storing or displaying real secrets in Git, docs, logs, screenshots, or chat.

## Phase Overview

| Phase | Title | Status |
| --- | --- | --- |
| P0A | Evidence Freeze And Config Gap Record | complete / current |
| P0B | Compose And Example Env Wiring | complete / current |
| P0C | Production Secret Installation And API Recreate | planned / next executable |
| P0D | Live PureTrack Login Smoke And Evidence Closeout | planned after P0C |
| P0E | Contingency Sanitized Diagnostics If Smoke Still Fails | blocked unless P0D fails |

## P0A - Evidence Freeze And Config Gap Record

Status: complete / current.

### Objective

Record the verified production failure mode and split the repair so the first
implementation phase is server config wiring, not Android behavior changes.

### Evidence

- Live Android reached production PureTrack routes.
- Invalid XCPro bearer-token probe returned the expected `401` envelope.
- Production API container lacked `XCPRO_PURETRACK_*` runtime environment.
- Production compose did not project PureTrack env keys into the `api` service.
- Local compose and `.env.example` have the same PureTrack env wiring gap.
- Official PureTrack docs confirm separate app-key and insert-key requirements.
- Legacy markdown in `DEPLOY.md`, `serverinfo.md`, and
  `docs/PURETRACK/PURETRACK_BACKEND_PROXY_CONTRACT.md` was updated to name the
  production config/deployment gap and remove stale wording that implied local
  route implementation alone was enough for live provider readiness.
- `DEPLOY.md` now contains the canonical "Live deployment contract" that every
  server-backed feature must satisfy before live readiness is claimed.

### Verification

Run:

```powershell
git -C C:\Users\Asus\AndroidStudioProjects\XCPro_Server diff --check -- docs/PURETRACK/CHANGE_PLAN_PURETRACK_PRODUCTION_CONFIG_DEPLOYMENT_PHASED_IP_2026-06-19.md
```

### Post-Review Verdict

Docs-only evidence record. No server code, production config, Android code, or
secrets changed.

Next phase: P0B.

## P0B - Compose And Example Env Wiring

Status: complete / current.

### Objective

Make the local server repository capable of projecting PureTrack runtime
configuration into the API container without storing real secrets.

### Allowed Scope

- `docker-compose.yml`
- `.env.example`
- `DEPLOY.md`, only if the live deployment contract or PureTrack production
  config instructions are stale after the compose/env edits.
- `docs/PURETRACK/PURETRACK_BACKEND_PROXY_CONTRACT.md`, only if wording needs
  deployment parity clarification.
- This IP file, for P0B evidence and P0C handoff.

### Required Changes

- Add API service environment projection for:
  - `XCPRO_PURETRACK_APP_KEY`
  - `XCPRO_PURETRACK_INSERT_KEY`
  - `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET`
  - `XCPRO_PURETRACK_API_BASE_URL`
  - `XCPRO_PURETRACK_TIMEOUT_SECONDS`
- Add placeholder-only entries to `.env.example`.
- Review `DEPLOY.md` for PureTrack production config instructions. Update it
  only if stale after the compose/env edits. If the live deployment contract
  already covers no real secrets in Git, real values in `/opt/xcpro/.env`,
  API-only recreate, and presence-only verification, do not rewrite it.
- Preserve existing Firebase Auth, Google login, LiveFollow, FCM, Google Play,
  and subscription environment wiring.

### Explicit Exclusions

- No route implementation changes.
- No database migration.
- No Android changes.
- No XCPro login/Firebase changes.
- No LiveFollow changes.
- No direct PureTrack credentials in committed files.
- No production `/opt/xcpro`, `.env`, container, volume, secret, or Caddy edits
  in this phase.
- SSH is allowed only for non-mutating compose validation in a temporary
  directory on a Docker-capable host when Docker/Compose is unavailable locally.

### Verification

Run from `C:\Users\Asus\AndroidStudioProjects\XCPro_Server`:

```powershell
git status --short --branch
docker compose --env-file .env.example config > $env:TEMP\xcpro-server-compose-config.yml
git diff --check -- docker-compose.yml .env.example DEPLOY.md docs/PURETRACK
rg -n "XCPRO_PURETRACK_(APP_KEY|INSERT_KEY|PROVIDER_SESSION_ENCRYPTION_SECRET)=\\S+" .env.example docs docker-compose.yml
rg -n "puretrack\\.io/api|api/login|api/traffic|api/insert" app -g "*.py"
```

Expected:

- `docker compose config` succeeds locally or on a Docker-capable host. If
  Docker is unavailable locally, record the host where this exact compose config
  was verified.
- If using a Docker-capable host, copy only the candidate `docker-compose.yml`
  and `.env.example` to a temporary directory outside `/opt/xcpro`, run the
  compose config command there, then delete the temporary directory.
- The first `rg` may show placeholder/example keys only; any real-looking
  secret value is a blocker.
- The second `rg` should show server-owned upstream PureTrack API use only.
  Android is not in this repo and must remain out of scope.

### Post-Review

- Seam/ownership pass: PASS. Scope stayed to compose/env docs only.
- Architecture review rationale: no route logic, auth, entitlement, provider
  session behavior, Android code, or upstream PureTrack provider code changed,
  so no code-level architecture review was required.
- Commit only scoped P0B files after verification passes.

### Evidence

Completed on 2026-06-19:

- `docker-compose.yml` now projects the PureTrack runtime env keys into the
  `api` service:
  - `XCPRO_PURETRACK_APP_KEY`
  - `XCPRO_PURETRACK_INSERT_KEY`
  - `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET`
  - `XCPRO_PURETRACK_API_BASE_URL`
  - `XCPRO_PURETRACK_TIMEOUT_SECONDS`
- `.env.example` now lists the same PureTrack keys with placeholder/default
  values only.
- Local Docker/Compose was unavailable, so compose verification was run on
  `xcpro-prod` in a temporary directory under `/tmp`. Only candidate
  `docker-compose.yml` and `.env.example` were copied. The command
  `docker compose --env-file .env.example config` returned
  `COMPOSE_CONFIG_OK`, and the temporary directory was deleted.
- No production `/opt/xcpro`, `.env`, container, volume, secret, or Caddy config
  was edited during P0B.
- `git diff --check -- docker-compose.yml .env.example DEPLOY.md docs/PURETRACK`
  passed with line-ending warnings only.
- Secret assignment scan passed with no committed real-value hits:
  `rg -n "XCPRO_PURETRACK_(APP_KEY|INSERT_KEY|PROVIDER_SESSION_ENCRYPTION_SECRET)=\\S+" .env.example docs docker-compose.yml`.
- PureTrack URL ownership scan stayed in server-owned provider code/tests only:
  `app/main.py` upstream calls and redaction tests in
  `app/tests/test_puretrack_backend_proxy.py`.

Next phase: P0C - Production Secret Installation And API Recreate. Do not start
P0C without explicit user direction and real secret handling confirmation.

## P0C - Production Secret Installation And API Recreate

Status: planned / next executable.

### Objective

Deploy the PureTrack runtime configuration to production without exposing
secrets, then recreate the API container so the running service receives the
values.

### Preconditions

- P0B is committed locally.
- A deployment decision exists for updating `/opt/xcpro/docker-compose.yml`
  from the repaired server repo.
- Real `XCPRO_PURETRACK_APP_KEY` is available out of band.
- Real `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET` is generated or
  supplied out of band.
- `XCPRO_PURETRACK_INSERT_KEY` is available if outbound Insert publishing is
  being validated in the same deployment window. It is not required for
  PureTrack login/connect.

### Allowed Scope

- Production `/opt/xcpro/docker-compose.yml`
- Production `/opt/xcpro/.env`
- Production API container recreation
- This IP file, for P0C evidence

### Required Operational Steps

- Back up production config before editing.
- Add missing PureTrack keys to `/opt/xcpro/.env`.
- Update production compose to project PureTrack keys into the `api` service.
- Validate compose config.
- Recreate only the API service:

```bash
cd /opt/xcpro
docker compose up -d --no-deps --force-recreate api
```

### Secret Handling Rules

- Do not print or paste real values into chat, commit logs, docs, shell output,
  screenshots, support notes, or test fixtures.
- Use commands that print only `SET` or `MISSING`.
- Do not reuse `XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET` as the PureTrack provider
  session encryption secret.

### Production Verification

Use presence-only checks, for example:

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

Expected for login/connect fix:

- `XCPRO_PURETRACK_APP_KEY` reports `SET`.
- `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET` reports `SET`.
- `XCPRO_PURETRACK_API_BASE_URL` reports `SET`.
- `XCPRO_PURETRACK_TIMEOUT_SECONDS` reports `SET`.
- `XCPRO_PURETRACK_INSERT_KEY` may report `MISSING` only if outbound Insert
  publishing is not being validated yet.

Also verify unauthenticated route behavior still fails closed:

```bash
curl -i https://api.xcpro.com.au/api/v1/puretrack/status
```

Expected:

- `401` unauthenticated without an XCPro bearer token.
- No raw secrets in logs.

## P0D - Live PureTrack Login Smoke And Evidence Closeout

Status: planned after P0C.

### Objective

Confirm the production PureTrack settings flow no longer reports backend
unavailability caused by missing server runtime config.

### Allowed Scope

- Manual Android smoke using the connected phone.
- Production sanitized logs.
- This IP file, for closeout evidence.

### Explicit Exclusions

- No Android code edits.
- No route implementation edits.
- No new logging of raw passwords, app keys, provider tokens, XCPro bearer
  tokens, provider session material, or direct provider responses.

### Verification

- Launch the installed Android debug app on the connected device.
- Confirm user is logged into XCPro through the normal XCPro account flow.
- Open Settings -> PureTrack.
- Enter PureTrack username/password on the phone.
- Observe the PureTrack settings state.
- Watch production logs with sanitized output only:

```bash
ssh xcpro-prod "cd /opt/xcpro && docker compose logs --no-color --since=15m api | grep -E 'puretrack/(status|connect|disconnect|insert|traffic)|puretrack_' | tail -n 120"
```

Expected outcomes:

- Valid PureTrack Pro credentials: connected/provider-capable state appears and
  "PureTrack backend unavailable" is gone.
- Valid non-Pro credentials: backend is available, but traffic/provider access
  is not allowed because PureTrack Pro is required.
- Invalid PureTrack credentials: backend is available, and the UI reports a
  credential/provider rejection rather than backend unavailable.

If all valid cases still report backend unavailable, do not guess. Stop and
move to P0E.

## P0E - Contingency Sanitized Diagnostics If Smoke Still Fails

Status: blocked unless P0D fails after P0C verifies required env is `SET`.

### Objective

Add the smallest server-side sanitized diagnostic needed to distinguish provider
config, provider credential rejection, provider session encryption, timeout,
and upstream PureTrack response mapping failures.

### Required Split Trigger

P0E must be split before implementation if diagnosis requires:

- route behavior changes
- response DTO changes consumed by Android
- database schema changes
- provider session storage changes
- Android UI error mapping changes
- additional production observability infrastructure

### Allowed Scope Before Split

- Server-side planning and evidence only.
- No implementation until P0D evidence identifies the exact unknown.

## Release-Grade Gates

Before claiming the production PureTrack login/connect fix is release-ready:

- Server repo is committed with compose/env/docs wiring.
- Production `/opt/xcpro/docker-compose.yml` projects PureTrack env keys.
- Production `/opt/xcpro/.env` contains required PureTrack values outside Git.
- Running API container reports required values as `SET` using presence-only
  checks.
- Unauthenticated status request still returns `401`.
- Android connected-device smoke proves the UI no longer reports backend
  unavailable for the missing-config path.
- Logs contain no raw app keys, insert keys, PureTrack passwords, provider
  tokens, provider sessions, or XCPro bearer tokens.

## P0B Paste-Ready Codex CLI Brief

Implement only P0B - Compose And Example Env Wiring from:

`C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docs\PURETRACK\CHANGE_PLAN_PURETRACK_PRODUCTION_CONFIG_DEPLOYMENT_PHASED_IP_2026-06-19.md`

Before editing:

1. Read the IP.
2. Read `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docs\PURETRACK\PURETRACK_BACKEND_PROXY_CONTRACT.md`.
3. Read `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\DEPLOY.md`.
4. Read `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docker-compose.yml`.
5. Read `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\.env.example`.
6. Run `git -C C:\Users\Asus\AndroidStudioProjects\XCPro_Server status --short --branch`.
7. Run a seam/ownership pass focused on PureTrack server config ownership and
   confirm this is compose/env/docs only.

Allowed scope:

- `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docker-compose.yml`
- `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\.env.example`
- `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\DEPLOY.md`, only if the
  live deployment contract or PureTrack production config instructions are
  stale after the compose/env edits
- `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docs\PURETRACK\PURETRACK_BACKEND_PROXY_CONTRACT.md`, only for deployment parity wording if needed
- `C:\Users\Asus\AndroidStudioProjects\XCPro_Server\docs\PURETRACK\CHANGE_PLAN_PURETRACK_PRODUCTION_CONFIG_DEPLOYMENT_PHASED_IP_2026-06-19.md`

Implement:

- Add PureTrack env projection to the `api` service in `docker-compose.yml`.
- Add placeholder-only PureTrack env keys to `.env.example`.
- Update deploy docs only if P0B finds the live deployment contract or
  PureTrack-specific config section stale after the compose/env edit.
- Update the IP with P0B evidence and P0C readiness.

Do not:

- Change route logic, auth, Firebase, Google login, LiveFollow, FCM,
  subscriptions, database schema, Android code, or PureTrack provider logic.
- Commit real secrets.
- Edit production `/opt/xcpro`, `.env`, containers, volumes, secrets, or Caddy
  config during P0B.
- Use SSH during P0B except for non-mutating temp-dir compose validation when
  Docker/Compose is unavailable locally.

Verification:

```powershell
cd C:\Users\Asus\AndroidStudioProjects\XCPro_Server
docker compose --env-file .env.example config > $env:TEMP\xcpro-server-compose-config.yml
git diff --check -- docker-compose.yml .env.example DEPLOY.md docs/PURETRACK
rg -n "XCPRO_PURETRACK_(APP_KEY|INSERT_KEY|PROVIDER_SESSION_ENCRYPTION_SECRET)=\\S+" .env.example docs docker-compose.yml
rg -n "puretrack\\.io/api|api/login|api/traffic|api/insert" app -g "*.py"
```

`docker compose config` must pass locally or on a Docker-capable host. If
Docker is unavailable locally, record where this exact compose config was
verified. Host-based validation must copy only `docker-compose.yml` and
`.env.example` to a temporary directory outside `/opt/xcpro`, run compose
config there, and delete the temporary directory without mutating production
config, containers, volumes, secrets, or Caddy.

Post-review:

- Run a focused seam/ownership review.
- If no route code changed, record no-code architecture rationale.
- Commit only scoped P0B files.
- Do not push unless explicitly requested.

Next phase after P0B:

- P0C - Production Secret Installation And API Recreate.
- Do not start P0C without explicit user direction and real secret handling
  confirmation.
