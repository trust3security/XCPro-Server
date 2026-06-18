# PureTrack Backend Proxy Contract

Status: reviewed contract; status/connect/disconnect and Insert publishing
implemented locally.
Date: 2026-06-18

## Purpose

This document defines the XCPro backend routes that Android may call for
PureTrack account/provider state. It exists to unblock Android-side contract
and DTO work without putting PureTrack credentials, app keys, Bearer tokens, or
raw Traffic API rows in the APK.

Local commit `4b7064a Add PureTrack backend proxy endpoints` implements and
tests the Android-facing status/connect/disconnect routes described here.
Production deployment/live-server parity, traffic proxy routes, Android queue
drain wiring, and foreground publishing runtime rollout remain separate future
work. The Android-facing Insert route contract below is implemented locally for
Android publishing code to consume through the XCPro backend only.

Verified current anchors:

- `app/main.py` contains the shared `ApiHTTPException` error envelope.
- `app/main.py` uses XCPro bearer `Authorization` headers through
  `resolve_bearer_identity(...)`.
- `app/main.py` validates Android package context with
  `X-XCPro-Package-Name` on entitlement/auth flows.
- `app/main.py` exposes `/api/v1/puretrack/status`,
  `/api/v1/puretrack/connect`, and `/api/v1/puretrack/disconnect`.
- `app/main.py` exposes `POST /api/v1/puretrack/insert`; tests verify bearer
  and package validation, verified XCPro PRO access, server-only Insert key
  configuration, ack semantics, retry delay propagation, and redaction.
- `app/main.py` still returns coarse `providerStates.pureTrack` values inside
  entitlement response builders; dedicated PureTrack settings state comes from
  `/api/v1/puretrack/status`.
- Android outbound publishing will call only the XCPro backend
  `POST /api/v1/puretrack/insert` route. The backend injects the server-only
  PureTrack Insert key into the upstream PureTrack request.

## Ownership

XCPro backend owns:

- PureTrack app key configuration.
- PureTrack login/token exchange.
- PureTrack provider token storage if implementation persists provider access.
- PureTrack Traffic API calls, bbox validation, filtering, rate limiting,
  compact-row parsing, and redaction in later traffic phases.
- Redacted Android-visible PureTrack status projection.
- Server-only PureTrack Insert key configuration and upstream Insert calls.

Android owns only:

- XCPro account Bearer token presentation to XCPro backend.
- User-entered PureTrack email/password submission for a connect command.
- Rendering redacted PureTrack status and errors returned by XCPro backend.
- Durable outbound publish queue records, retry state, and queue deletion
  decisions based on server `acceptedClientPointIds`.

Android must never store or receive:

- PureTrack app key.
- PureTrack Insert key.
- PureTrack access token, refresh token, Bearer token, cookie, or session key.
- PureTrack password after the connect request completes.
- Raw compact Traffic API rows.
- Direct PureTrack API URLs for production calls.

## Environment Contract

Server implementation must use these environment keys:

```dotenv
XCPRO_PURETRACK_APP_KEY=
XCPRO_PURETRACK_INSERT_KEY=
XCPRO_PURETRACK_API_BASE_URL=https://puretrack.io
XCPRO_PURETRACK_TIMEOUT_SECONDS=10
```

Rules:

- `XCPRO_PURETRACK_APP_KEY`, XCPro `PlanTier.PRO` entitlement, and PureTrack
  Pro provider access are required before `trafficApiAllowed` can be true.
  Android/server domain models represent the PureTrack provider-capable state as
  `PREMIUM` to avoid conflating it with XCPro `PlanTier.PRO`.
- `XCPRO_PURETRACK_INSERT_KEY` is required before `insertApiConfigured` can be
  true and before `POST /api/v1/puretrack/insert` can publish upstream.
- Outbound Insert publishing requires verified XCPro `PlanTier.PRO` entitlement
  plus server Insert-key configuration. A PureTrack Pro provider account is not
  required for outbound publishing unless a later product decision updates this
  contract and Android billing policy together.
- `XCPRO_PURETRACK_APP_KEY` and `XCPRO_PURETRACK_INSERT_KEY` must not be
  emitted in responses, logs, audit detail, support snapshots, test failure
  messages, or Android fixtures.
- `XCPRO_PURETRACK_API_BASE_URL` is server-only configuration.
- `XCPRO_PURETRACK_TIMEOUT_SECONDS` controls backend-to-PureTrack requests.

## Common Request Headers

All routes in this contract require:

```http
Authorization: Bearer <xcpro-user-token>
X-XCPro-Package-Name: com.trust3.xcpro
```

Rules:

- Debug package acceptance follows the existing server package policy used by
  entitlement endpoints.
- Missing or invalid XCPro bearer token returns the existing
  `unauthenticated` error envelope.
- Invalid package context returns the existing `invalid_package` error envelope.
- The `Authorization` value must never be logged raw.

## Route Summary

```text
GET  /api/v1/puretrack/status
POST /api/v1/puretrack/connect
POST /api/v1/puretrack/disconnect
POST /api/v1/puretrack/insert
```

No Android production route may call PureTrack directly. These routes are
Android-to-XCPro-backend only.

## Shared Response Model

`PureTrackStatusResponse`:

```json
{
  "connected": false,
  "appKeyConfigured": false,
  "trafficApiAllowed": false,
  "insertApiConfigured": false,
  "userAccess": "UNKNOWN",
  "verifiedAtMs": null,
  "validUntilMs": null,
  "accountLabel": null,
  "errorCode": null,
  "retryAfterMs": null,
  "auditId": null
}
```

Fields:

- `connected`: true only when backend has a valid server-side PureTrack
  provider session for this XCPro account.
- `appKeyConfigured`: true only when server has `XCPRO_PURETRACK_APP_KEY`.
- `trafficApiAllowed`: server-computed combined allowance. It is true only
  when server app key, XCPro `PlanTier.PRO` entitlement, and PureTrack provider
  state allow production inbound PureTrack traffic proxy calls. Android must
  consume this field and must not infer PureTrack provider entitlement locally.
- `insertApiConfigured`: true only when server-side Insert publishing
  configuration is present through `XCPRO_PURETRACK_INSERT_KEY`.
- `userAccess`: PureTrack provider access only; one of `UNKNOWN`, `NONE`,
  `FREE`, `PREMIUM`, `ERROR`. `PREMIUM` represents a PureTrack Pro-capable
  provider account and is not the XCPro `PlanTier.PRO` entitlement value.
- `verifiedAtMs`: server wall-clock epoch milliseconds for last provider state
  verification, or null.
- `validUntilMs`: server wall-clock epoch milliseconds for known provider
  validity/cache expiry, or null.
- `accountLabel`: redacted display label only, for example `p***@example.com`;
  never a raw credential or token.
- `errorCode`: nullable PureTrack provider/status code from the table below.
- `retryAfterMs`: nullable retry delay for retryable provider/backend errors.
- `auditId`: nullable redacted backend audit id for support correlation.

## Status Route

```http
GET /api/v1/puretrack/status
```

Request body: none.

Success response: `200 PureTrackStatusResponse`.

Behavior:

- Returns the current server-side redacted PureTrack status projection.
- Must not perform password login.
- May refresh provider state if a valid server-side provider session exists.
- Must not return provider tokens, raw provider responses, raw Traffic API rows,
  or direct PureTrack endpoint URLs.

## Connect Route

```http
POST /api/v1/puretrack/connect
Content-Type: application/json
```

`PureTrackConnectRequest`:

```json
{
  "email": "pilot@example.com",
  "password": "provider-password"
}
```

Validation:

- `email`: required non-blank string, maximum 320 characters.
- `password`: required non-empty string, maximum 1024 characters.
- Request models must reject unknown fields.

Success response: `200 PureTrackConnectResponse`.

`PureTrackConnectResponse`:

```json
{
  "result": "CONNECTED",
  "status": {
    "connected": true,
    "appKeyConfigured": true,
    "trafficApiAllowed": true,
    "insertApiConfigured": false,
    "userAccess": "PREMIUM",
    "verifiedAtMs": 1760000000000,
    "validUntilMs": 1760003600000,
    "accountLabel": "p***@example.com",
    "errorCode": null,
    "retryAfterMs": null,
    "auditId": "redacted-audit-id"
  }
}
```

`result` values:

- `CONNECTED`
- `AUTH_REJECTED`
- `APP_KEY_UNCONFIGURED`
- `PROVIDER_UNAVAILABLE`
- `RATE_LIMITED`
- `ERROR`

Behavior:

- The backend exchanges the submitted credential with PureTrack server-side.
- Password must not be persisted.
- Password must be redacted from logs, audit detail, support snapshots, test
  failure output, and exception detail.
- Provider token/session material, if any, is server-side only.
- Provider authentication rejection is represented by `result=AUTH_REJECTED`
  and a redacted `status.errorCode`, not by returning the PureTrack response.

## Disconnect Route

```http
POST /api/v1/puretrack/disconnect
Content-Type: application/json
```

Request body:

```json
{}
```

Request models must reject unknown fields.

Success response: `200 PureTrackDisconnectResponse`.

`PureTrackDisconnectResponse`:

```json
{
  "result": "DISCONNECTED",
  "status": {
    "connected": false,
    "appKeyConfigured": true,
    "trafficApiAllowed": false,
    "insertApiConfigured": false,
    "userAccess": "NONE",
    "verifiedAtMs": 1760000000000,
    "validUntilMs": null,
    "accountLabel": null,
    "errorCode": null,
    "retryAfterMs": null,
    "auditId": "redacted-audit-id"
  }
}
```

`result` values:

- `DISCONNECTED`
- `NOT_CONNECTED`
- `ERROR`

Behavior:

- Clears server-side PureTrack provider token/session material for the XCPro
  account.
- Does not alter XCPro account identity, Google Play entitlement, LiveFollow
  relationships, or Android local profile data.
- Does not call PureTrack Insert publishing or traffic overlay routes.

## Insert Publish Route

```http
POST /api/v1/puretrack/insert
Content-Type: application/json
```

Request body: `PureTrackInsertPublishRequest`.

```json
{
  "clientBatchId": "batch-20260618-0001",
  "trackers": [
    {
      "deviceID": "d7ry390",
      "type": 1,
      "rego": "ZK-ABC",
      "label": "XCPro",
      "points": [
        {
          "clientPointId": "queue-row-1",
          "ts": 1713563621,
          "lat": -41.2334745,
          "lng": 174.348365,
          "alt": 345.1,
          "speed": 25.0,
          "course": 270.0,
          "vspeed": 5.3
        }
      ]
    }
  ]
}
```

`PureTrackInsertPublishRequest` validation:

- `clientBatchId`: required non-blank string, maximum 128 characters. It is a
  client queue/drain correlation id only and is not a secret.
- `trackers`: required non-empty array, maximum 16 tracker objects.
- Each tracker object:
  - `deviceID`: required non-blank string, maximum 64 characters. This is the
    Android-selected aircraft PureTrack Device ID; the server must not generate
    a fallback Device ID.
  - `type`: optional integer PureTrack type id. Android P1A freezes the
    supported mappings as sailplane/glider `1`, paraglider `7`, and hang glider
    `6`, matching PureTrack's published type list at contract freeze time.
  - `rego`: optional non-blank string, maximum 32 characters. Use only when
    Android identity policy has an aircraft registration value.
  - `label`: optional non-blank string, maximum 64 characters. Use only a
    display label; do not include credentials or raw account emails.
  - `points`: required non-empty array, maximum 256 point objects.
- Each point object:
  - `clientPointId`: required non-blank string, maximum 128 characters. Android
    queue deletion is keyed only by ids returned in `acceptedClientPointIds`.
  - `ts`: required Unix epoch seconds from the GPS/fix timestamp.
  - `lat`: required finite decimal latitude in `[-90, 90]`.
  - `lng`: required finite decimal longitude in `[-180, 180]`.
  - `alt`: optional finite altitude in meters.
  - `speed`: optional finite ground speed in meters per second.
  - `course`: optional finite course in degrees in `[0, 360]`.
  - `vspeed`: optional finite vertical speed in meters per second.
- Request models must reject unknown fields at every level.
- Android must not send the upstream PureTrack `key` field, PureTrack provider
  tokens, Bearer tokens, passwords, raw provider payloads, or direct PureTrack
  URLs.

The backend maps each tracker to the upstream PureTrack Insert shape by adding
the server-only `XCPRO_PURETRACK_INSERT_KEY` as `key` and forwarding only the
validated fields above. The backend may combine multiple Android trackers in a
single upstream array request. The backend does not persist queued publish
points; Android remains the durable queue owner.

Success response: `200 PureTrackInsertPublishResponse`.

```json
{
  "result": "ACCEPTED",
  "acceptedClientPointIds": ["queue-row-1"],
  "serverReceivedPointCount": 1,
  "providerInsertedPointCount": 1,
  "retryAfterMs": null,
  "auditId": "redacted-audit-id"
}
```

`result` values:

- `ACCEPTED`: upstream PureTrack reported all received points inserted. The
  response returns every accepted client point id in `acceptedClientPointIds`.
- `PARTIAL_RETRY`: upstream PureTrack reported fewer inserted points than
  received, or returned an ambiguous partial result. Because upstream PureTrack
  returns aggregate counts rather than per-point ids, the server returns an
  empty `acceptedClientPointIds`; Android must retain the batch for retry.
- `RETRYABLE_FAILURE`: upstream PureTrack timed out, returned 429/5xx, or the
  backend hit a retryable provider/network failure. The response returns an
  empty `acceptedClientPointIds`; Android must retain the batch and respect
  `retryAfterMs` when present.
- `REJECTED`: the backend or upstream response determined the whole batch is
  non-retryable after validation. The response returns an empty
  `acceptedClientPointIds`; Android must not delete queued records unless a
  later queue-drain phase explicitly defines a non-retryable drop policy.

Queue-drain semantics:

- Android deletes or marks sent only queue records whose `clientPointId` appears
  in `acceptedClientPointIds`.
- `acceptedClientPointIds` is all-or-none for the initial server
  implementation: full upstream success returns all supplied point ids; partial
  or failed upstream outcomes return none.
- `serverReceivedPointCount` is the total number of validated request points.
- `providerInsertedPointCount` is the upstream aggregate inserted count when
  available; otherwise it is null.
- `auditId` is a redacted backend support correlation id. It must not encode
  the bearer token, account email, Device ID, location, Insert key, or provider
  payload.
- `retryAfterMs` is nullable and non-negative. If the route returns HTTP 429 or
  another retryable HTTP error with a reliable retry delay, the HTTP
  `Retry-After` header and JSON `retryAfterMs` must agree.

Insert route behavior:

- Requires the common XCPro Bearer token and package header.
- Requires verified XCPro `PlanTier.PRO` entitlement for the authenticated
  account.
- Requires `XCPRO_PURETRACK_INSERT_KEY`; when absent, returns
  `puretrack_insert_key_unconfigured`.
- Does not require a connected PureTrack provider session or PureTrack Pro
  provider account.
- Does not mutate PureTrack connect/disconnect provider session state.
- Does not read or write Android queue state, LiveFollow state, map/traffic
  state, or profile identity state.

## Error Envelope

All HTTP errors use the existing server envelope:

```json
{
  "code": "error_code",
  "detail": "redacted human-readable detail"
}
```

Required error codes:

- `unauthenticated`: missing or invalid XCPro bearer token.
- `invalid_package`: missing or invalid Android package context.
- `validation_error`: malformed JSON or field validation failure.
- `feature_access_denied`: authenticated account lacks verified XCPro PRO
  access for outbound Insert publishing.
- `puretrack_app_key_unconfigured`: server lacks
  `XCPRO_PURETRACK_APP_KEY`.
- `puretrack_insert_key_unconfigured`: server lacks
  `XCPRO_PURETRACK_INSERT_KEY`.
- `puretrack_insert_rejected`: request was valid at the XCPro boundary but the
  upstream provider rejected the whole Insert batch as non-retryable.
- `puretrack_provider_unavailable`: PureTrack request failed, timed out, or
  returned retryable 5xx/429.
- `puretrack_rate_limited`: backend or PureTrack rate limit applies.
- `puretrack_state_invalid`: persisted server-side provider state is invalid.

Provider credential rejection must normally be a `200` connect response with
`result=AUTH_REJECTED`, not an XCPro `unauthenticated` response, because the
XCPro user session is still valid.

Retryable HTTP errors must include `Retry-After` when a reliable retry delay is
known. The JSON body should also include `retryAfterMs` when returning a normal
PureTrack command/status response.

## Provider State In Entitlement Readback

The existing entitlement readback may continue to include coarse PureTrack
provider state:

```json
"providerStates": {
  "pureTrack": {
    "appKeyConfigured": false,
    "trafficApiAllowed": false,
    "insertApiConfigured": false,
    "userAccess": "UNKNOWN",
    "verifiedAtMs": null,
    "validUntilMs": null,
    "errorCode": null
  }
}
```

Decision:

- Entitlement readback remains the coarse paid-access/config gate.
- `/api/v1/puretrack/status` is the authoritative Android-visible connection
  status route for settings UI.
- Connect/disconnect must update the server-side source used by status.
- Android P3A2 is required only if the entitlement provider-state fields above
  change or if entitlement readback starts carrying additional PureTrack fields.

No entitlement response may include PureTrack tokens, app key, password,
account raw email, or raw provider payloads.

## Traffic API And Insert API Separation

Future inbound PureTrack overlay/aircraft traffic must use backend/proxy
routes and must return normalized XCPro DTOs. Android must not parse or store
raw compact PureTrack Traffic API rows.

Outbound Insert publishing is a separate flow from status/connect/disconnect
and traffic proxy routes. It must consume the selected aircraft PureTrack Device
ID from Android-side identity policy and use only
`POST /api/v1/puretrack/insert`; it must not be implemented through
status/connect/disconnect routes.

## Redaction Rules

Always redact these values before logs, audit detail, support snapshots, UI
state, test fixtures, and exception detail:

- Android `Authorization` header.
- `XCPRO_PURETRACK_APP_KEY`.
- `XCPRO_PURETRACK_INSERT_KEY`.
- PureTrack password.
- PureTrack access token, refresh token, Bearer token, cookie, or session key.
- Raw PureTrack provider responses.
- Raw PureTrack Insert provider responses.
- Raw location point payloads and queued publish batches in support snapshots.
- Raw compact Traffic API rows.
- Direct PureTrack request URLs when query parameters could contain sensitive
  account, location, bbox, or filtering context.
- Raw account email outside the inbound connect request.

Allowed Android-visible account display is `accountLabel`, which must be
redacted.

Tests must assert that rejected credentials, provider failures, and support
snapshots do not contain raw password, app key, provider token, raw email, or
raw PureTrack payloads.

## Implementation Split After This Contract

Recommended server phases:

1. Complete/current locally: add config/error/model skeleton and tests without
   external PureTrack calls.
2. Complete/current locally in commit `4b7064a`: add status/connect/disconnect
   route implementation with fake provider adapter tests.
3. Planned: add real PureTrack provider adapter behind server-only
   configuration.
4. Complete/current locally: implement `POST /api/v1/puretrack/insert` using
   the contract above, fake-provider tests, server-only Insert key
   configuration, and redaction/ack semantics.
5. Planned later: add traffic proxy contracts and production deployment parity
   as separate phases.

Android P3A1 is complete. Android P3B1 may implement the production HTTP
adapter against these XCPro backend status/connect/disconnect routes after the
Android PureTrack IP records the local server evidence. Live server deployment
parity remains a separate deployment/release concern.
