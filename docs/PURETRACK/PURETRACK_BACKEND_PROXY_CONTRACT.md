# PureTrack Backend Proxy Contract

Status: reviewed contract for Android P3A0, implementation pending.
Date: 2026-06-18

## Purpose

This document defines the XCPro backend routes that Android may call for
PureTrack account/provider state. It exists to unblock Android-side contract
and DTO work without putting PureTrack credentials, app keys, Bearer tokens, or
raw Traffic API rows in the APK.

The current server implementation does not yet provide these routes. Current
code only exposes placeholder PureTrack provider state inside entitlement
readback.

Verified current anchors:

- `app/main.py` contains the shared `ApiHTTPException` error envelope.
- `app/main.py` uses XCPro bearer `Authorization` headers through
  `resolve_bearer_identity(...)`.
- `app/main.py` validates Android package context with
  `X-XCPro-Package-Name` on entitlement/auth flows.
- `app/main.py` currently returns `providerStates.pureTrack` placeholders from
  entitlement response builders.

## Ownership

XCPro backend owns:

- PureTrack app key configuration.
- PureTrack login/token exchange.
- PureTrack provider token storage if implementation persists provider access.
- PureTrack Traffic API calls, bbox validation, filtering, rate limiting,
  compact-row parsing, and redaction in later traffic phases.
- Redacted Android-visible PureTrack status projection.

Android owns only:

- XCPro account Bearer token presentation to XCPro backend.
- User-entered PureTrack email/password submission for a connect command.
- Rendering redacted PureTrack status and errors returned by XCPro backend.

Android must never store or receive:

- PureTrack app key.
- PureTrack access token, refresh token, Bearer token, cookie, or session key.
- PureTrack password after the connect request completes.
- Raw compact Traffic API rows.
- Direct PureTrack API URLs for production calls.

## Environment Contract

Server implementation must use these environment keys:

```dotenv
XCPRO_PURETRACK_APP_KEY=
XCPRO_PURETRACK_API_BASE_URL=https://puretrack.io
XCPRO_PURETRACK_TIMEOUT_SECONDS=10
```

Rules:

- `XCPRO_PURETRACK_APP_KEY`, XCPro `PlanTier.PRO` entitlement, and PureTrack
  Premium provider access are required before `trafficApiAllowed` can be true.
- `XCPRO_PURETRACK_APP_KEY` must not be emitted in responses, logs, audit
  detail, support snapshots, test failure messages, or Android fixtures.
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
  configuration is present. Insert publishing remains a separate future flow.
- `userAccess`: PureTrack provider access only; one of `UNKNOWN`, `NONE`,
  `FREE`, `PREMIUM`, `ERROR`. This is not the XCPro `PlanTier.PRO`
  entitlement value.
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
- `puretrack_app_key_unconfigured`: server lacks
  `XCPRO_PURETRACK_APP_KEY`.
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

This contract covers status/connect/disconnect only.

Future inbound PureTrack overlay/aircraft traffic must use backend/proxy
routes and must return normalized XCPro DTOs. Android must not parse or store
raw compact PureTrack Traffic API rows.

Future outbound Insert publishing is a separate flow. Insert publishing must
consume the selected aircraft PureTrack Device ID from Android-side identity
policy but must not be implemented through status/connect/disconnect routes.

## Redaction Rules

Always redact these values before logs, audit detail, support snapshots, UI
state, test fixtures, and exception detail:

- Android `Authorization` header.
- `XCPRO_PURETRACK_APP_KEY`.
- PureTrack password.
- PureTrack access token, refresh token, Bearer token, cookie, or session key.
- Raw PureTrack provider responses.
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

1. Add config/error/model skeleton and tests without external PureTrack calls.
2. Add status/connect/disconnect route implementation with fake provider
   adapter tests.
3. Add real PureTrack provider adapter behind server-only configuration.
4. Add later traffic proxy and Insert publishing contracts as separate phases.

Android may start P3A1 after this contract is synced into the Android PureTrack
IP. Android must still not implement a production HTTP adapter until the server
routes exist and are tested.
