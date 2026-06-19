# PureTrack Backend Proxy Contract

Status: reviewed contract; status/connect/disconnect and Insert publishing
implemented locally; P0B2A provider-session material/status gating implemented
locally; P0B2B inbound traffic overlay route implemented locally.
Date: 2026-06-19

## Purpose

This document defines the XCPro backend routes that Android may call for
PureTrack account/provider state. It exists to unblock Android-side contract
and DTO work without putting PureTrack credentials, app keys, Bearer tokens, or
raw Traffic API rows in the APK.

Local commit `4b7064a Add PureTrack backend proxy endpoints` implements and
tests the Android-facing status/connect/disconnect routes described here.
Production deployment/live-server parity, traffic proxy implementation,
Android queue drain wiring, and foreground publishing runtime rollout remain
separate future work. The Android-facing Insert route contract below is
implemented locally for Android publishing code to consume through the XCPro
backend only. P0B2A adds encrypted recoverable server-side provider session
material and fails traffic allowance closed for hash-only, missing, corrupt,
expired, or unconfigured provider-session material. The inbound traffic overlay
route contract below is implemented locally as of P0B2B on 2026-06-19.
Live-server deployment parity remains a separate release/deployment phase
before Android production rollout claims.

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
- `app/main.py` exposes `POST /api/v1/puretrack/traffic`; tests verify bearer
  and package validation, verified XCPro PRO access, app-key configuration,
  connected non-expired PureTrack `PREMIUM` provider state, decryptable
  provider session material, bbox validation, provider error mapping, local
  rate limiting, normalized DTO caching, and redaction.
- `app/main.py` stores `PureTrackProviderSession.provider_session_hash` only as
  redacted identity/dedupe material and stores recoverable provider session
  material only in encrypted
  `PureTrackProviderSession.provider_session_ciphertext`.
  Hash-only rows are not sufficient to authorize future PureTrack Traffic API
  calls and fail closed with `puretrack_provider_session_unavailable`.
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
- PureTrack provider token/session storage for inbound traffic. Existing
  hash-only session rows are not usable Traffic API credentials.
- PureTrack Traffic API calls, bbox validation, filtering, rate limiting,
  compact-row parsing, normalization, cache policy, and redaction in later
  traffic phases.
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
- Raw PureTrack Traffic API object keys, tracker IDs, target IDs, source
  tracker IDs, receiver names, phone numbers, raw pilot names, usernames, or
  provider response URLs.
- Direct PureTrack API URLs for production calls.

## Environment Contract

Server implementation must use these environment keys:

```dotenv
XCPRO_PURETRACK_APP_KEY=
XCPRO_PURETRACK_INSERT_KEY=
XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET=
XCPRO_PURETRACK_API_BASE_URL=https://puretrack.io
XCPRO_PURETRACK_TIMEOUT_SECONDS=10
```

Rules:

- `XCPRO_PURETRACK_APP_KEY`, verified XCPro `PlanTier.PRO` entitlement,
  PureTrack Pro provider access, and usable server-side provider session
  material are required before `trafficApiAllowed` can be true. Android/server
  domain models represent the PureTrack provider-capable state as `PREMIUM` to
  avoid conflating it with XCPro `PlanTier.PRO`.
- `XCPRO_PURETRACK_INSERT_KEY` is required before `insertApiConfigured` can be
  true and before `POST /api/v1/puretrack/insert` can publish upstream.
- `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET` is required before the
  backend can persist or decrypt recoverable PureTrack provider session
  material for inbound Traffic API Bearer/session use. It must not reuse
  `XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET`.
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
POST /api/v1/puretrack/traffic
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
  when server app key, XCPro `PlanTier.PRO` entitlement, PureTrack provider
  `PREMIUM` access, and usable server-side provider session material allow
  production inbound PureTrack traffic proxy calls. Android must consume this
  field and must not infer PureTrack provider entitlement locally.
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

## Inbound Traffic Overlay Route

```http
POST /api/v1/puretrack/traffic
Content-Type: application/json
```

Status: implemented locally in P0B2B. Production/live-server deployment parity
is deferred to P6.

Request body: `PureTrackTrafficRequest`.

```json
{
  "bbox": {
    "north": -37.49503,
    "east": 176.54678,
    "south": -38.06575,
    "west": 174.82046
  },
  "filters": {
    "category": "air",
    "objectTypeIds": [1, 2, 6, 7],
    "sourceTypeIds": [0, 7, 12, 16],
    "maxAgeSeconds": 300
  },
  "clientRequestId": "map-refresh-20260619-0001"
}
```

Request validation:

- Request models must reject unknown fields at every level.
- `bbox` is required.
- `bbox.north`, `bbox.south`, `bbox.east`, and `bbox.west` are required finite
  decimals.
- `north` and `south` are latitude degrees in `[-90, 90]`; `north` must be
  greater than `south`.
- `east` and `west` are longitude degrees in `[-180, 180]`; `east` must be
  greater than `west` in the initial contract.
- Anti-meridian-crossing requests are rejected with `validation_error` in the
  initial contract. Supporting anti-meridian fetches requires a later explicit
  split phase because it needs multiple upstream provider calls, merge/dedupe
  behavior, and separate verification.
- The server validates requested bbox size before calling PureTrack. Initial
  limits are:
  - width must be `<= 300000` meters;
  - height must be `<= 300000` meters;
  - diagonal must be `<= 425000` meters.
- The server must not call the provider when bbox validation fails. It must not
  rely on the provider's large-bbox fallback because the official provider docs
  say large bboxes can lower max age and return whole-planet data.
- `filters` is optional; when omitted, the default is category `air`,
  `maxAgeSeconds=300`, and no object/source filter.
- `filters.category` is optional. Allowed values are `air`, `ground`, `other`,
  and `water`, matching the provider type categories. The initial Android
  overlay phases should request `air` unless a later product phase explicitly
  extends scope.
- `filters.objectTypeIds` is optional. It is an array of unique integers in
  `[0, 999]`, maximum 32 entries. The server maps this to the provider `o`
  parameter when present.
- `filters.sourceTypeIds` is optional. It is an array of unique integers in
  `[0, 999]`, maximum 32 entries. The official provider Traffic API does not
  document an upstream source filter parameter, so the server applies this only
  after parsing normalized rows.
- `filters.maxAgeSeconds` is optional. It is an integer in `[30, 900]`, default
  `300`. The server maps it to the provider `t` parameter by rounding up to
  whole minutes.
- Provider `s` always-include and `i` isolate filters are not accepted in the
  initial Android-facing route. They require raw provider map item keys, so a
  later phase must first define an opaque Android-safe target reference if the
  product needs always-include/isolate behavior.
- `clientRequestId` is optional. When present it must be a non-blank string,
  maximum 128 characters. It is a client correlation id only and must not
  contain account emails, provider IDs, phone numbers, coordinates, or tokens.
- Android must not send PureTrack provider app keys, Insert keys, provider
  Bearer tokens, passwords, raw provider URLs, raw compact rows, or direct
  PureTrack request parameters outside this XCPro request schema.

Provider call behavior:

- Requires the common XCPro Bearer token and package header.
- Requires verified XCPro `PlanTier.PRO` entitlement for the authenticated
  account.
- Requires `XCPRO_PURETRACK_APP_KEY`; when absent, returns
  `puretrack_app_key_unconfigured`.
- Requires a connected server-side PureTrack provider session with provider
  access `PREMIUM`. `FREE`, `NONE`, `UNKNOWN`, `ERROR`, expired, missing, or
  invalid provider state must not call the Traffic API.
- Requires recoverable server-side PureTrack provider session material for
  Bearer authentication to the provider Traffic API. Existing hash-only
  `provider_session_hash` rows are insufficient for traffic and must fail
  closed with `puretrack_provider_session_unavailable` or require reconnect.
- The backend injects the server-only `XCPRO_PURETRACK_APP_KEY` into the
  upstream Traffic API request and sends the provider Bearer token/session only
  server-to-provider.
- The backend may call the provider with POST or GET as allowed by the provider,
  but Android sees only this XCPro POST route.
- The route does not mutate PureTrack connect/disconnect state, outbound Insert
  queue state, LiveFollow state, map state, traffic state, or profile state.

Success response: `200 PureTrackTrafficResponse`.

```json
{
  "result": "OK",
  "targets": [
    {
      "targetId": "pt_opaque_01H...",
      "lastSeenAtMs": 1713592586000,
      "latitudeDeg": -37.78174,
      "longitudeDeg": 174.88159,
      "altitudeGpsMeters": 4685.0,
      "altitudePressureMeters": null,
      "courseDeg": 338.0,
      "groundSpeedMps": 144.05,
      "verticalSpeedMps": -13.31,
      "objectTypeId": 56,
      "objectCategory": "air",
      "sourceTypeId": 12,
      "sourceLabel": "ADSBHub",
      "displayLabel": "ZK-MZE",
      "registration": "ZK-MZE",
      "callsign": "ANZ118M",
      "model": null,
      "colorHex": null,
      "groundElevationMeters": 43.0,
      "thermalClimbRateMps": null,
      "signalQuality": null,
      "stealth": false,
      "noTracking": false,
      "onGround": false,
      "randomId": false
    }
  ],
  "bbox": {
    "north": -37.49503,
    "east": 176.54678,
    "south": -38.06575,
    "west": 174.82046
  },
  "filtersApplied": {
    "category": "air",
    "objectTypeIds": [1, 2, 6, 7],
    "sourceTypeIds": [0, 7, 12, 16],
    "maxAgeSeconds": 300
  },
  "serverFetchedAtMs": 1760000000000,
  "freshUntilMs": 1760000005000,
  "providerRowCount": 1,
  "droppedRowCount": 0,
  "redactedFieldCount": 3,
  "cacheStatus": "MISS",
  "retryAfterMs": null,
  "auditId": "redacted-audit-id"
}
```

`PureTrackTrafficResponse` fields:

- `result`: one of `OK`, `EMPTY`, `DEGRADED_CACHE`, or `PARTIAL`. HTTP errors
  use the normal error envelope instead of these result values.
- `targets`: normalized live overlay targets, never raw provider rows.
- `bbox`: the validated Android-requested bbox, not a provider URL and not a
  raw provider debug bbox.
- `filtersApplied`: the validated filters the server used.
- `serverFetchedAtMs`: server wall-clock epoch milliseconds when the provider
  fetch or cache hit was produced.
- `freshUntilMs`: server wall-clock epoch milliseconds after which Android
  should consider this response stale unless a later repository phase defines a
  stricter local freshness rule.
- `providerRowCount`: count of provider compact rows received before redaction
  and filtering. It is diagnostic only and must not expose row contents.
- `droppedRowCount`: count of rows dropped because required fields were missing,
  malformed, outside requested filters, or privacy rules required omission.
- `redactedFieldCount`: count of provider fields omitted for privacy/security.
- `cacheStatus`: one of `MISS`, `HIT`, or `BYPASS`.
- `retryAfterMs`: nullable non-negative retry delay. When the HTTP response has
  `Retry-After`, this value and the header must agree.
- `auditId`: redacted backend support correlation id. It must not encode the
  bearer token, account email, provider email, provider token, app key, bbox,
  location, provider object key, raw row, or provider URL.

`PureTrackTrafficTarget` fields:

- `targetId`: required opaque Android-safe id. It may be stable for a provider
  object across refreshes, but it must not contain the raw provider `K` key,
  tracker UID, target id, aircraft id, source tracker id, phone, email, or
  provider token. Android treats it as opaque and must not display it.
- `lastSeenAtMs`: required provider timestamp converted from Unix seconds to
  epoch milliseconds.
- `latitudeDeg`, `longitudeDeg`: required decimal degrees.
- `altitudeGpsMeters`, `altitudePressureMeters`, `groundElevationMeters`:
  optional meters.
- `courseDeg`: optional degrees in `[0, 360]`.
- `groundSpeedMps`, `verticalSpeedMps`, `thermalClimbRateMps`: optional meters
  per second.
- `objectTypeId`: optional provider object type id.
- `objectCategory`: optional category derived from the official type list.
- `sourceTypeId`: optional provider source type id.
- `sourceLabel`: optional server-owned display label for known source ids. It
  must be generic and must not include raw receiver names or account details.
- `displayLabel`: optional sanitized display label, maximum 48 characters.
  Server selection must prefer public aircraft registration or callsign-style
  values and must never expose phone numbers, email addresses, provider token
  fragments, raw pilot names, usernames, or raw tracker ids.
- `registration`: optional sanitized aircraft registration, maximum 32
  characters.
- `callsign`: optional sanitized callsign, maximum 32 characters.
- `model`: optional sanitized aircraft model, maximum 64 characters.
- `colorHex`: optional `#RRGGBB` display color. Invalid provider colors become
  null.
- `signalQuality`: optional finite numeric quality if provider row carries it.
- `stealth`, `noTracking`, `onGround`, `randomId`: optional booleans derived
  from provider flags; absent flags map to `false`.

Privacy and redaction:

- The server must never return raw compact rows, raw provider responses, raw
  provider URLs, provider app key, provider Bearer token/session, Insert key,
  provider password, raw account email, or Android `Authorization` header.
- The server must never return provider `phone`, raw pilot `name`, `username`,
  raw `tracker_uid`, raw `tracker_id`, raw `target_id`, raw `target_key`,
  raw `aircraft_id`, raw `receiver_name`, raw source-specific keys such as
  InReach/SPOT/FFVL identifiers, raw competition names/classes, takeoff/landing
  ids, voltage fields, satellite counts, or OGN forwarding identifiers.
- When provider stealth/no-tracking flags are present, the server may return
  location only as already provided by the provider Traffic API, but it must
  omit identity fields that are not needed for rendering: `displayLabel`,
  `registration`, `callsign`, `model`, and `colorHex`.
- Parser, mapper, error, audit, support, and test-fixture code must prove that
  excluded fields cannot reach Android-visible responses or logs.

Cache, freshness, and rate limits:

- Initial server cache is optional and may be in-memory only. If implemented,
  it must be per-authenticated XCPro account and keyed by normalized bbox,
  filters, and package context. It must not be shared across accounts.
- Cache TTL must be `<= 5000` ms. Cached responses must set
  `cacheStatus=HIT`, preserve the original `serverFetchedAtMs`, and compute
  `freshUntilMs` from the cached fetch time.
- Raw provider rows must not be persisted in cache. Cache may store only the
  normalized redacted `PureTrackTrafficResponse`.
- The route should enforce a per-account rate limit of 12 requests per minute
  with a burst of 3. A stricter production limit is allowed; a looser limit
  requires a contract update.
- Rate-limited responses return HTTP 429, error code `puretrack_rate_limited`,
  a `Retry-After` header in seconds, and JSON `retryAfterMs` when the response
  shape can carry it.
- Provider 429 responses return HTTP 429 with `puretrack_rate_limited` and
  propagate a reliable provider `Retry-After` delay after clamping it to a
  non-negative value.
- Provider timeouts, network failures, and provider 5xx return
  `puretrack_provider_unavailable`; if a non-expired normalized cache entry is
  available, the server may return `200 result=DEGRADED_CACHE` instead.

Official provider references checked for this contract:

- Traffic API: `https://puretrack.io/help/api`, last checked 2026-06-19.
- Type list: `https://puretrack.io/types.json`, last checked 2026-06-19.
- Insert API: `https://puretrack.io/help/api-insert`, last checked
  2026-06-19. This is outbound contrast only and must not drive inbound
  overlay route cadence, Device ID, queue, or Insert-key behavior.

P0B server tests must add or update `app/tests/test_puretrack_backend_proxy.py`
coverage for:

- route registration and request model unknown-field rejection;
- valid/invalid bearer and package headers;
- missing app key, missing/expired/non-`PREMIUM` provider session, hash-only
  provider session material, and missing XCPro PRO entitlement;
- bbox coordinate bounds, size limits, and anti-meridian rejection;
- category/object/source/max-age filter mapping and source post-filtering;
- always-include/isolate rejection in the initial contract;
- parser required fields, optional fields, malformed row drops, unit mapping,
  and nullability;
- privacy redaction for phone, name, username, raw keys/ids, receiver names,
  provider URL, app key, and provider token;
- cache hit/miss/freshness behavior, route rate limiting, provider
  `Retry-After` propagation, and redacted `auditId` behavior.

Android P1B contract tests must prove Android DTOs contain only the normalized
fields above and have no raw compact row, provider URL, provider token/session,
app key, phone, username, or raw provider key/id fields.

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
  access for outbound Insert publishing or inbound traffic.
- `puretrack_app_key_unconfigured`: server lacks
  `XCPRO_PURETRACK_APP_KEY`.
- `puretrack_insert_key_unconfigured`: server lacks
  `XCPRO_PURETRACK_INSERT_KEY`.
- `puretrack_insert_rejected`: request was valid at the XCPro boundary but the
  upstream provider rejected the whole Insert batch as non-retryable.
- `puretrack_provider_not_connected`: authenticated XCPro account has no
  connected PureTrack provider session for inbound traffic.
- `puretrack_provider_access_denied`: connected provider state is not
  `PREMIUM`, so PureTrack Pro provider access is missing for inbound traffic.
- `puretrack_provider_session_unavailable`: provider state exists but the
  server lacks usable recoverable provider Bearer/session material for Traffic
  API authentication.
- `puretrack_provider_unavailable`: PureTrack request failed, timed out, or
  returned retryable 5xx/429.
- `puretrack_traffic_rejected`: request was valid at the XCPro boundary but the
  upstream provider rejected the Traffic API request as non-retryable.
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
- `trafficApiAllowed` may be true only when the server has app-key config,
  verified XCPro PRO entitlement, provider `PREMIUM` access, and usable
  server-side provider session material. Hash-only provider session state must
  not unlock inbound traffic.
- Android P3A2 is required only if the entitlement provider-state fields above
  change or if entitlement readback starts carrying additional PureTrack fields.

No entitlement response may include PureTrack tokens, app key, password,
account raw email, or raw provider payloads.

## Traffic API And Insert API Separation

Inbound PureTrack overlay/aircraft traffic must use the XCPro backend
`POST /api/v1/puretrack/traffic` route and must return normalized XCPro DTOs.
Android must not parse or store raw compact PureTrack Traffic API rows.

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
- Raw PureTrack Traffic API object keys, tracker UIDs, tracker IDs, target IDs,
  target keys, aircraft IDs, source tracker IDs, receiver names, source-specific
  keys, phone numbers, pilot names, usernames, competition names/classes,
  takeoff/landing IDs, voltage values, satellite counts, and OGN forwarding
  identifiers.
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
5. Complete/current contract docs: specify `POST /api/v1/puretrack/traffic`
   for inbound normalized overlay traffic before Android implementation.
6. Complete/current locally in commit `8fbdb4d`: add traffic request/response
   models, bbox validation, parser,
   normalization, redaction helpers, and tests without exposing a route if the
   server structure allows that split.
7. P0B2A complete locally with post-review PASS: add nullable
   `provider_session_ciphertext`, dedicated provider-session encryption config
   from `XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET`, exact
   encrypt/decrypt/material-available helpers, and status/connect tests proving
   `trafficApiAllowed` requires usable recoverable session material.
8. Complete/current P0B2B: added the traffic route/provider adapter,
   entitlement checks, cache/rate limits, and route tests. This phase consumes
   the encrypted provider-session read path from P0B2A and does not treat
   hash-only rows as Traffic API credentials. Local verification:
   `.venv\Scripts\python.exe -m pytest app\tests\test_puretrack_backend_proxy.py`
   passed with `39 passed`.
9. Planned later: record live-server deployment parity before production
   rollout.

Android P3A1 is complete. Android P3B1 may implement the production HTTP
adapter against these XCPro backend status/connect/disconnect routes after the
Android PureTrack IP records the local server evidence. Android inbound traffic
P1B/P2 phases must wait until the server traffic contract and P0B route
evidence exist. Live server deployment parity remains a separate
deployment/release concern.
