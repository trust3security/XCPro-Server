from dataclasses import dataclass
from collections.abc import Mapping as MappingABC
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StrictInt, StrictStr
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional
from urllib.parse import quote
import uuid
import os
import json
import logging
import random
import string
import secrets
import hashlib
import hmac
import math
import re
import base64
import threading
import time

import httpx
from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, and_, case, create_engine, func, or_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, sessionmaker
import redis
from cryptography.fernet import Fernet, InvalidToken

try:
    from google.auth.exceptions import GoogleAuthError
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token as google_id_token
    from google.oauth2 import service_account as google_service_account
except ImportError:
    GoogleAuthError = None
    google_requests = None
    google_id_token = None
    google_service_account = None

try:
    import firebase_admin
    from firebase_admin import auth as firebase_auth
    from firebase_admin import credentials as firebase_credentials
    from firebase_admin import exceptions as firebase_exceptions
except ImportError:
    firebase_admin = None
    firebase_auth = None
    firebase_credentials = None
    firebase_exceptions = None

try:
    from pydantic import model_validator

    PYDANTIC_V2 = True
except ImportError:
    from pydantic import root_validator

    PYDANTIC_V2 = False

app = FastAPI()
logger = logging.getLogger("xcpro.puretrack.traffic")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/xcpro")
STALE_AFTER_SECONDS = 120
MAX_POSITION_FUTURE_SKEW_SECONDS = 300
MAX_REASONABLE_ALT_M = 20000
MIN_REASONABLE_ALT_M = -1000
MAX_REASONABLE_SPEED = 1000
MAX_IMPOSSIBLE_GROUND_SPEED_KMH = 500
MAX_TASK_RADIUS_M = 500000
SPECTATOR_STATS_CURRENT_CLIMB_MAX_DELTA_SECONDS = 120.0
SPECTATOR_STATS_BEST_CLIMB_MIN_WINDOW_SECONDS = 20.0
SPECTATOR_STATS_BEST_CLIMB_MAX_WINDOW_SECONDS = 45.0
HANDLE_PATTERN = re.compile(r"^[a-z0-9._]{3,24}$")
FOLLOW_REQUEST_STATUS_PENDING = "pending"
FOLLOW_REQUEST_STATUS_ACCEPTED = "accepted"
FOLLOW_REQUEST_STATUS_DECLINED = "declined"
NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED = "follow_request_received"
NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED = "follow_request_accepted"
NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER = "follow_new_follower"
NOTIFICATION_EVENT_FOLLOW_MUTUAL = "follow_mutual"
NOTIFICATION_OUTBOX_STATUS_PENDING = "pending"
NOTIFICATION_OUTBOX_STATUS_SENT = "sent"
NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED = "retryable_failed"
NOTIFICATION_OUTBOX_STATUS_FAILED = "failed"
NOTIFICATION_OUTBOX_DELIVERABLE_STATUSES = frozenset({
    NOTIFICATION_OUTBOX_STATUS_PENDING,
    NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED,
})
SOCIAL_NOTIFICATION_EVENT_TYPES = frozenset({
    NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
    NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED,
    NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER,
    NOTIFICATION_EVENT_FOLLOW_MUTUAL,
})
NOTIFICATION_OUTBOX_ERROR_MAX_LENGTH = 320
NOTIFICATION_DELIVERY_DEFAULT_LIMIT = 50
NOTIFICATION_DELIVERY_MAX_LIMIT = 200
LIVE_VISIBILITY_OFF = "off"
LIVE_VISIBILITY_FOLLOWERS = "followers"
LIVE_VISIBILITY_PUBLIC = "public"
SEARCH_RELATIONSHIP_NONE = "none"
SEARCH_RELATIONSHIP_OUTGOING_PENDING = "outgoing_pending"
SEARCH_RELATIONSHIP_INCOMING_PENDING = "incoming_pending"
SEARCH_RELATIONSHIP_FOLLOWING = "following"
SEARCH_RELATIONSHIP_FOLLOWED_BY = "followed_by"
SEARCH_RELATIONSHIP_MUTUAL = "mutual"
MIN_SEARCH_QUERY_LENGTH = 2
SEARCH_RESULT_LIMIT = 25
RELATIONSHIP_LIST_DEFAULT_LIMIT = 50
RELATIONSHIP_LIST_MAX_LIMIT = 200
BULK_RELATIONSHIP_STATUS_MAX_IDS = 100
PRIVATE_FOLLOW_BEARER_VERSION = 1
DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS = 60 * 60 * 24 * 30
XCPRO_RELEASE_PACKAGE_NAME = "com.trust3.xcpro"
XCPRO_DEBUG_PACKAGE_NAME = "com.trust3.xcpro.debug"
FREE_ENTITLEMENT_STALE_AFTER_MS = 86_400_000
FREE_ENTITLEMENT_HARD_REFRESH_AFTER_MS = 604_800_000
DENIED_ENTITLEMENT_STALE_AFTER_MS = 900_000
DENIED_ENTITLEMENT_HARD_REFRESH_AFTER_MS = 3_600_000
PAID_CONTINUITY_STALE_AFTER_MS = 21_600_000
PAID_CONTINUITY_HARD_REFRESH_AFTER_MS = 259_200_000
PLAN_TIER_VALUES = frozenset({"FREE", "BASIC", "SOARING", "XC", "PRO"})
PURETRACK_USER_ACCESS_VALUES = frozenset({"UNKNOWN", "NONE", "FREE", "PREMIUM", "ERROR"})
PURETRACK_PROVIDER_ACCESS_UNKNOWN = "UNKNOWN"
PURETRACK_PROVIDER_ACCESS_NONE = "NONE"
PURETRACK_PROVIDER_ACCESS_FREE = "FREE"
PURETRACK_PROVIDER_ACCESS_PREMIUM = "PREMIUM"
PURETRACK_PROVIDER_ACCESS_ERROR = "ERROR"
PURETRACK_CONNECT_RESULT_CONNECTED = "CONNECTED"
PURETRACK_CONNECT_RESULT_AUTH_REJECTED = "AUTH_REJECTED"
PURETRACK_CONNECT_RESULT_APP_KEY_UNCONFIGURED = "APP_KEY_UNCONFIGURED"
PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
PURETRACK_CONNECT_RESULT_RATE_LIMITED = "RATE_LIMITED"
PURETRACK_CONNECT_RESULT_ERROR = "ERROR"
PURETRACK_DISCONNECT_RESULT_DISCONNECTED = "DISCONNECTED"
PURETRACK_DISCONNECT_RESULT_NOT_CONNECTED = "NOT_CONNECTED"
PURETRACK_DISCONNECT_RESULT_ERROR = "ERROR"
PURETRACK_INSERT_RESULT_ACCEPTED = "ACCEPTED"
PURETRACK_INSERT_RESULT_PARTIAL_RETRY = "PARTIAL_RETRY"
PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
PURETRACK_INSERT_RESULT_REJECTED = "REJECTED"
PURETRACK_TRAFFIC_RESULT_OK = "OK"
PURETRACK_TRAFFIC_RESULT_EMPTY = "EMPTY"
PURETRACK_TRAFFIC_RESULT_DEGRADED_CACHE = "DEGRADED_CACHE"
PURETRACK_TRAFFIC_RESULT_PARTIAL = "PARTIAL"
PURETRACK_TRAFFIC_CACHE_MISS = "MISS"
PURETRACK_TRAFFIC_CACHE_HIT = "HIT"
PURETRACK_TRAFFIC_CACHE_BYPASS = "BYPASS"
PURETRACK_TRAFFIC_ROUTE_PATH = "/api/v1/puretrack/traffic"
PURETRACK_TRAFFIC_EVIDENCE_EVENT_NAME = "puretrack_traffic_cadence"
PURETRACK_TRAFFIC_DEFAULT_CATEGORY = "air"
PURETRACK_TRAFFIC_DEFAULT_MAX_AGE_SECONDS = 300
PURETRACK_TRAFFIC_MIN_MAX_AGE_SECONDS = 30
PURETRACK_TRAFFIC_MAX_MAX_AGE_SECONDS = 900
PURETRACK_TRAFFIC_MAX_FILTER_IDS = 32
PURETRACK_TRAFFIC_MIN_FILTER_ID = 0
PURETRACK_TRAFFIC_MAX_FILTER_ID = 999
PURETRACK_TRAFFIC_MAX_BBOX_WIDTH_METERS = 300_000.0
PURETRACK_TRAFFIC_MAX_BBOX_HEIGHT_METERS = 300_000.0
PURETRACK_TRAFFIC_MAX_BBOX_DIAGONAL_METERS = 425_000.0
PURETRACK_TRAFFIC_EARTH_RADIUS_METERS = 6_371_000.0
PURETRACK_TRAFFIC_CACHE_TTL_MS = 5_000
PURETRACK_TRAFFIC_RATE_LIMIT_BURST = 3
PURETRACK_TRAFFIC_RATE_LIMIT_PER_MINUTE = 12
PURETRACK_TRAFFIC_ALLOWED_CATEGORIES = frozenset({"air", "ground", "other", "water"})
PURETRACK_TRAFFIC_OBJECT_TYPE_CATEGORIES = {
    0: "other",
    1: "air",
    2: "air",
    3: "air",
    4: "air",
    5: "air",
    6: "air",
    7: "air",
    8: "air",
    9: "air",
    10: "air",
    11: "air",
    12: "air",
    13: "air",
    15: "other",
    16: "air",
    17: "air",
    18: "air",
    19: "air",
    20: "other",
    21: "ground",
    22: "ground",
    23: "ground",
    24: "ground",
    25: "ground",
    26: "ground",
    27: "ground",
    28: "ground",
    29: "ground",
    30: "ground",
    31: "ground",
    32: "ground",
    33: "ground",
    34: "ground",
    35: "ground",
    36: "water",
    37: "water",
    38: "water",
    39: "water",
    40: "water",
    41: "water",
    42: "water",
    43: "water",
    44: "water",
    45: "water",
    46: "water",
    47: "ground",
    48: "air",
    49: "air",
    50: "air",
    51: "air",
    52: "air",
    53: "air",
    54: "air",
    55: "air",
    56: "air",
    57: "air",
    58: "air",
    59: "ground",
    60: "water",
    61: "water",
    62: "water",
    63: "other",
    64: "ground",
    65: "other",
    66: "water",
    67: "ground",
    68: "ground",
    69: "ground",
    70: "ground",
    71: "water",
    72: "water",
    73: "water",
    74: "water",
    75: "water",
    76: "water",
    77: "water",
    78: "water",
    79: "ground",
    80: "ground",
    81: "ground",
    82: "air",
}
PURETRACK_TRAFFIC_SOURCE_LABELS = {
    0: "OGN",
    1: "SPOT",
    2: "Particle",
    3: "Overland",
    4: "SPOT NZ",
    5: "InReach NZ",
    6: "BTraced",
    7: "PureTrack API",
    8: "MT600 GNZ",
    9: "InReach",
    10: "IGC File",
    11: "Pi",
    12: "ADSBHub",
    13: "IGC Droid",
    14: "SeeYou Navigator",
    16: "PureTrack App",
    17: "Teltonika",
    18: "Cellular tracker",
    19: "MT600",
    20: "MT600-l",
    21: "API",
    22: "FR24",
    23: "XContest",
    24: "SkyLines",
    25: "Flymaster",
    26: "LiveGliding",
    27: "ADSBExchange",
    28: "adsb.lol",
    29: "adsb.fi",
    30: "SportsTrackLive",
    31: "FFVL Tracking",
    32: "Zoleo",
    33: "Total Vario",
    34: "Tracker App",
    35: "OGN ICAO",
    36: "XC Guide",
    37: "Bircom",
    38: "JimiIoT",
    39: "XCMania",
    40: "Traccar",
    41: "SKYTRAXX",
    42: "Gaggle",
    43: "Wingman",
    44: "Schar",
    45: "airplanes.live",
    46: "ADSB",
    47: "XCglobe/FlyMe",
    48: "NASA Balloons",
    49: "XAlps",
    50: "Naviter Omni",
    51: "Garmin Watch",
    52: "Ticatag",
    53: "Naviter Oudie N",
    54: "GPSLogger",
    55: "ProteGear",
    56: "SkySignals",
    57: "GeoTracks",
}
PURETRACK_TRAFFIC_EXCLUDED_ROW_KEYS = frozenset({
    "K",
    "D",
    "J",
    "R",
    "N",
    "i",
    "j",
    "k",
    "l",
    "u",
    "n",
    "b",
    "q",
    "p",
    "F",
    "1",
    "2",
    "3",
    "4",
    "5",
    "^",
})
PURETRACK_TRAFFIC_STEALTH_IDENTITY_KEYS = frozenset({"B", "E", "M", "c", "m"})
PURETRACK_DEFAULT_API_BASE_URL = "https://puretrack.io"
PURETRACK_DEFAULT_TIMEOUT_SECONDS = 10.0
BILLING_PERIOD_VALUES = frozenset({"NONE", "MONTHLY", "ANNUAL"})
ENTITLEMENT_SOURCE_VALUES = frozenset({"NONE", "GOOGLE_PLAY"})
SUBSCRIPTION_STATUS_VALUES = frozenset({
    "FREE_ACTIVE",
    "PENDING",
    "ACTIVE",
    "GRACE_PERIOD",
    "CANCELED_BUT_ACTIVE",
    "ON_HOLD",
    "PAUSED",
    "SUSPENDED",
    "EXPIRED",
    "REVOKED",
    "RECOVERY_REQUIRED",
    "ERROR",
})
VERIFICATION_STATE_VALUES = frozenset({
    "VERIFIED",
    "FREE_CANONICAL",
    "STALE_CACHE",
    "UNVERIFIED",
    "ACCOUNT_MISMATCH",
    "RECOVERY_REQUIRED",
    "ERROR",
})
RECOVERY_ACTION_VALUES = frozenset({
    "NONE",
    "SIGN_IN_REQUIRED",
    "CONTACT_SUPPORT",
    "CHOOSE_CORRECT_ACCOUNT",
    "OPEN_PLAY_SUBSCRIPTIONS",
    "RETRY_LATER",
})
PRODUCT_ID_BY_TIER = {
    "BASIC": "xcpro_basic",
    "SOARING": "xcpro_soaring",
    "XC": "xcpro_xc",
    "PRO": "xcpro_pro",
}
TIER_BY_PRODUCT_ID = {product_id: tier for tier, product_id in PRODUCT_ID_BY_TIER.items()}
BASE_PLAN_BY_PERIOD = {
    "MONTHLY": "monthly-auto2",
    "ANNUAL": "annual-auto2",
}
LEGACY_BASE_PLANS_BY_PERIOD = {
    "MONTHLY": frozenset({"monthly", "monthly-auto"}),
    "ANNUAL": frozenset({"annual", "annual-auto"}),
}
PERIOD_BY_BASE_PLAN = {base_plan_id: period for period, base_plan_id in BASE_PLAN_BY_PERIOD.items()}
for period, base_plan_ids in LEGACY_BASE_PLANS_BY_PERIOD.items():
    PERIOD_BY_BASE_PLAN.update({base_plan_id: period for base_plan_id in base_plan_ids})
PAID_CONTINUITY_STATUSES = frozenset({"ACTIVE", "GRACE_PERIOD", "CANCELED_BUT_ACTIVE"})
DENIED_SUBSCRIPTION_STATUSES = frozenset({
    "PENDING",
    "ON_HOLD",
    "PAUSED",
    "SUSPENDED",
    "EXPIRED",
    "REVOKED",
    "RECOVERY_REQUIRED",
    "ERROR",
})
LIVEFOLLOW_FOLLOWING_CAP_BY_TIER = {
    "FREE": 1,
    "BASIC": 4,
    "SOARING": 15,
    "XC": 50,
    "PRO": 100,
}
RELATIONSHIP_LIMIT_UNDER = "under_limit"
RELATIONSHIP_LIMIT_AT = "at_limit"
RELATIONSHIP_LIMIT_OVER = "over_limit"
PUSH_PLATFORM_ANDROID = "android"
PUSH_PROVIDER_FCM = "fcm"
PUSH_TOKEN_MAX_LENGTH = 4096
PUSH_DEVICE_ID_MAX_LENGTH = 160
PUSH_APP_VERSION_MAX_LENGTH = 80
PUSH_TOKEN_FERNET_KEY_CONTEXT = b"xcpro-push-token-fernet-v1"
PURETRACK_PROVIDER_SESSION_FERNET_KEY_CONTEXT = b"xcpro-puretrack-provider-session-fernet-v1"
FCM_MESSAGING_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
FCM_SEND_URL_TEMPLATE = "https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
GOOGLE_PLAY_SYNC_RESULT_VALUES = frozenset({
    "ACCEPTED_VERIFIED",
    "ACCEPTED_PENDING",
    "CANONICAL_FREE",
    "ACCOUNT_MISMATCH",
    "TOKEN_ALREADY_OWNED",
    "SUPERSEDED_PURCHASE_IGNORED",
    "INVALID_PRODUCT",
    "INVALID_BASE_PLAN",
    "INVALID_PACKAGE",
    "PENDING_NO_ENTITLEMENT",
    "REVOKED_OR_EXPIRED",
    "VERIFICATION_TEMPORARILY_UNAVAILABLE",
    "ERROR",
})
GOOGLE_PLAY_CLIENT_PURCHASE_STATE_VALUES = frozenset({
    "PENDING",
    "PURCHASED",
    "UNSPECIFIED",
})
GOOGLE_PLAY_ACKNOWLEDGEMENT_STATES = frozenset({
    "NOT_REQUIRED",
    "ACK_PENDING",
    "ACKNOWLEDGED",
    "ACK_RETRYABLE",
    "ACK_FAILED",
})
GOOGLE_PLAY_ANDROID_PUBLISHER_SCOPE = "https://www.googleapis.com/auth/androidpublisher"
GOOGLE_PLAY_ANDROID_PUBLISHER_BASE_URL = (
    "https://androidpublisher.googleapis.com/androidpublisher/v3"
)
GOOGLE_PLAY_RTDN_PROCESSING_RESULTS = frozenset({
    "RECORDED",
    "DUPLICATE",
    "TEST_NOTIFICATION",
    "ACCEPTED_VERIFIED",
    "ACCEPTED_PENDING",
    "REVOKED_OR_EXPIRED",
    "TOKEN_NOT_OWNED",
    "SUPERSEDED_PURCHASE_IGNORED",
    "INVALID_PRODUCT",
    "INVALID_BASE_PLAN",
    "INVALID_PACKAGE",
    "VERIFICATION_TEMPORARILY_UNAVAILABLE",
    "ERROR",
})
GOOGLE_PLAY_RTDN_RETRYABLE_RESULTS = frozenset({
    "RECORDED",
    "VERIFICATION_TEMPORARILY_UNAVAILABLE",
})
DISCOVERABILITY_VALUES = frozenset({"searchable", "hidden"})
FOLLOW_POLICY_VALUES = frozenset({"approval_required", "auto_approve", "closed"})
DEFAULT_LIVE_VISIBILITY_VALUES = frozenset({
    LIVE_VISIBILITY_OFF,
    LIVE_VISIBILITY_FOLLOWERS,
    LIVE_VISIBILITY_PUBLIC
})
CONNECTION_LIST_VISIBILITY_VALUES = frozenset({"owner_only", "mutuals_only", "public"})
DEFAULT_DISCOVERABILITY = "searchable"
DEFAULT_FOLLOW_POLICY = "approval_required"
DEFAULT_LIVE_VISIBILITY = LIVE_VISIBILITY_FOLLOWERS
DEFAULT_CONNECTION_LIST_VISIBILITY = "owner_only"
DEFAULT_SOCIAL_NOTIFICATIONS_ENABLED = True
DEFAULT_LIVE_NOTIFICATIONS_ENABLED = False
RUNTIME_ENV_DEV = "dev"
RUNTIME_ENV_STAGING = "staging"
RUNTIME_ENV_PROD = "prod"
PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


class ErrorCode:
    VALIDATION_ERROR = "validation_error"
    UNAUTHENTICATED = "unauthenticated"
    AUTH_UNAVAILABLE = "auth_unavailable"
    INVALID_PACKAGE = "invalid_package"
    INVALID_PRODUCT = "invalid_product"
    INVALID_BASE_PLAN = "invalid_base_plan"
    GOOGLE_PLAY_VERIFICATION_UNAVAILABLE = "google_play_verification_unavailable"
    RTDN_AUTH_UNAVAILABLE = "rtdn_auth_unavailable"
    INVALID_RTDN_AUTH = "invalid_rtdn_auth"
    INVALID_RTDN_ENVELOPE = "invalid_rtdn_envelope"
    ENTITLEMENT_STATE_INVALID = "entitlement_state_invalid"
    INVALID_GOOGLE_ID_TOKEN = "invalid_google_id_token"
    INVALID_FIREBASE_ID_TOKEN = "invalid_firebase_id_token"
    UNSUPPORTED_FIREBASE_AUTH_PROVIDER = "unsupported_firebase_auth_provider"
    EMAIL_VERIFICATION_REQUIRED = "email_verification_required"
    AUTH_RECOVERY_REQUIRED = "auth_recovery_required"
    SESSION_NOT_FOUND = "session_not_found"
    MISSING_SESSION_TOKEN = "missing_session_token"
    SESSION_TOKEN_UNAVAILABLE = "session_token_unavailable"
    INVALID_SESSION_TOKEN = "invalid_session_token"
    SESSION_ALREADY_ENDED = "session_already_ended"
    POSITION_COORDINATE_OUT_OF_RANGE = "position_coordinate_out_of_range"
    POSITION_ALT_OUT_OF_RANGE = "position_alt_out_of_range"
    POSITION_SPEED_OUT_OF_RANGE = "position_speed_out_of_range"
    POSITION_HEADING_OUT_OF_RANGE = "position_heading_out_of_range"
    POSITION_TIMESTAMP_IN_FUTURE = "position_timestamp_in_future"
    POSITION_OUT_OF_ORDER = "position_out_of_order"
    POSITION_CONFLICTING_DUPLICATE_TIMESTAMP = "position_conflicting_duplicate_timestamp"
    POSITION_IMPOSSIBLE_JUMP = "position_impossible_jump"
    INVALID_NUMERIC_VALUE = "invalid_numeric_value"
    TASK_NAME_REQUIRED = "task_name_required"
    TASK_TURNPOINTS_INVALID = "task_turnpoints_invalid"
    TASK_TURNPOINT_INVALID = "task_turnpoint_invalid"
    TASK_TURNPOINT_NAME_REQUIRED = "task_turnpoint_name_required"
    TASK_TURNPOINT_TYPE_REQUIRED = "task_turnpoint_type_required"
    TASK_TURNPOINT_COORDINATES_REQUIRED = "task_turnpoint_coordinates_required"
    TASK_COORDINATE_OUT_OF_RANGE = "task_coordinate_out_of_range"
    TASK_RADIUS_OUT_OF_RANGE = "task_radius_out_of_range"
    TASK_BOUNDARY_INVALID = "task_boundary_invalid"
    TASK_BOUNDARY_TYPE_INVALID = "task_boundary_type_invalid"
    TASK_BOUNDARY_RADIUS_OUT_OF_RANGE = "task_boundary_radius_out_of_range"
    TASK_CLEAR_PAYLOAD_INVALID = "task_clear_payload_invalid"
    HANDLE_ALREADY_TAKEN = "handle_already_taken"
    INVALID_HANDLE = "invalid_handle"
    PROFILE_INCOMPLETE = "profile_incomplete"
    INVALID_PRIVACY_SETTING = "invalid_privacy_setting"
    SEARCH_QUERY_TOO_SHORT = "search_query_too_short"
    USER_NOT_FOUND = "user_not_found"
    FOLLOW_REQUEST_SELF = "follow_request_self"
    FOLLOW_REQUEST_ALREADY_EXISTS = "follow_request_already_exists"
    FOLLOW_REQUEST_CLOSED = "follow_request_closed"
    ALREADY_FOLLOWING = "already_following"
    FOLLOW_REQUEST_NOT_FOUND = "follow_request_not_found"
    FOLLOW_REQUEST_NOT_PENDING = "follow_request_not_pending"
    FAVORITE_REQUIRES_FOLLOWING = "favorite_requires_following"
    LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED = "livefollow_following_limit_exceeded"
    LIVEFOLLOW_RATE_LIMITED = "livefollow_rate_limited"
    NOT_AUTHORIZED_TO_VIEW_FOLLOWERS = "not_authorized_to_view_followers"
    NOT_AUTHORIZED_TO_VIEW_FOLLOWING = "not_authorized_to_view_following"
    BLOCK_SELF = "block_self"
    BLOCKED_RELATIONSHIP = "blocked_relationship"
    INVALID_PUSH_TOKEN = "invalid_push_token"
    PUSH_TOKEN_ENCRYPTION_UNAVAILABLE = "push_token_encryption_unavailable"
    PURETRACK_APP_KEY_UNCONFIGURED = "puretrack_app_key_unconfigured"
    PURETRACK_INSERT_KEY_UNCONFIGURED = "puretrack_insert_key_unconfigured"
    PURETRACK_INSERT_REJECTED = "puretrack_insert_rejected"
    PURETRACK_PROVIDER_NOT_CONNECTED = "puretrack_provider_not_connected"
    PURETRACK_PROVIDER_ACCESS_DENIED = "puretrack_provider_access_denied"
    PURETRACK_PROVIDER_SESSION_UNAVAILABLE = "puretrack_provider_session_unavailable"
    PURETRACK_PROVIDER_UNAVAILABLE = "puretrack_provider_unavailable"
    PURETRACK_TRAFFIC_REJECTED = "puretrack_traffic_rejected"
    PURETRACK_RATE_LIMITED = "puretrack_rate_limited"
    PURETRACK_STATE_INVALID = "puretrack_state_invalid"
    FEATURE_ACCESS_DENIED = "feature_access_denied"


POSITION_MONOTONIC_FIELD_NAMES = frozenset({
    "fix_mono_ms",
    "fixMonoMs",
    "monotonic_ms",
    "monotonicMs",
    "monotonic_time_ms",
    "monotonicTimeMs",
    "client_monotonic_ms",
    "clientMonotonicMs"
})


class ApiHTTPException(HTTPException):
    def __init__(
        self,
        status_code: int,
        code: str,
        detail: Any,
        headers: Optional[dict[str, str]] = None
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)
        self.code = code


def utcnow() -> datetime:
    return datetime.utcnow()


@dataclass(frozen=True)
class ResolvedBearerIdentity:
    provider: str
    provider_subject: str
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    display_name: Optional[str] = None


@dataclass(frozen=True)
class ResolvedFirebaseIdentity:
    firebase_uid: str
    email: Optional[str]
    email_verified: bool
    display_name: Optional[str]
    sign_in_provider: Optional[str]
    provider_identities: dict[str, list[str]]


@dataclass(frozen=True)
class PrivateFollowRuntimeConfig:
    runtime_env: str
    allow_static_dev_bearer_auth: bool
    allow_debug_entitlement_package: bool
    has_static_bearer_tokens_env: bool
    static_bearer_tokens: dict[str, ResolvedBearerIdentity]
    google_server_client_ids: frozenset[str]
    private_follow_bearer_secret: Optional[bytes]
    push_token_encryption_secret: Optional[bytes]
    private_follow_bearer_ttl_seconds: int
    firebase_auth_project_id: Optional[str] = None
    firebase_auth_service_account_json_path: Optional[str] = None


@dataclass(frozen=True)
class GooglePlayRuntimeConfig:
    package_name: Optional[str]
    service_account_json_path: Optional[str]
    rtdn_oidc_audience: Optional[str]
    rtdn_expected_service_account_email: Optional[str]
    rtdn_test_ingest_token: Optional[str]
    allow_test_rtdn_header_auth: bool


@dataclass(frozen=True)
class FcmRuntimeConfig:
    project_id: Optional[str]
    service_account_json_path: Optional[str]


@dataclass(frozen=True)
class PureTrackRuntimeConfig:
    app_key: Optional[str]
    api_base_url: str
    timeout_seconds: float
    insert_key: Optional[str] = None
    provider_session_encryption_secret: Optional[bytes] = None


@dataclass(frozen=True)
class PureTrackProviderLoginResult:
    result: str
    user_access: str = PURETRACK_PROVIDER_ACCESS_UNKNOWN
    provider_session_secret: Optional[str] = None
    retry_after_ms: Optional[int] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class PureTrackInsertProviderResult:
    result: str
    provider_inserted_point_count: Optional[int] = None
    retry_after_ms: Optional[int] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class PureTrackTrafficProviderResult:
    rows: Optional[list[str]] = None
    provider_row_count: int = 0
    retry_after_ms: Optional[int] = None
    error_code: Optional[str] = None


@dataclass(frozen=True)
class LiveReadRateLimitConfig:
    window_seconds: float
    global_limit: int
    per_user_limit: int
    per_ip_limit: int
    per_session_limit: int


def parse_boolean_env(name: str, raw_value: Optional[str], default: bool = False) -> bool:
    normalized = (raw_value or "").strip().lower()
    if not normalized:
        return default
    if normalized in PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES:
        return True
    if normalized in PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES:
        return False
    raise RuntimeError(
        f"{name} must be one of {sorted(PRIVATE_FOLLOW_BOOLEAN_TRUE_VALUES | PRIVATE_FOLLOW_BOOLEAN_FALSE_VALUES)}"
    )


def parse_non_negative_int_env(
    name: str,
    raw_value: Optional[str],
    default: int
) -> int:
    normalized = (raw_value or "").strip()
    if not normalized:
        return default
    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a non-negative integer") from exc
    if parsed < 0:
        raise RuntimeError(f"{name} must be a non-negative integer")
    return parsed


def parse_non_negative_float_env(
    name: str,
    raw_value: Optional[str],
    default: float
) -> float:
    normalized = (raw_value or "").strip()
    if not normalized:
        return default
    try:
        parsed = float(normalized)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise RuntimeError(f"{name} must be a non-negative number")
    return parsed


def load_live_read_rate_limit_config_from_env(
    env: Optional[dict[str, str]] = None
) -> LiveReadRateLimitConfig:
    resolved_env = os.environ if env is None else env
    return LiveReadRateLimitConfig(
        window_seconds=parse_non_negative_float_env(
            "XCPRO_LIVE_READ_RATE_LIMIT_WINDOW_SECONDS",
            resolved_env.get("XCPRO_LIVE_READ_RATE_LIMIT_WINDOW_SECONDS"),
            60.0
        ),
        global_limit=parse_non_negative_int_env(
            "XCPRO_LIVE_READ_RATE_LIMIT_GLOBAL",
            resolved_env.get("XCPRO_LIVE_READ_RATE_LIMIT_GLOBAL"),
            0
        ),
        per_user_limit=parse_non_negative_int_env(
            "XCPRO_LIVE_READ_RATE_LIMIT_PER_USER",
            resolved_env.get("XCPRO_LIVE_READ_RATE_LIMIT_PER_USER"),
            0
        ),
        per_ip_limit=parse_non_negative_int_env(
            "XCPRO_LIVE_READ_RATE_LIMIT_PER_IP",
            resolved_env.get("XCPRO_LIVE_READ_RATE_LIMIT_PER_IP"),
            0
        ),
        per_session_limit=parse_non_negative_int_env(
            "XCPRO_LIVE_READ_RATE_LIMIT_PER_SESSION",
            resolved_env.get("XCPRO_LIVE_READ_RATE_LIMIT_PER_SESSION"),
            0
        )
    )


def normalize_runtime_env(raw_value: Optional[str]) -> str:
    normalized = (raw_value or "").strip().lower()
    if not normalized:
        return RUNTIME_ENV_PROD
    aliases = {
        "dev": RUNTIME_ENV_DEV,
        "development": RUNTIME_ENV_DEV,
        "local": RUNTIME_ENV_DEV,
        "staging": RUNTIME_ENV_STAGING,
        "stage": RUNTIME_ENV_STAGING,
        "prod": RUNTIME_ENV_PROD,
        "production": RUNTIME_ENV_PROD,
    }
    resolved = aliases.get(normalized)
    if resolved is None:
        raise RuntimeError(
            "XCPRO_RUNTIME_ENV must be one of ['dev', 'staging', 'prod']"
        )
    return resolved


def parse_static_bearer_tokens(raw_value: str) -> dict[str, ResolvedBearerIdentity]:
    if not raw_value.strip():
        return {}

    parsed = json.loads(raw_value)
    if not isinstance(parsed, dict):
        raise RuntimeError("XCPRO_STATIC_BEARER_TOKENS_JSON must be a JSON object")

    token_map: dict[str, ResolvedBearerIdentity] = {}
    for raw_token, raw_identity in parsed.items():
        token = str(raw_token).strip()
        if not token:
            continue

        if isinstance(raw_identity, str):
            provider = "static"
            subject = raw_identity.strip()
            email = None
            display_name = None
        elif isinstance(raw_identity, dict):
            provider = str(raw_identity.get("provider", "static")).strip()
            subject = str(raw_identity.get("subject", "")).strip()
            email = str(raw_identity.get("email", "")).strip() or None
            display_name = str(raw_identity.get("display_name", "")).strip() or None
        else:
            raise RuntimeError("static bearer identity must be a string or object")

        if not provider or not subject:
            raise RuntimeError("static bearer identity must include provider and subject")

        token_map[token] = ResolvedBearerIdentity(
            provider=provider,
            provider_subject=subject,
            email=email,
            display_name=display_name
        )

    return token_map


def is_static_dev_bearer_auth_enabled_for_env(
    env: Optional[dict[str, str]] = None
) -> bool:
    resolved_env = os.environ if env is None else env
    return parse_boolean_env(
        "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH",
        resolved_env.get("XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH"),
        default=False
    )


def load_google_server_client_ids_from_env(
    env: Optional[dict[str, str]] = None
) -> frozenset[str]:
    resolved_env = os.environ if env is None else env
    raw_values = [
        resolved_env.get("XCPRO_GOOGLE_SERVER_CLIENT_IDS", ""),
        resolved_env.get("XCPRO_GOOGLE_SERVER_CLIENT_ID", "")
    ]
    client_ids = {
        entry.strip()
        for raw_value in raw_values
        for entry in raw_value.split(",")
        if entry.strip()
    }
    return frozenset(client_ids)


def load_private_follow_bearer_secret_from_env(
    env: Optional[dict[str, str]] = None
) -> Optional[bytes]:
    resolved_env = os.environ if env is None else env
    raw_value = resolved_env.get("XCPRO_PRIVATE_FOLLOW_BEARER_SECRET", "").strip()
    if not raw_value:
        return None
    return raw_value.encode("utf-8")


def load_push_token_encryption_secret_from_env(
    env: Optional[dict[str, str]] = None
) -> Optional[bytes]:
    resolved_env = os.environ if env is None else env
    raw_value = resolved_env.get("XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET", "").strip()
    if not raw_value:
        return None
    return raw_value.encode("utf-8")


def load_puretrack_provider_session_encryption_secret_from_env(
    env: Optional[dict[str, str]] = None
) -> Optional[bytes]:
    resolved_env = os.environ if env is None else env
    raw_value = resolved_env.get(
        "XCPRO_PURETRACK_PROVIDER_SESSION_ENCRYPTION_SECRET",
        ""
    ).strip()
    if not raw_value:
        return None
    return raw_value.encode("utf-8")


def load_private_follow_bearer_ttl_seconds_from_env(
    env: Optional[dict[str, str]] = None
) -> int:
    resolved_env = os.environ if env is None else env
    return max(
        300,
        int(
            resolved_env.get(
                "XCPRO_PRIVATE_FOLLOW_BEARER_TTL_SECONDS",
                str(DEFAULT_PRIVATE_FOLLOW_BEARER_TTL_SECONDS)
            )
        )
    )


def load_optional_trimmed_env_value(
    env: Mapping[str, str],
    name: str
) -> Optional[str]:
    value = (env.get(name, "") or "").strip()
    return value or None


def build_private_follow_runtime_config(
    env: Optional[dict[str, str]] = None
) -> PrivateFollowRuntimeConfig:
    resolved_env = os.environ if env is None else env
    runtime_env = normalize_runtime_env(resolved_env.get("XCPRO_RUNTIME_ENV"))
    allow_static_dev_bearer_auth = is_static_dev_bearer_auth_enabled_for_env(resolved_env)
    allow_debug_entitlement_package = parse_boolean_env(
        "XCPRO_ALLOW_DEBUG_ENTITLEMENT_PACKAGE",
        resolved_env.get("XCPRO_ALLOW_DEBUG_ENTITLEMENT_PACKAGE"),
        default=False
    )
    raw_static_bearer_tokens = resolved_env.get("XCPRO_STATIC_BEARER_TOKENS_JSON", "").strip()
    has_static_bearer_tokens_env = bool(raw_static_bearer_tokens)
    static_bearer_tokens = {}
    if allow_static_dev_bearer_auth and runtime_env == RUNTIME_ENV_DEV:
        static_bearer_tokens = parse_static_bearer_tokens(raw_static_bearer_tokens)
    return PrivateFollowRuntimeConfig(
        runtime_env=runtime_env,
        allow_static_dev_bearer_auth=allow_static_dev_bearer_auth,
        allow_debug_entitlement_package=allow_debug_entitlement_package,
        has_static_bearer_tokens_env=has_static_bearer_tokens_env,
        static_bearer_tokens=static_bearer_tokens,
        google_server_client_ids=load_google_server_client_ids_from_env(resolved_env),
        private_follow_bearer_secret=load_private_follow_bearer_secret_from_env(resolved_env),
        push_token_encryption_secret=load_push_token_encryption_secret_from_env(resolved_env),
        private_follow_bearer_ttl_seconds=load_private_follow_bearer_ttl_seconds_from_env(resolved_env),
        firebase_auth_project_id=load_optional_trimmed_env_value(
            resolved_env,
            "XCPRO_FIREBASE_AUTH_PROJECT_ID"
        ),
        firebase_auth_service_account_json_path=load_optional_trimmed_env_value(
            resolved_env,
            "XCPRO_FIREBASE_AUTH_SERVICE_ACCOUNT_JSON_PATH"
        )
    )


def env_value_or_none(
    resolved_env: dict[str, str],
    *names: str
) -> Optional[str]:
    for name in names:
        value = (resolved_env.get(name, "") or "").strip()
        if value:
            return value
    return None


def load_google_play_runtime_config(
    env: Optional[dict[str, str]] = None
) -> GooglePlayRuntimeConfig:
    resolved_env = os.environ if env is None else env
    return GooglePlayRuntimeConfig(
        package_name=env_value_or_none(
            resolved_env,
            "XCPRO_GOOGLE_PLAY_PACKAGE_NAME",
        ),
        service_account_json_path=env_value_or_none(
            resolved_env,
            "XCPRO_GOOGLE_PLAY_SERVICE_ACCOUNT_JSON_PATH",
        ),
        rtdn_oidc_audience=env_value_or_none(
            resolved_env,
            "XCPRO_GOOGLE_PLAY_RTDN_OIDC_AUDIENCE",
            "XCPRO_RTDN_OIDC_AUDIENCE",
        ),
        rtdn_expected_service_account_email=env_value_or_none(
            resolved_env,
            "XCPRO_GOOGLE_PLAY_RTDN_EXPECTED_SERVICE_ACCOUNT_EMAIL",
            "XCPRO_RTDN_OIDC_SERVICE_ACCOUNT_EMAIL",
        ),
        rtdn_test_ingest_token=env_value_or_none(
            resolved_env,
            "XCPRO_RTDN_INGEST_TOKEN",
        ),
        allow_test_rtdn_header_auth=parse_boolean_env(
            "XCPRO_ALLOW_TEST_RTDN_HEADER_AUTH",
            resolved_env.get("XCPRO_ALLOW_TEST_RTDN_HEADER_AUTH"),
            default=False,
        ),
    )


def load_fcm_runtime_config(
    env: Optional[dict[str, str]] = None
) -> FcmRuntimeConfig:
    resolved_env = os.environ if env is None else env
    return FcmRuntimeConfig(
        project_id=env_value_or_none(
            resolved_env,
            "XCPRO_FCM_PROJECT_ID",
            "FIREBASE_PROJECT_ID",
        ),
        service_account_json_path=env_value_or_none(
            resolved_env,
            "XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ),
    )


def normalize_puretrack_api_base_url(raw_value: Optional[str]) -> str:
    normalized = (raw_value or "").strip() or PURETRACK_DEFAULT_API_BASE_URL
    return normalized.rstrip("/")


def load_puretrack_runtime_config(
    env: Optional[dict[str, str]] = None
) -> PureTrackRuntimeConfig:
    resolved_env = os.environ if env is None else env
    return PureTrackRuntimeConfig(
        app_key=load_optional_trimmed_env_value(
            resolved_env,
            "XCPRO_PURETRACK_APP_KEY"
        ),
        api_base_url=normalize_puretrack_api_base_url(
            resolved_env.get("XCPRO_PURETRACK_API_BASE_URL")
        ),
        timeout_seconds=parse_non_negative_float_env(
            "XCPRO_PURETRACK_TIMEOUT_SECONDS",
            resolved_env.get("XCPRO_PURETRACK_TIMEOUT_SECONDS"),
            PURETRACK_DEFAULT_TIMEOUT_SECONDS
        ),
        insert_key=load_optional_trimmed_env_value(
            resolved_env,
            "XCPRO_PURETRACK_INSERT_KEY"
        ),
        provider_session_encryption_secret=(
            load_puretrack_provider_session_encryption_secret_from_env(resolved_env)
        ),
    )


def load_puretrack_traffic_evidence_enabled(
    env: Optional[dict[str, str]] = None
) -> bool:
    resolved_env = os.environ if env is None else env
    return parse_boolean_env(
        "XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED",
        resolved_env.get("XCPRO_PURETRACK_TRAFFIC_EVIDENCE_ENABLED"),
        default=False,
    )


def collect_private_follow_runtime_safety_errors(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    errors: list[str] = []
    if config.allow_static_dev_bearer_auth and config.runtime_env != RUNTIME_ENV_DEV:
        errors.append(
            "XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH is only permitted when XCPRO_RUNTIME_ENV=dev"
        )
    if config.has_static_bearer_tokens_env and config.runtime_env != RUNTIME_ENV_DEV:
        errors.append(
            "XCPRO_STATIC_BEARER_TOKENS_JSON must not be set unless XCPRO_RUNTIME_ENV=dev"
        )
    return errors


def collect_private_follow_preflight_errors(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    errors = list(collect_private_follow_runtime_safety_errors(config))
    if config.runtime_env in {RUNTIME_ENV_STAGING, RUNTIME_ENV_PROD}:
        if not config.google_server_client_ids:
            errors.append(
                "Missing XCPRO_GOOGLE_SERVER_CLIENT_ID or XCPRO_GOOGLE_SERVER_CLIENT_IDS"
            )
        if config.firebase_auth_project_id is None:
            errors.append("Missing XCPRO_FIREBASE_AUTH_PROJECT_ID")
        if config.private_follow_bearer_secret is None:
            errors.append("Missing XCPRO_PRIVATE_FOLLOW_BEARER_SECRET")
        if config.push_token_encryption_secret is None:
            errors.append("Missing XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET")
    return errors


def collect_private_follow_preflight_warnings(
    config: PrivateFollowRuntimeConfig
) -> list[str]:
    warnings: list[str] = []
    if (
        config.runtime_env == RUNTIME_ENV_DEV and
        config.has_static_bearer_tokens_env and
        not config.allow_static_dev_bearer_auth
    ):
        warnings.append(
            "XCPRO_STATIC_BEARER_TOKENS_JSON is set but ignored until XCPRO_ALLOW_DEV_STATIC_BEARER_AUTH=1"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and not config.google_server_client_ids:
        warnings.append(
            "Google exchange remains unavailable until XCPRO_GOOGLE_SERVER_CLIENT_ID(S) is configured"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and config.firebase_auth_project_id is None:
        warnings.append(
            "Firebase Auth exchange remains unavailable until XCPRO_FIREBASE_AUTH_PROJECT_ID is configured"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and config.private_follow_bearer_secret is None:
        warnings.append(
            "Issued XCPro bearer tokens remain unavailable until XCPRO_PRIVATE_FOLLOW_BEARER_SECRET is configured"
        )
    if config.runtime_env == RUNTIME_ENV_DEV and config.push_token_encryption_secret is None:
        warnings.append(
            "Push token registration remains unavailable until XCPRO_PUSH_TOKEN_ENCRYPTION_SECRET is configured"
        )
    if config.runtime_env == RUNTIME_ENV_PROD and config.allow_debug_entitlement_package:
        warnings.append(
            "Debug entitlement package com.trust3.xcpro.debug is accepted in prod; disable before public release"
        )
    return warnings


def build_private_follow_preflight_report(
    config: Optional[PrivateFollowRuntimeConfig] = None
) -> dict[str, Any]:
    resolved_config = config or PRIVATE_FOLLOW_RUNTIME_CONFIG
    errors = collect_private_follow_preflight_errors(resolved_config)
    warnings = collect_private_follow_preflight_warnings(resolved_config)
    return {
        "ok": not errors,
        "runtime_env": resolved_config.runtime_env,
        "allow_static_dev_bearer_auth": resolved_config.allow_static_dev_bearer_auth,
        "allow_debug_entitlement_package": resolved_config.allow_debug_entitlement_package,
        "has_static_bearer_tokens_env": resolved_config.has_static_bearer_tokens_env,
        "active_static_bearer_tokens": len(resolved_config.static_bearer_tokens),
        "has_google_server_client_ids": bool(resolved_config.google_server_client_ids),
        "has_firebase_auth_project_id": resolved_config.firebase_auth_project_id is not None,
        "has_firebase_auth_service_account_json_path": (
            resolved_config.firebase_auth_service_account_json_path is not None
        ),
        "has_private_follow_bearer_secret": resolved_config.private_follow_bearer_secret is not None,
        "has_push_token_encryption_secret": resolved_config.push_token_encryption_secret is not None,
        "private_follow_bearer_ttl_seconds": resolved_config.private_follow_bearer_ttl_seconds,
        "errors": errors,
        "warnings": warnings,
    }


def assert_private_follow_runtime_safety(
    config: PrivateFollowRuntimeConfig
) -> None:
    errors = collect_private_follow_runtime_safety_errors(config)
    if errors:
        raise RuntimeError("Unsafe private-follow auth configuration: " + "; ".join(errors))


PRIVATE_FOLLOW_RUNTIME_CONFIG = build_private_follow_runtime_config()
assert_private_follow_runtime_safety(PRIVATE_FOLLOW_RUNTIME_CONFIG)
STATIC_BEARER_TOKENS = PRIVATE_FOLLOW_RUNTIME_CONFIG.static_bearer_tokens
GOOGLE_SERVER_CLIENT_IDS = PRIVATE_FOLLOW_RUNTIME_CONFIG.google_server_client_ids
PRIVATE_FOLLOW_BEARER_SECRET = PRIVATE_FOLLOW_RUNTIME_CONFIG.private_follow_bearer_secret
PUSH_TOKEN_ENCRYPTION_SECRET = PRIVATE_FOLLOW_RUNTIME_CONFIG.push_token_encryption_secret
PRIVATE_FOLLOW_BEARER_TTL_SECONDS = PRIVATE_FOLLOW_RUNTIME_CONFIG.private_follow_bearer_ttl_seconds
GOOGLE_PLAY_RUNTIME_CONFIG = load_google_play_runtime_config()
FCM_RUNTIME_CONFIG = load_fcm_runtime_config()
PURETRACK_RUNTIME_CONFIG = load_puretrack_runtime_config()
PURETRACK_TRAFFIC_EVIDENCE_ENABLED = load_puretrack_traffic_evidence_enabled()
GOOGLE_PLAY_RTDN_INGEST_TOKEN = GOOGLE_PLAY_RUNTIME_CONFIG.rtdn_test_ingest_token
GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH = (
    GOOGLE_PLAY_RUNTIME_CONFIG.allow_test_rtdn_header_auth
)


def parse_retry_after_ms(raw_value: Optional[str]) -> Optional[int]:
    if raw_value is None:
        return None
    seconds = raw_value.strip().split(".", 1)[0]
    parsed = int(seconds) if seconds.isdigit() else None
    if parsed is None:
        return None
    return parsed * 1000


def coerce_non_negative_int(raw_value: Any, field_name: str) -> int:
    if isinstance(raw_value, bool):
        raise ValueError(f"{field_name} must be a non-negative integer")
    value = int(raw_value)
    if value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


class HttpPureTrackProviderClient:
    def login(
        self,
        email: str,
        password: str,
        config: PureTrackRuntimeConfig
    ) -> PureTrackProviderLoginResult:
        app_key = trim_to_none(config.app_key)
        if app_key is None:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_APP_KEY_UNCONFIGURED,
                user_access=PURETRACK_PROVIDER_ACCESS_UNKNOWN,
                error_code=ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
            )

        try:
            response = httpx.post(
                f"{config.api_base_url}/api/login",
                data={
                    "key": app_key,
                    "email": email,
                    "password": password,
                },
                headers={"Accept": "application/json"},
                timeout=config.timeout_seconds,
            )
        except httpx.TimeoutException:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )
        except httpx.HTTPError:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )

        if response.status_code == 429:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_RATE_LIMITED,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                retry_after_ms=parse_retry_after_ms(response.headers.get("Retry-After")),
                error_code=ErrorCode.PURETRACK_RATE_LIMITED,
            )
        if response.status_code in {401, 403}:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_AUTH_REJECTED,
                user_access=PURETRACK_PROVIDER_ACCESS_NONE,
                error_code="auth_rejected",
            )
        if 500 <= response.status_code <= 599:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_PROVIDER_UNAVAILABLE,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )
        if response.status_code != 200:
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_ERROR,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                error_code="provider_error",
            )

        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("PureTrack login response must be a JSON object")
            provider_session_secret = str(payload.get("access_" + "token", "")).strip()
            provider_is_premium = payload["pro"]
            if not provider_session_secret or not isinstance(provider_is_premium, bool):
                raise ValueError("PureTrack login response is missing required fields")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return PureTrackProviderLoginResult(
                result=PURETRACK_CONNECT_RESULT_ERROR,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                error_code="malformed_provider_response",
            )

        return PureTrackProviderLoginResult(
            result=PURETRACK_CONNECT_RESULT_CONNECTED,
            user_access=(
                PURETRACK_PROVIDER_ACCESS_PREMIUM
                if provider_is_premium
                else PURETRACK_PROVIDER_ACCESS_FREE
            ),
            provider_session_secret=provider_session_secret,
        )


PURETRACK_PROVIDER_CLIENT = HttpPureTrackProviderClient()


class HttpPureTrackInsertClient:
    def publish(
        self,
        trackers: list[dict[str, Any]],
        config: PureTrackRuntimeConfig,
    ) -> PureTrackInsertProviderResult:
        insert_key = trim_to_none(config.insert_key)
        if insert_key is None:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                error_code=ErrorCode.PURETRACK_INSERT_KEY_UNCONFIGURED,
            )

        upstream_trackers = []
        for tracker in trackers:
            upstream_tracker = dict(tracker)
            upstream_tracker["key"] = insert_key
            upstream_trackers.append(upstream_tracker)

        try:
            response = httpx.post(
                f"{config.api_base_url}/api/insert",
                json=upstream_trackers,
                headers={"Accept": "application/json"},
                timeout=config.timeout_seconds,
            )
        except httpx.TimeoutException:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )
        except httpx.HTTPError:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )

        if response.status_code == 429:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                retry_after_ms=parse_retry_after_ms(response.headers.get("Retry-After")),
                error_code=ErrorCode.PURETRACK_RATE_LIMITED,
            )
        if 500 <= response.status_code <= 599:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )
        if response.status_code != 200:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_REJECTED,
                error_code=ErrorCode.PURETRACK_INSERT_REJECTED,
            )

        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("PureTrack insert response must be a JSON object")
            received_count = coerce_non_negative_int(
                payload.get("total_points_recieved", payload.get("total_points_received")),
                "total_points_recieved"
            )
            inserted_count = coerce_non_negative_int(
                payload.get("total_points_inserted"),
                "total_points_inserted"
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_RETRYABLE_FAILURE,
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )

        if inserted_count == received_count:
            return PureTrackInsertProviderResult(
                result=PURETRACK_INSERT_RESULT_ACCEPTED,
                provider_inserted_point_count=inserted_count,
            )
        return PureTrackInsertProviderResult(
            result=PURETRACK_INSERT_RESULT_PARTIAL_RETRY,
            provider_inserted_point_count=inserted_count,
        )


PURETRACK_INSERT_CLIENT = HttpPureTrackInsertClient()


def puretrack_traffic_provider_failure_for_http_status(
    status_code: int,
    retry_after_ms: Optional[int] = None,
) -> PureTrackTrafficProviderResult:
    if status_code == 429:
        return PureTrackTrafficProviderResult(
            retry_after_ms=retry_after_ms,
            error_code=ErrorCode.PURETRACK_RATE_LIMITED,
        )
    if status_code == 401:
        return PureTrackTrafficProviderResult(
            error_code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
        )
    if status_code == 403:
        return PureTrackTrafficProviderResult(
            error_code=ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
        )
    if status_code == 422:
        return PureTrackTrafficProviderResult(
            error_code=ErrorCode.PURETRACK_TRAFFIC_REJECTED,
        )
    if 500 <= status_code <= 599:
        return PureTrackTrafficProviderResult(
            error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
        )
    return PureTrackTrafficProviderResult(
        error_code=ErrorCode.PURETRACK_TRAFFIC_REJECTED,
    )


class HttpPureTrackTrafficClient:
    def fetch(
        self,
        bbox: "PureTrackTrafficBbox",
        filters: "PureTrackTrafficFilters",
        provider_session_secret: str,
        config: PureTrackRuntimeConfig,
    ) -> PureTrackTrafficProviderResult:
        app_key = trim_to_none(config.app_key)
        if app_key is None:
            return PureTrackTrafficProviderResult(
                error_code=ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
            )

        params: dict[str, Any] = {
            "key": app_key,
            "lat1": bbox.north,
            "long1": bbox.east,
            "lat2": bbox.south,
            "long2": bbox.west,
            "cat": filters.category,
            "t": math.ceil(filters.maxAgeSeconds / 60.0),
        }
        if filters.objectTypeIds:
            params["o"] = ",".join(str(value) for value in filters.objectTypeIds)

        try:
            response = httpx.post(
                f"{config.api_base_url}/api/traffic",
                data=params,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {provider_session_secret}",
                },
                timeout=config.timeout_seconds,
            )
        except httpx.TimeoutException:
            return PureTrackTrafficProviderResult(
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )
        except httpx.HTTPError:
            return PureTrackTrafficProviderResult(
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )

        if response.status_code != 200:
            return puretrack_traffic_provider_failure_for_http_status(
                response.status_code,
                parse_retry_after_ms(response.headers.get("Retry-After")),
            )

        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("PureTrack traffic response must be a JSON object")
            provider_http_code = coerce_non_negative_int(
                payload.get("http_code", 200),
                "http_code",
            )
            if payload.get("success") is not True or provider_http_code != 200:
                return puretrack_traffic_provider_failure_for_http_status(
                    provider_http_code,
                    parse_retry_after_ms(response.headers.get("Retry-After")),
                )
            raw_rows = payload.get("data")
            if not isinstance(raw_rows, list) or not all(
                isinstance(row, str) for row in raw_rows
            ):
                raise ValueError("PureTrack traffic data must be a string array")
        except (TypeError, ValueError, json.JSONDecodeError):
            return PureTrackTrafficProviderResult(
                error_code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
            )

        return PureTrackTrafficProviderResult(
            rows=raw_rows,
            provider_row_count=len(raw_rows),
        )


PURETRACK_TRAFFIC_CLIENT = HttpPureTrackTrafficClient()
_puretrack_traffic_cache: dict[str, dict[str, Any]] = {}
_puretrack_traffic_cache_lock = threading.Lock()
_puretrack_traffic_rate_limits: dict[str, tuple[float, int]] = {}
_puretrack_traffic_rate_limit_lock = threading.Lock()


def base64url_encode(raw_bytes: bytes) -> str:
    return base64.urlsafe_b64encode(raw_bytes).decode("ascii").rstrip("=")


def base64url_decode(raw_value: str) -> bytes:
    padding = "=" * (-len(raw_value) % 4)
    return base64.urlsafe_b64decode(f"{raw_value}{padding}")


def issue_private_follow_bearer(identity: ResolvedBearerIdentity) -> str:
    if PRIVATE_FOLLOW_BEARER_SECRET is None:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.AUTH_UNAVAILABLE,
            detail="private-follow bearer secret is not configured"
        )

    issued_at = int(utcnow().timestamp())
    expires_at = issued_at + PRIVATE_FOLLOW_BEARER_TTL_SECONDS
    payload = {
        "v": PRIVATE_FOLLOW_BEARER_VERSION,
        "provider": identity.provider,
        "sub": identity.provider_subject,
        "email": identity.email,
        "display_name": identity.display_name,
        "iat": issued_at,
        "exp": expires_at
    }
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    payload_segment = base64url_encode(payload_json)
    signature = hmac.new(
        PRIVATE_FOLLOW_BEARER_SECRET,
        payload_segment.encode("ascii"),
        hashlib.sha256
    ).digest()
    return f"xcps.{payload_segment}.{base64url_encode(signature)}"


def verify_private_follow_bearer(token: str) -> Optional[ResolvedBearerIdentity]:
    if PRIVATE_FOLLOW_BEARER_SECRET is None or not token.startswith("xcps."):
        return None

    parts = token.split(".")
    if len(parts) != 3 or parts[0] != "xcps":
        return None

    payload_segment = parts[1].strip()
    signature_segment = parts[2].strip()
    if not payload_segment or not signature_segment:
        return None

    expected_signature = base64url_encode(
        hmac.new(
            PRIVATE_FOLLOW_BEARER_SECRET,
            payload_segment.encode("ascii"),
            hashlib.sha256
        ).digest()
    )
    if not secrets.compare_digest(signature_segment, expected_signature):
        return None

    try:
        payload = json.loads(base64url_decode(payload_segment).decode("utf-8"))
    except Exception:
        return None

    if payload.get("v") != PRIVATE_FOLLOW_BEARER_VERSION:
        return None

    expires_at = int(payload.get("exp", 0))
    if expires_at <= int(utcnow().timestamp()):
        return None

    provider = str(payload.get("provider", "")).strip()
    provider_subject = str(payload.get("sub", "")).strip()
    if not provider or not provider_subject:
        return None

    def optional_string_claim(raw_value: Any) -> Optional[str]:
        if not isinstance(raw_value, str):
            return None
        return raw_value.strip() or None

    email = optional_string_claim(payload.get("email"))
    display_name = optional_string_claim(payload.get("display_name"))
    return ResolvedBearerIdentity(
        provider=provider,
        provider_subject=provider_subject,
        email=email,
        display_name=display_name
    )


PRIVATE_FOLLOW_BEARER_TOKEN_VERIFIER = verify_private_follow_bearer


def verify_google_id_token_for_exchange(token: str) -> Optional[ResolvedBearerIdentity]:
    if google_id_token is None or google_requests is None or not GOOGLE_SERVER_CLIENT_IDS:
        return None

    try:
        decoded_token = google_id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            audience=None
        )
    except Exception:
        return None

    audience = str(decoded_token.get("aud", "")).strip()
    if audience not in GOOGLE_SERVER_CLIENT_IDS:
        return None

    provider_subject = str(decoded_token.get("sub") or "").strip()
    if not provider_subject:
        return None

    issuer = str(decoded_token.get("iss", "")).strip()
    if issuer not in {"accounts.google.com", "https://accounts.google.com"}:
        return None

    email = str(decoded_token.get("email", "")).strip() or None
    display_name = str(decoded_token.get("name", "")).strip() or None
    return ResolvedBearerIdentity(
        provider="google",
        provider_subject=provider_subject,
        email=email,
        display_name=display_name
    )


GOOGLE_ID_TOKEN_VERIFIER = verify_google_id_token_for_exchange

FIREBASE_AUTH_APP_NAME = "xcpro-firebase-auth"
_FIREBASE_AUTH_APP = None
_FIREBASE_AUTH_APP_CONFIG_KEY: Optional[tuple[str, Optional[str]]] = None
_FIREBASE_AUTH_APP_LOCK = threading.Lock()


def firebase_auth_unavailable_exception(detail: str) -> ApiHTTPException:
    return ApiHTTPException(
        status_code=503,
        code=ErrorCode.AUTH_UNAVAILABLE,
        detail=detail
    )


def invalid_firebase_id_token_exception(
    status_code: int = 401,
    detail: str = "invalid Firebase ID token"
) -> ApiHTTPException:
    return ApiHTTPException(
        status_code=status_code,
        code=ErrorCode.INVALID_FIREBASE_ID_TOKEN,
        detail=detail
    )


def firebase_exception_matches(exc: Exception, module: Any, names: tuple[str, ...]) -> bool:
    if module is None:
        return False
    for name in names:
        exception_type = getattr(module, name, None)
        if isinstance(exception_type, type) and isinstance(exc, exception_type):
            return True
    return False


def is_firebase_auth_unavailable_exception(exc: Exception) -> bool:
    if GoogleAuthError is not None and isinstance(exc, GoogleAuthError):
        return True
    return (
        firebase_exception_matches(exc, firebase_auth, ("CertificateFetchError",)) or
        firebase_exception_matches(
            exc,
            firebase_exceptions,
            ("DeadlineExceededError", "InternalError", "UnavailableError")
        )
    )


def initialize_firebase_auth_app_for_exchange(
    project_id: str,
    service_account_json_path: Optional[str]
):
    if firebase_admin is None or firebase_auth is None or firebase_credentials is None:
        raise firebase_auth_unavailable_exception("Firebase Admin SDK is not installed")

    global _FIREBASE_AUTH_APP
    global _FIREBASE_AUTH_APP_CONFIG_KEY

    config_key = (project_id, service_account_json_path)
    with _FIREBASE_AUTH_APP_LOCK:
        if _FIREBASE_AUTH_APP is not None and _FIREBASE_AUTH_APP_CONFIG_KEY == config_key:
            return _FIREBASE_AUTH_APP

        if _FIREBASE_AUTH_APP is not None:
            try:
                firebase_admin.delete_app(_FIREBASE_AUTH_APP)
            except Exception:
                pass
            _FIREBASE_AUTH_APP = None
            _FIREBASE_AUTH_APP_CONFIG_KEY = None

        try:
            if service_account_json_path:
                credential = firebase_credentials.Certificate(service_account_json_path)
            else:
                credential = firebase_credentials.ApplicationDefault()
            app_instance = firebase_admin.initialize_app(
                credential,
                {"projectId": project_id},
                name=FIREBASE_AUTH_APP_NAME
            )
        except ValueError:
            try:
                app_instance = firebase_admin.get_app(FIREBASE_AUTH_APP_NAME)
            except Exception as exc:
                raise firebase_auth_unavailable_exception(
                    "Firebase Auth verifier is unavailable"
                ) from exc
        except Exception as exc:
            raise firebase_auth_unavailable_exception(
                "Firebase Auth verifier is unavailable"
            ) from exc

        _FIREBASE_AUTH_APP = app_instance
        _FIREBASE_AUTH_APP_CONFIG_KEY = config_key
        return app_instance


def get_firebase_auth_app_for_exchange():
    project_id = PRIVATE_FOLLOW_RUNTIME_CONFIG.firebase_auth_project_id
    if project_id is None:
        raise firebase_auth_unavailable_exception(
            "Firebase Auth project ID is not configured"
        )

    return initialize_firebase_auth_app_for_exchange(
        project_id,
        PRIVATE_FOLLOW_RUNTIME_CONFIG.firebase_auth_service_account_json_path
    )


def firebase_decoded_token_matches_project(
    decoded_token: MappingABC,
    project_id: str
) -> bool:
    audience = str(decoded_token.get("aud", "") or "").strip()
    issuer = str(decoded_token.get("iss", "") or "").strip()
    return (
        audience == project_id and
        issuer == f"https://securetoken.google.com/{project_id}"
    )


def extract_firebase_provider_identities(decoded_token: MappingABC) -> dict[str, list[str]]:
    firebase_claim = decoded_token.get("firebase")
    if not isinstance(firebase_claim, MappingABC):
        return {}

    raw_identities = firebase_claim.get("identities")
    if not isinstance(raw_identities, MappingABC):
        return {}

    provider_identities: dict[str, list[str]] = {}
    for raw_provider_id, raw_values in raw_identities.items():
        provider_id = str(raw_provider_id or "").strip()
        if not provider_id:
            continue

        if isinstance(raw_values, (list, tuple, set)):
            values = [
                str(raw_value or "").strip()
                for raw_value in raw_values
                if str(raw_value or "").strip()
            ]
        else:
            value = str(raw_values or "").strip()
            values = [value] if value else []

        if values:
            provider_identities[provider_id] = values

    return provider_identities


def extract_firebase_sign_in_provider(decoded_token: MappingABC) -> Optional[str]:
    firebase_claim = decoded_token.get("firebase")
    if not isinstance(firebase_claim, MappingABC):
        return None
    return str(firebase_claim.get("sign_in_provider") or "").strip() or None


def verify_firebase_id_token_for_exchange(token: str) -> ResolvedFirebaseIdentity:
    trimmed_token = (token or "").strip()
    if not trimmed_token:
        raise invalid_firebase_id_token_exception(
            status_code=422,
            detail="firebase_id_token is required"
        )

    project_id = PRIVATE_FOLLOW_RUNTIME_CONFIG.firebase_auth_project_id
    app_instance = get_firebase_auth_app_for_exchange()

    try:
        decoded_token = firebase_auth.verify_id_token(
            trimmed_token,
            app=app_instance,
            check_revoked=True
        )
    except Exception as exc:
        if is_firebase_auth_unavailable_exception(exc):
            raise firebase_auth_unavailable_exception(
                "Firebase Auth verifier is unavailable"
            ) from exc
        raise invalid_firebase_id_token_exception() from exc

    if not isinstance(decoded_token, MappingABC):
        raise invalid_firebase_id_token_exception()

    if project_id is None or not firebase_decoded_token_matches_project(decoded_token, project_id):
        raise invalid_firebase_id_token_exception()

    firebase_uid = str(decoded_token.get("uid") or "").strip()
    if not firebase_uid:
        raise invalid_firebase_id_token_exception()

    return ResolvedFirebaseIdentity(
        firebase_uid=firebase_uid,
        email=str(decoded_token.get("email") or "").strip() or None,
        email_verified=decoded_token.get("email_verified") is True,
        display_name=str(decoded_token.get("name") or "").strip() or None,
        sign_in_provider=extract_firebase_sign_in_provider(decoded_token),
        provider_identities=extract_firebase_provider_identities(decoded_token)
    )


FIREBASE_ID_TOKEN_VERIFIER = verify_firebase_id_token_for_exchange


def reject_monotonic_position_fields(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    monotonic_fields = sorted(POSITION_MONOTONIC_FIELD_NAMES.intersection(payload.keys()))
    if monotonic_fields:
        field_list = ", ".join(monotonic_fields)
        raise ValueError(
            f"client monotonic time is not accepted on the wire ({field_list})"
        )
    return payload


@app.exception_handler(ApiHTTPException)
def api_http_exception_handler(_request: Request, exc: ApiHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.code,
            "detail": exc.detail
        },
        headers=exc.headers
    )


@app.exception_handler(RequestValidationError)
def request_validation_exception_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "code": ErrorCode.VALIDATION_ERROR,
            "detail": jsonable_encoder(exc.errors())
        }
    )


def generate_share_code(length=8):
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def generate_write_token():
    return secrets.token_urlsafe(24)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def hash_purchase_token(purchase_token: str) -> str:
    return hashlib.sha256(purchase_token.encode("utf-8")).hexdigest()


def hash_push_token(push_token: str) -> str:
    return hashlib.sha256(push_token.encode("utf-8")).hexdigest()


def derive_push_token_fernet_key(secret: bytes) -> bytes:
    return base64.urlsafe_b64encode(
        hmac.new(
            secret,
            PUSH_TOKEN_FERNET_KEY_CONTEXT,
            hashlib.sha256
        ).digest()
    )


def build_push_token_fernet() -> Fernet:
    if PUSH_TOKEN_ENCRYPTION_SECRET is None:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PUSH_TOKEN_ENCRYPTION_UNAVAILABLE,
            detail="push token encryption secret is not configured"
        )
    return Fernet(derive_push_token_fernet_key(PUSH_TOKEN_ENCRYPTION_SECRET))


def encrypt_push_token(push_token: str) -> str:
    return build_push_token_fernet().encrypt(push_token.encode("utf-8")).decode("ascii")


def decrypt_push_token(push_token_ciphertext: str) -> str:
    try:
        return build_push_token_fernet().decrypt(
            push_token_ciphertext.encode("ascii")
        ).decode("utf-8")
    except (InvalidToken, UnicodeDecodeError):
        raise ValueError("push token ciphertext is invalid")


def derive_puretrack_provider_session_fernet_key(secret: bytes) -> bytes:
    return base64.urlsafe_b64encode(
        hmac.new(
            secret,
            PURETRACK_PROVIDER_SESSION_FERNET_KEY_CONTEXT,
            hashlib.sha256
        ).digest()
    )


def build_puretrack_provider_session_fernet(
    config: Optional[PureTrackRuntimeConfig] = None
) -> Fernet:
    resolved_config = config or PURETRACK_RUNTIME_CONFIG
    if resolved_config.provider_session_encryption_secret is None:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            detail="PureTrack provider session encryption secret is not configured"
        )
    return Fernet(
        derive_puretrack_provider_session_fernet_key(
            resolved_config.provider_session_encryption_secret
        )
    )


def encrypt_puretrack_provider_session_secret(
    provider_session_secret: str,
    config: Optional[PureTrackRuntimeConfig] = None,
) -> str:
    return build_puretrack_provider_session_fernet(config).encrypt(
        provider_session_secret.encode("utf-8")
    ).decode("ascii")


def decrypt_puretrack_provider_session_secret(
    provider_session_ciphertext: str,
    config: Optional[PureTrackRuntimeConfig] = None,
) -> str:
    try:
        return build_puretrack_provider_session_fernet(config).decrypt(
            provider_session_ciphertext.encode("ascii")
        ).decode("utf-8")
    except (InvalidToken, UnicodeDecodeError):
        raise ValueError("PureTrack provider session ciphertext is invalid")


def to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def to_epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_number(value, field_name: str, code: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_name} must be numeric")


def validate_lat_lon(lat: float, lon: float, field_prefix: str, code: str) -> None:
    if lat < -90 or lat > 90:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_prefix}.lat out of range")
    if lon < -180 or lon > 180:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_prefix}.lon out of range")


def validate_radius(radius_value, field_name: str, code: str) -> None:
    radius = parse_number(radius_value, field_name, code)
    if radius <= 0 or radius > MAX_TASK_RADIUS_M:
        raise ApiHTTPException(status_code=400, code=code, detail=f"{field_name} out of range")


@dataclass(frozen=True)
class LiveSpectatorStatsPoint:
    lat: float
    lon: float
    altitude_msl_meters: float
    timestamp: datetime


@dataclass(frozen=True)
class LiveSpectatorStatsSnapshot:
    first_position_at: datetime
    last_position_at: datetime
    position_count: int
    highest_altitude_msl_meters: float
    distance_flown_meters: float
    current_climb_sink_ms: Optional[float]
    best_short_window_climb_ms: Optional[float]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def finite_float(value: float) -> bool:
    return isinstance(value, (float, int)) and math.isfinite(float(value))


def valid_spectator_stats_point(point: LiveSpectatorStatsPoint) -> bool:
    if not finite_float(point.lat) or not finite_float(point.lon):
        return False
    if point.lat < -90.0 or point.lat > 90.0:
        return False
    if point.lon < -180.0 or point.lon > 180.0:
        return False
    if not finite_float(point.altitude_msl_meters):
        return False
    return isinstance(point.timestamp, datetime)


def normalized_spectator_stats_points(
    points: list[LiveSpectatorStatsPoint]
) -> list[LiveSpectatorStatsPoint]:
    valid_points = [
        LiveSpectatorStatsPoint(
            lat=float(point.lat),
            lon=float(point.lon),
            altitude_msl_meters=float(point.altitude_msl_meters),
            timestamp=to_utc_naive(point.timestamp)
        )
        for point in points
        if valid_spectator_stats_point(point)
    ]
    return sorted(valid_points, key=lambda point: point.timestamp)


def spectator_stats_delta_seconds(
    start_at: datetime,
    end_at: datetime
) -> float:
    return (end_at - start_at).total_seconds()


def spectator_stats_climb_rate_ms(
    start: LiveSpectatorStatsPoint,
    end: LiveSpectatorStatsPoint,
    min_delta_seconds: float,
    max_delta_seconds: float
) -> Optional[float]:
    delta_seconds = spectator_stats_delta_seconds(start.timestamp, end.timestamp)
    if delta_seconds < min_delta_seconds or delta_seconds > max_delta_seconds:
        return None
    return (end.altitude_msl_meters - start.altitude_msl_meters) / delta_seconds


def spectator_stats_distance_meters(
    points: list[LiveSpectatorStatsPoint]
) -> float:
    distance_meters = 0.0
    for previous, current in zip(points, points[1:]):
        if spectator_stats_delta_seconds(previous.timestamp, current.timestamp) <= 0.0:
            continue
        distance_meters += haversine_m(
            previous.lat,
            previous.lon,
            current.lat,
            current.lon
        )
    return distance_meters


def spectator_stats_current_climb_sink_ms(
    points: list[LiveSpectatorStatsPoint]
) -> Optional[float]:
    if len(points) < 2:
        return None
    return spectator_stats_climb_rate_ms(
        start=points[-2],
        end=points[-1],
        min_delta_seconds=0.000001,
        max_delta_seconds=SPECTATOR_STATS_CURRENT_CLIMB_MAX_DELTA_SECONDS
    )


def spectator_stats_best_short_window_climb_ms(
    points: list[LiveSpectatorStatsPoint]
) -> Optional[float]:
    best_climb_ms: Optional[float] = None
    for end_index, end in enumerate(points):
        for start in points[:end_index]:
            climb_ms = spectator_stats_climb_rate_ms(
                start=start,
                end=end,
                min_delta_seconds=SPECTATOR_STATS_BEST_CLIMB_MIN_WINDOW_SECONDS,
                max_delta_seconds=SPECTATOR_STATS_BEST_CLIMB_MAX_WINDOW_SECONDS
            )
            if climb_ms is None or climb_ms <= 0.0:
                continue
            if best_climb_ms is None or climb_ms > best_climb_ms:
                best_climb_ms = climb_ms
    return best_climb_ms


def build_live_spectator_stats_snapshot(
    points: list[LiveSpectatorStatsPoint]
) -> Optional[LiveSpectatorStatsSnapshot]:
    normalized_points = normalized_spectator_stats_points(points)
    if not normalized_points:
        return None

    return LiveSpectatorStatsSnapshot(
        first_position_at=normalized_points[0].timestamp,
        last_position_at=normalized_points[-1].timestamp,
        position_count=len(normalized_points),
        highest_altitude_msl_meters=max(
            point.altitude_msl_meters
            for point in normalized_points
        ),
        distance_flown_meters=spectator_stats_distance_meters(normalized_points),
        current_climb_sink_ms=spectator_stats_current_climb_sink_ms(normalized_points),
        best_short_window_climb_ms=spectator_stats_best_short_window_climb_ms(
            normalized_points
        )
    )


class LiveSession(Base):
    __tablename__ = "live_sessions"
    __table_args__ = (
        CheckConstraint(
            "visibility IN ('off', 'followers', 'public')",
            name="ck_live_sessions_visibility"
        ),
        Index("ix_live_sessions_owner_status", "owner_user_id", "status"),
    )

    id = Column(String, primary_key=True, index=True)
    share_code = Column(String, unique=True, index=True, nullable=False)
    owner_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=True)
    visibility = Column(
        String(24),
        nullable=False,
        default=LIVE_VISIBILITY_PUBLIC,
        server_default=text("'public'")
    )
    created_at = Column(DateTime, nullable=False)
    status = Column(
    String(20),
    nullable=False,
    default="active",
    server_default=text("'active'"))
    last_position_at = Column(DateTime, nullable=True)
    ended_at = Column(DateTime, nullable=True)
    write_token_hash = Column(String(64), nullable=True)


class LivePosition(Base):
    __tablename__ = "live_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, index=True, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    alt = Column(Float, nullable=False)
    agl_meters = Column(Float, nullable=True)
    speed = Column(Float, nullable=False)
    heading = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)


class LiveTask(Base):
    __tablename__ = "live_tasks"

    id = Column(String, primary_key=True, index=True)
    session_id = Column(String, unique=True, index=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    current_revision = Column(Integer, nullable=False)


class LiveTaskRevision(Base):
    __tablename__ = "live_task_revisions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String, index=True, nullable=False)
    revision = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    payload_json = Column(Text, nullable=False)


class LiveSessionViewer(Base):
    __tablename__ = "live_session_viewers"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "viewer_user_id",
            name="uq_live_session_viewers_session_viewer"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    session_id = Column(String, ForeignKey("live_sessions.id"), index=True, nullable=False)
    viewer_user_id = Column(String, ForeignKey("users.id"), nullable=False)
    first_seen_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)


class LiveSessionSpectatorStats(Base):
    __tablename__ = "live_session_spectator_stats"

    session_id = Column(String, ForeignKey("live_sessions.id"), primary_key=True, index=True)
    first_position_at = Column(DateTime, nullable=False)
    last_position_at = Column(DateTime, nullable=False)
    position_count = Column(Integer, nullable=False)
    highest_altitude_msl_meters = Column(Float, nullable=False)
    distance_flown_meters = Column(Float, nullable=False)
    current_climb_sink_ms = Column(Float, nullable=True)
    best_short_window_climb_ms = Column(Float, nullable=True)
    updated_at = Column(DateTime, nullable=False)


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class AuthIdentity(Base):
    __tablename__ = "auth_identities"
    __table_args__ = (
        UniqueConstraint("provider", "provider_subject", name="uq_auth_identities_provider_subject"),
    )

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    provider = Column(String(40), nullable=False)
    provider_subject = Column(String(255), nullable=False)
    provider_email = Column(String(255), nullable=True)
    provider_email_verified = Column(Boolean, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    last_seen_at = Column(DateTime, nullable=False)


class PilotProfile(Base):
    __tablename__ = "pilot_profiles"
    __table_args__ = (
        UniqueConstraint("handle_normalized", name="uq_pilot_profiles_handle_normalized"),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    handle = Column(String(24), nullable=True)
    handle_normalized = Column(String(24), index=True, nullable=True)
    display_name = Column(String(80), nullable=True)
    comp_number = Column(String(24), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class PrivacySetting(Base):
    __tablename__ = "privacy_settings"
    __table_args__ = (
        CheckConstraint(
            "discoverability IN ('searchable', 'hidden')",
            name="ck_privacy_settings_discoverability"
        ),
        CheckConstraint(
            "follow_policy IN ('approval_required', 'auto_approve', 'closed')",
            name="ck_privacy_settings_follow_policy"
        ),
        CheckConstraint(
            "default_live_visibility IN ('off', 'followers', 'public')",
            name="ck_privacy_settings_default_live_visibility"
        ),
        CheckConstraint(
            "connection_list_visibility IN ('owner_only', 'mutuals_only', 'public')",
            name="ck_privacy_settings_connection_list_visibility"
        ),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    discoverability = Column(String(24), nullable=False)
    follow_policy = Column(String(32), nullable=False)
    default_live_visibility = Column(String(24), nullable=False)
    connection_list_visibility = Column(String(24), nullable=False)
    social_notifications_enabled = Column(Boolean, nullable=False, default=True)
    live_notifications_enabled = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class AccountEntitlementSnapshot(Base):
    __tablename__ = "account_entitlement_snapshots"

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    tier = Column(String(24), nullable=False)
    billing_period = Column(String(24), nullable=False)
    status = Column(String(40), nullable=False)
    source = Column(String(40), nullable=False)
    verification_state = Column(String(40), nullable=False)
    product_id = Column(String(80), nullable=True)
    base_plan_id = Column(String(80), nullable=True)
    expiry_time_ms = Column(BigInteger, nullable=True)
    auto_renewing = Column(Boolean, nullable=True)
    will_lose_access_at_ms = Column(BigInteger, nullable=True)
    verified_at_ms = Column(BigInteger, nullable=True)
    fetched_at_ms = Column(BigInteger, nullable=False)
    valid_until_ms = Column(BigInteger, nullable=True)
    stale_after_ms = Column(BigInteger, nullable=True)
    hard_refresh_after_ms = Column(BigInteger, nullable=True)
    recovery_action = Column(String(40), nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class PureTrackProviderSession(Base):
    __tablename__ = "puretrack_provider_sessions"
    __table_args__ = (
        CheckConstraint(
            "user_access IN ('UNKNOWN', 'NONE', 'FREE', 'PREMIUM', 'ERROR')",
            name="ck_puretrack_provider_sessions_user_access",
        ),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    provider_session_hash = Column(String(64), nullable=True, index=True)
    provider_session_ciphertext = Column(Text, nullable=True)
    user_access = Column(String(24), nullable=False)
    account_label = Column(String(320), nullable=True)
    verified_at_ms = Column(BigInteger, nullable=True)
    valid_until_ms = Column(BigInteger, nullable=True)
    error_code = Column(String(80), nullable=True)
    retry_after_ms = Column(BigInteger, nullable=True)
    audit_id = Column(String(80), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class BillingGooglePurchase(Base):
    __tablename__ = "billing_google_purchases"
    __table_args__ = (
        UniqueConstraint(
            "purchase_token_hash",
            name="uq_billing_google_purchases_purchase_token_hash",
        ),
    )

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    package_name = Column(String(120), nullable=False)
    product_id = Column(String(80), nullable=False)
    base_plan_id = Column(String(80), nullable=False)
    purchase_token_hash = Column(String(64), nullable=False, index=True)
    linked_purchase_token_hash = Column(String(64), nullable=True)
    google_subscription_state = Column(String(80), nullable=False)
    xcpro_subscription_status = Column(String(40), nullable=False)
    acknowledgement_state = Column(String(40), nullable=False)
    expiry_time_ms = Column(BigInteger, nullable=True)
    auto_renewing = Column(Boolean, nullable=True)
    last_verified_at_ms = Column(BigInteger, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class BillingGoogleEvent(Base):
    __tablename__ = "billing_google_events"
    __table_args__ = (
        UniqueConstraint(
            "pubsub_message_id",
            name="uq_billing_google_events_pubsub_message_id",
        ),
    )

    id = Column(String, primary_key=True, index=True)
    pubsub_message_id = Column(String(255), nullable=False, index=True)
    event_type = Column(String(80), nullable=False)
    package_name = Column(String(120), nullable=True)
    product_id = Column(String(80), nullable=True)
    purchase_token_hash = Column(String(64), nullable=True, index=True)
    published_at = Column(DateTime, nullable=True)
    processed_at = Column(DateTime, nullable=True)
    processing_result = Column(String(80), nullable=False)
    audit_id = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class BillingAuditRecord(Base):
    __tablename__ = "billing_audit_records"

    audit_id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=True)
    event_type = Column(String(80), nullable=False)
    redacted_subject = Column(String(160), nullable=False)
    purchase_token_hash = Column(String(64), nullable=True, index=True)
    result = Column(String(80), nullable=False)
    detail_json = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)


class FollowRequest(Base):
    __tablename__ = "follow_requests"
    __table_args__ = (
        UniqueConstraint(
            "requester_user_id",
            "target_user_id",
            name="uq_follow_requests_requester_target"
        ),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'declined')",
            name="ck_follow_requests_status"
        ),
        Index(
            "ix_follow_requests_requester_status_updated_at",
            "requester_user_id",
            "status",
            "updated_at"
        ),
        Index(
            "ix_follow_requests_target_status_updated_at",
            "target_user_id",
            "status",
            "updated_at"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    requester_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    target_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    status = Column(String(24), nullable=False)
    responded_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FollowEdge(Base):
    __tablename__ = "follow_edges"
    __table_args__ = (
        Index("ix_follow_edges_followed_user_id", "followed_user_id"),
    )

    follower_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    followed_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class FavoriteFollow(Base):
    __tablename__ = "favorite_follows"
    __table_args__ = (
        Index("ix_favorite_follows_favorite_user_id", "favorite_user_id"),
        CheckConstraint(
            "user_id <> favorite_user_id",
            name="ck_favorite_follows_no_self"
        ),
    )

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    favorite_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class UserRelationshipCounter(Base):
    __tablename__ = "user_relationship_counters"

    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    followers_count = Column(Integer, nullable=False)
    following_count = Column(Integer, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class UserBlock(Base):
    __tablename__ = "blocks"
    __table_args__ = (
        UniqueConstraint(
            "blocker_user_id",
            "blocked_user_id",
            name="uq_blocks_blocker_blocked"
        ),
        CheckConstraint(
            "blocker_user_id <> blocked_user_id",
            name="ck_blocks_no_self_block"
        ),
    )

    blocker_user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    blocked_user_id = Column(String, ForeignKey("users.id"), primary_key=True, index=True)
    created_at = Column(DateTime, nullable=False)


class DevicePushToken(Base):
    __tablename__ = "device_push_tokens"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "platform",
            "provider",
            "device_id",
            name="uq_device_push_tokens_user_platform_provider_device"
        ),
        CheckConstraint(
            "platform IN ('android')",
            name="ck_device_push_tokens_platform"
        ),
        CheckConstraint(
            "provider IN ('fcm')",
            name="ck_device_push_tokens_provider"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    platform = Column(String(24), nullable=False)
    provider = Column(String(24), nullable=False)
    token_hash = Column(String(64), nullable=False, index=True)
    token_ciphertext = Column(Text, nullable=False)
    device_id = Column(String(160), nullable=False)
    app_version = Column(String(80), nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    revoked_at = Column(DateTime, nullable=True)


class NotificationOutboxEvent(Base):
    __tablename__ = "notification_outbox_events"
    __table_args__ = (
        UniqueConstraint(
            "dedupe_key",
            name="uq_notification_outbox_events_dedupe_key"
        ),
        CheckConstraint(
            (
                "event_type IN ("
                "'follow_request_received', "
                "'follow_request_accepted', "
                "'follow_new_follower', "
                "'follow_mutual'"
                ")"
            ),
            name="ck_notification_outbox_events_event_type"
        ),
        CheckConstraint(
            "status IN ('pending', 'sent', 'retryable_failed', 'failed')",
            name="ck_notification_outbox_events_status"
        ),
    )

    id = Column(String, primary_key=True, index=True)
    event_type = Column(String(80), nullable=False, index=True)
    recipient_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    actor_user_id = Column(String, ForeignKey("users.id"), index=True, nullable=False)
    follow_request_id = Column(String, ForeignKey("follow_requests.id"), index=True, nullable=True)
    dedupe_key = Column(String(255), nullable=False)
    status = Column(String(24), nullable=False)
    attempt_count = Column(Integer, nullable=False)
    last_attempt_at = Column(DateTime, nullable=True)
    sent_at = Column(DateTime, nullable=True)
    last_error = Column(String(NOTIFICATION_OUTBOX_ERROR_MAX_LENGTH), nullable=True)
    last_error_retryable = Column(Boolean, nullable=True)
    payload_json = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class Position(BaseModel):
    """Deployed telemetry ingest contract.

    `speed` is XCPro groundSpeedMs in meters per second.
    `timestamp` is client wall-clock time in a UTC/ISO-8601-compatible format.
    Client monotonic timestamps stay transport-local and are not part of this wire DTO.
    """

    session_id: str
    lat: float
    lon: float
    alt: float
    agl_meters: Optional[float] = Field(
        default=None,
        description="Optional XCPro AGL height in meters."
    )
    speed: float = Field(description="XCPro groundSpeedMs in meters per second.")
    heading: float
    timestamp: datetime = Field(
        description="Client wall-clock time in UTC/ISO-8601-compatible format."
    )

    if PYDANTIC_V2:
        @model_validator(mode="before")
        @classmethod
        def validate_wire_contract(cls, payload: Any):
            return reject_monotonic_position_fields(payload)
    else:
        @root_validator(pre=True)
        def validate_wire_contract(cls, payload):
            return reject_monotonic_position_fields(payload)


class TaskUpsertRequest(BaseModel):
    session_id: str
    clear_task: bool = False
    task_name: Optional[str] = None
    task: Optional[dict] = None


class SessionEndRequest(BaseModel):
    session_id: str


class LiveSessionStartRequest(BaseModel):
    visibility: Optional[str] = None


class LiveSessionVisibilityPatchRequest(BaseModel):
    visibility: str


class MeProfilePatchRequest(BaseModel):
    handle: Optional[str] = None
    display_name: Optional[str] = None
    comp_number: Optional[str] = None


class MePrivacyPatchRequest(BaseModel):
    discoverability: Optional[str] = None
    follow_policy: Optional[str] = None
    default_live_visibility: Optional[str] = None
    connection_list_visibility: Optional[str] = None
    social_notifications_enabled: Optional[bool] = None
    live_notifications_enabled: Optional[bool] = None


class GoogleAuthExchangeRequest(BaseModel):
    google_id_token: str


class FirebaseAuthExchangeRequest(BaseModel):
    firebase_id_token: str


class FollowRequestCreateRequest(BaseModel):
    target_user_id: str


class BulkRelationshipStatusRequest(BaseModel):
    user_ids: list[str]


class BlockCreateRequest(BaseModel):
    target_user_id: str


class PushTokenRegistrationRequest(BaseModel):
    token: str
    device_id: str
    platform: str = PUSH_PLATFORM_ANDROID
    provider: str = PUSH_PROVIDER_FCM
    app_version: Optional[str] = None


class GooglePlaySyncRequest(BaseModel):
    packageName: str
    productId: str
    basePlanId: str
    purchaseToken: str
    clientPurchaseState: str
    clientAcknowledged: bool
    obfuscatedAccountId: str
    obfuscatedProfileId: Optional[str] = None
    clientSeenAtMs: int
    appVersionCode: int
    priorProductId: Optional[str] = None
    priorBasePlanId: Optional[str] = None
    replacementMode: Optional[str] = None


class GooglePlayRestoreRequest(BaseModel):
    packageName: str
    purchaseToken: str
    clientPurchaseState: str
    clientAcknowledged: bool
    obfuscatedAccountId: Optional[str] = None
    clientSeenAtMs: Optional[int] = None
    appVersionCode: Optional[int] = None
    clientProductIds: Optional[list[str]] = None

    class Config:
        extra = "forbid"


class PureTrackConnectRequest(BaseModel):
    email: StrictStr = Field(min_length=1, max_length=320)
    password: StrictStr = Field(min_length=1, max_length=1024)

    class Config:
        extra = "forbid"


class PureTrackDisconnectRequest(BaseModel):
    class Config:
        extra = "forbid"


class PureTrackInsertPointRequest(BaseModel):
    clientPointId: StrictStr = Field(min_length=1, max_length=128)
    ts: int
    lat: float
    lng: float
    alt: Optional[float] = None
    speed: Optional[float] = None
    course: Optional[float] = None
    vspeed: Optional[float] = None

    class Config:
        extra = "forbid"


class PureTrackInsertTrackerRequest(BaseModel):
    deviceID: StrictStr = Field(min_length=1, max_length=64)
    points: list[PureTrackInsertPointRequest] = Field(min_items=1, max_items=256)
    type: Optional[int] = None
    rego: Optional[StrictStr] = Field(default=None, max_length=32)
    label: Optional[StrictStr] = Field(default=None, max_length=64)

    class Config:
        extra = "forbid"


class PureTrackInsertPublishRequest(BaseModel):
    clientBatchId: StrictStr = Field(min_length=1, max_length=128)
    trackers: list[PureTrackInsertTrackerRequest] = Field(min_items=1, max_items=16)

    class Config:
        extra = "forbid"


class PureTrackTrafficBbox(BaseModel):
    north: float
    east: float
    south: float
    west: float

    class Config:
        extra = "forbid"


class PureTrackTrafficFilters(BaseModel):
    category: Optional[StrictStr] = None
    objectTypeIds: Optional[list[StrictInt]] = Field(default=None, max_items=32)
    sourceTypeIds: Optional[list[StrictInt]] = Field(default=None, max_items=32)
    maxAgeSeconds: Optional[StrictInt] = None

    class Config:
        extra = "forbid"


class PureTrackTrafficRequest(BaseModel):
    bbox: PureTrackTrafficBbox
    filters: Optional[PureTrackTrafficFilters] = None
    clientRequestId: Optional[StrictStr] = Field(default=None, max_length=128)

    class Config:
        extra = "forbid"


class PureTrackTrafficTarget(BaseModel):
    targetId: str
    lastSeenAtMs: int
    latitudeDeg: float
    longitudeDeg: float
    altitudeGpsMeters: Optional[float] = None
    altitudePressureMeters: Optional[float] = None
    courseDeg: Optional[float] = None
    groundSpeedMps: Optional[float] = None
    verticalSpeedMps: Optional[float] = None
    objectTypeId: Optional[int] = None
    objectCategory: Optional[str] = None
    sourceTypeId: Optional[int] = None
    sourceLabel: Optional[str] = None
    displayLabel: Optional[str] = None
    registration: Optional[str] = None
    callsign: Optional[str] = None
    model: Optional[str] = None
    colorHex: Optional[str] = None
    groundElevationMeters: Optional[float] = None
    thermalClimbRateMps: Optional[float] = None
    signalQuality: Optional[float] = None
    stealth: bool = False
    noTracking: bool = False
    onGround: bool = False
    randomId: bool = False

    class Config:
        extra = "forbid"

    if PYDANTIC_V2:
        @model_validator(mode="before")
        @classmethod
        def validate_safe_payload(cls, payload: Any):
            return reject_sensitive_puretrack_traffic_target_payload(payload)
    else:
        @root_validator(pre=True)
        def validate_safe_payload(cls, payload):
            return reject_sensitive_puretrack_traffic_target_payload(payload)


class PureTrackTrafficResponse(BaseModel):
    result: str
    targets: list[PureTrackTrafficTarget]
    bbox: PureTrackTrafficBbox
    filtersApplied: PureTrackTrafficFilters
    serverFetchedAtMs: int
    freshUntilMs: int
    providerRowCount: int
    droppedRowCount: int
    redactedFieldCount: int
    cacheStatus: str
    retryAfterMs: Optional[int] = None
    auditId: str

    class Config:
        extra = "forbid"

    if PYDANTIC_V2:
        @model_validator(mode="before")
        @classmethod
        def validate_safe_payload(cls, payload: Any):
            return reject_sensitive_puretrack_traffic_response_payload(payload)
    else:
        @root_validator(pre=True)
        def validate_safe_payload(cls, payload):
            return reject_sensitive_puretrack_traffic_response_payload(payload)


class PureTrackStatusResponse(BaseModel):
    connected: bool
    appKeyConfigured: bool
    trafficApiAllowed: bool
    insertApiConfigured: bool
    userAccess: str
    verifiedAtMs: Optional[int] = None
    validUntilMs: Optional[int] = None
    accountLabel: Optional[str] = None
    errorCode: Optional[str] = None
    retryAfterMs: Optional[int] = None
    auditId: Optional[str] = None


class PureTrackConnectResponse(BaseModel):
    result: str
    status: PureTrackStatusResponse


class PureTrackDisconnectResponse(BaseModel):
    result: str
    status: PureTrackStatusResponse


class PureTrackInsertPublishResponse(BaseModel):
    result: str
    acceptedClientPointIds: list[str]
    serverReceivedPointCount: int
    providerInsertedPointCount: Optional[int] = None
    retryAfterMs: Optional[int] = None
    auditId: str


class GooglePlaySyncResponse(BaseModel):
    result: str
    entitlement: dict[str, Any]
    acknowledgementRequired: bool
    acknowledgementCompleted: bool
    acknowledgementRetryAfterMs: Optional[int] = None
    recoveryAction: str
    nextRefreshAfterMs: Optional[int] = None
    auditId: str


class PubSubPushMessage(BaseModel):
    data: str
    messageId: Optional[str] = None
    message_id: Optional[str] = None
    publishTime: Optional[datetime] = None
    publish_time: Optional[datetime] = None
    attributes: Optional[dict[str, str]] = None


class PubSubPushEnvelope(BaseModel):
    message: PubSubPushMessage
    subscription: Optional[str] = None


class GooglePlayRtdnIngestionResponse(BaseModel):
    result: str
    deduped: bool
    auditId: Optional[str] = None


@dataclass
class CurrentUserRecord:
    user: User
    auth_identity: AuthIdentity
    profile: PilotProfile
    privacy: PrivacySetting


@dataclass(frozen=True)
class RelationshipLookup:
    outgoing_pending: frozenset[str]
    incoming_pending: frozenset[str]
    following: frozenset[str]
    followed_by: frozenset[str]


@dataclass(frozen=True)
class RelationshipPolicyContext:
    current_user_id: str
    target_user_id: str
    is_blocked: bool
    current_follows_target: bool
    target_follows_current: bool

    @property
    def is_self(self) -> bool:
        return self.current_user_id == self.target_user_id

    @property
    def is_mutual(self) -> bool:
        return self.current_follows_target and self.target_follows_current


@dataclass(frozen=True)
class LiveFollowFollowingLimit:
    effective_tier: str
    max_following: int


@dataclass(frozen=True)
class GooglePlayVerificationResult:
    package_name: str
    product_id: str
    base_plan_id: str
    subscription_status: str
    expiry_time_ms: Optional[int] = None
    auto_renewing: Optional[bool] = None
    acknowledgement_required: bool = False
    linked_purchase_token: Optional[str] = None


@dataclass(frozen=True)
class GooglePlayProcessingOutcome:
    result: str
    audit_id: str
    acknowledgement_required: bool
    acknowledgement_completed: bool
    acknowledgement_retry_after_ms: Optional[int]
    recovery_action: str


class GooglePlayVerificationTemporarilyUnavailable(Exception):
    pass


class GooglePlayVerificationRejected(Exception):
    pass


class FcmDeliveryTemporarilyUnavailable(Exception):
    pass


class FcmDeliveryRejected(Exception):
    pass


class AndroidPublisherApiClient:
    def __init__(
        self,
        config: Optional[GooglePlayRuntimeConfig] = None,
        timeout_seconds: float = 10.0,
    ):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def resolved_config(self) -> GooglePlayRuntimeConfig:
        return self.config or GOOGLE_PLAY_RUNTIME_CONFIG

    def _require_configured_package(self, package_name: str) -> GooglePlayRuntimeConfig:
        config = self.resolved_config()
        errors: list[str] = []
        if google_service_account is None or google_requests is None:
            errors.append("google-auth service account support is unavailable")
        if not config.service_account_json_path:
            errors.append("missing XCPRO_GOOGLE_PLAY_SERVICE_ACCOUNT_JSON_PATH")
        elif not os.path.isfile(config.service_account_json_path):
            errors.append("Google Play service account JSON path is not readable")
        if not config.package_name:
            errors.append("missing XCPRO_GOOGLE_PLAY_PACKAGE_NAME")
        elif config.package_name != package_name:
            errors.append("configured Google Play package does not match request package")
        if errors:
            raise GooglePlayVerificationTemporarilyUnavailable("; ".join(errors))
        return config

    def _access_token(self, package_name: str) -> str:
        config = self._require_configured_package(package_name)
        try:
            credentials = google_service_account.Credentials.from_service_account_file(
                config.service_account_json_path,
                scopes=[GOOGLE_PLAY_ANDROID_PUBLISHER_SCOPE],
            )
            credentials.refresh(google_requests.Request())
        except Exception as exc:
            if GoogleAuthError is not None and isinstance(exc, GoogleAuthError):
                raise GooglePlayVerificationTemporarilyUnavailable(
                    "Google Play service account authentication failed"
                )
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play service account credentials are unavailable"
            )
        token = getattr(credentials, "token", None)
        if not token:
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play service account did not return an access token"
            )
        return token

    def _authorized_headers(self, package_name: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token(package_name)}",
            "Accept": "application/json",
        }

    def _handle_response(self, response: httpx.Response) -> dict[str, Any]:
        if 200 <= response.status_code < 300:
            if not response.content:
                return {}
            return response.json()
        if response.status_code in {408, 429} or response.status_code >= 500:
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play Developer API temporarily unavailable"
            )
        if response.status_code in {401, 403}:
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play Developer API authentication failed"
            )
        raise GooglePlayVerificationRejected(
            f"Google Play Developer API rejected request with status {response.status_code}"
        )

    def get_subscription_v2(
        self,
        package_name: str,
        purchase_token: str,
    ) -> dict[str, Any]:
        encoded_package = quote(package_name, safe="")
        encoded_token = quote(purchase_token, safe="")
        url = (
            f"{GOOGLE_PLAY_ANDROID_PUBLISHER_BASE_URL}/applications/"
            f"{encoded_package}/purchases/subscriptionsv2/tokens/{encoded_token}"
        )
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(
                    url,
                    headers=self._authorized_headers(package_name),
                )
        except httpx.HTTPError:
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play Developer API request failed"
            )
        return self._handle_response(response)

    def acknowledge_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str,
    ) -> bool:
        encoded_package = quote(package_name, safe="")
        encoded_product = quote(product_id, safe="")
        encoded_token = quote(purchase_token, safe="")
        url = (
            f"{GOOGLE_PLAY_ANDROID_PUBLISHER_BASE_URL}/applications/"
            f"{encoded_package}/purchases/subscriptions/{encoded_product}/tokens/"
            f"{encoded_token}:acknowledge"
        )
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    url,
                    headers={
                        **self._authorized_headers(package_name),
                        "Content-Type": "application/json",
                    },
                    json={},
                )
        except httpx.HTTPError:
            raise GooglePlayVerificationTemporarilyUnavailable(
                "Google Play acknowledgement request failed"
            )
        self._handle_response(response)
        return True


class FcmHttpV1Sender:
    def __init__(
        self,
        config: Optional[FcmRuntimeConfig] = None,
        timeout_seconds: float = 10.0,
    ):
        self.config = config
        self.timeout_seconds = timeout_seconds

    def resolved_config(self) -> FcmRuntimeConfig:
        return self.config or FCM_RUNTIME_CONFIG

    def _require_config(self) -> FcmRuntimeConfig:
        config = self.resolved_config()
        errors: list[str] = []
        if google_service_account is None or google_requests is None:
            errors.append("google-auth service account support is unavailable")
        if not config.project_id:
            errors.append("missing XCPRO_FCM_PROJECT_ID")
        if not config.service_account_json_path:
            errors.append("missing XCPRO_FCM_SERVICE_ACCOUNT_JSON_PATH")
        elif not os.path.isfile(config.service_account_json_path):
            errors.append("FCM service account JSON path is not readable")
        if errors:
            raise FcmDeliveryTemporarilyUnavailable("; ".join(errors))
        return config

    def _access_token(self) -> str:
        config = self._require_config()
        try:
            credentials = google_service_account.Credentials.from_service_account_file(
                config.service_account_json_path,
                scopes=[FCM_MESSAGING_SCOPE],
            )
            credentials.refresh(google_requests.Request())
        except Exception as exc:
            if GoogleAuthError is not None and isinstance(exc, GoogleAuthError):
                raise FcmDeliveryTemporarilyUnavailable(
                    "FCM service account authentication failed"
                )
            raise FcmDeliveryTemporarilyUnavailable(
                "FCM service account credentials are unavailable"
            )
        token = getattr(credentials, "token", None)
        if not token:
            raise FcmDeliveryTemporarilyUnavailable(
                "FCM service account did not return an access token"
            )
        return token

    def _authorized_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token()}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _handle_response(self, response: httpx.Response) -> None:
        if 200 <= response.status_code < 300:
            return
        if response.status_code in {408, 429} or response.status_code >= 500:
            raise FcmDeliveryTemporarilyUnavailable(
                "FCM request temporarily unavailable"
            )
        if response.status_code in {401, 403}:
            raise FcmDeliveryTemporarilyUnavailable(
                "FCM authentication failed"
            )
        raise FcmDeliveryRejected(
            f"FCM rejected message with status {response.status_code}"
        )

    def send_message(self, token: str, data: dict[str, str]) -> None:
        config = self._require_config()
        url = FCM_SEND_URL_TEMPLATE.format(project_id=quote(config.project_id, safe=""))
        body = {
            "message": {
                "token": token,
                "data": data,
                "android": {
                    "priority": "HIGH",
                },
            }
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(
                    url,
                    headers=self._authorized_headers(),
                    json=body,
                )
        except httpx.HTTPError:
            raise FcmDeliveryTemporarilyUnavailable("FCM request failed")
        self._handle_response(response)


def parse_google_play_timestamp_ms(raw_value: Optional[str]) -> Optional[int]:
    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    fractional_match = re.match(
        r"^(.*T\d{2}:\d{2}:\d{2})\.(\d{1,9})(Z|[+-]\d{2}:\d{2})$",
        value,
    )
    if fractional_match is not None:
        value = (
            f"{fractional_match.group(1)}."
            f"{fractional_match.group(2)[:6].ljust(6, '0')}"
            f"{fractional_match.group(3)}"
        )
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        return to_epoch_ms(datetime.fromisoformat(value))
    except ValueError:
        return None


def select_google_play_subscription_line_item(
    response_json: dict[str, Any],
    requested_product_id: str,
) -> Optional[dict[str, Any]]:
    raw_line_items = response_json.get("lineItems")
    if not isinstance(raw_line_items, list):
        return None
    line_items = [item for item in raw_line_items if isinstance(item, dict)]
    matching = [
        item
        for item in line_items
        if str(item.get("productId", "")).strip() == requested_product_id
    ]
    candidates = matching or line_items
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: parse_google_play_timestamp_ms(item.get("expiryTime")) or -1,
    )


def google_play_line_item_product_id(line_item: Optional[dict[str, Any]]) -> str:
    if line_item is None:
        return ""
    return str(line_item.get("productId", "")).strip()


def google_play_line_item_base_plan_id(line_item: Optional[dict[str, Any]]) -> str:
    if line_item is None:
        return ""
    offer_details = line_item.get("offerDetails")
    if not isinstance(offer_details, dict):
        return ""
    return str(offer_details.get("basePlanId", "")).strip()


def google_play_line_item_auto_renewing(line_item: Optional[dict[str, Any]]) -> Optional[bool]:
    if line_item is None:
        return None
    auto_renewing_plan = line_item.get("autoRenewingPlan")
    if isinstance(auto_renewing_plan, dict) and "autoRenewEnabled" in auto_renewing_plan:
        return bool(auto_renewing_plan.get("autoRenewEnabled"))
    if isinstance(line_item.get("prepaidPlan"), dict):
        return False
    return None


def google_play_line_item_expiry_time_ms(line_item: Optional[dict[str, Any]]) -> Optional[int]:
    if line_item is None:
        return None
    return parse_google_play_timestamp_ms(line_item.get("expiryTime"))


def supported_google_play_subscription_line_items(
    response_json: dict[str, Any]
) -> list[dict[str, Any]]:
    raw_line_items = response_json.get("lineItems")
    if not isinstance(raw_line_items, list):
        return []
    supported: list[dict[str, Any]] = []
    for item in raw_line_items:
        if not isinstance(item, dict):
            continue
        product_id = google_play_line_item_product_id(item)
        base_plan_id = google_play_line_item_base_plan_id(item)
        if product_id in TIER_BY_PRODUCT_ID and base_plan_id in PERIOD_BY_BASE_PLAN:
            supported.append(item)
    return supported


def select_backend_derived_google_play_subscription_line_item(
    response_json: dict[str, Any]
) -> dict[str, Any]:
    candidates = supported_google_play_subscription_line_items(response_json)
    if not candidates:
        raise GooglePlayVerificationRejected(
            "verified subscription has no supported XCPro product/base plan"
        )
    if len(candidates) == 1:
        return candidates[0]

    ranked: list[tuple[int, dict[str, Any]]] = []
    for item in candidates:
        expiry_time_ms = google_play_line_item_expiry_time_ms(item)
        if expiry_time_ms is None:
            raise GooglePlayVerificationRejected(
                "verified subscription has ambiguous supported line items"
            )
        ranked.append((expiry_time_ms, item))
    ranked.sort(key=lambda entry: entry[0], reverse=True)
    if len(ranked) > 1 and ranked[0][0] == ranked[1][0]:
        raise GooglePlayVerificationRejected(
            "verified subscription has ambiguous supported line items"
        )
    return ranked[0][1]


def map_google_play_subscription_state(
    response_json: dict[str, Any],
    expiry_time_ms: Optional[int],
) -> str:
    subscription_state = str(response_json.get("subscriptionState", "")).strip()
    if subscription_state == "SUBSCRIPTION_STATE_PENDING":
        return "PENDING"
    if subscription_state == "SUBSCRIPTION_STATE_ACTIVE":
        return "ACTIVE"
    if subscription_state == "SUBSCRIPTION_STATE_IN_GRACE_PERIOD":
        return "GRACE_PERIOD"
    if subscription_state == "SUBSCRIPTION_STATE_ON_HOLD":
        return "ON_HOLD"
    if subscription_state == "SUBSCRIPTION_STATE_PAUSED":
        return "PAUSED"
    if subscription_state == "SUBSCRIPTION_STATE_PENDING_PURCHASE_CANCELED":
        return "REVOKED"
    canceled_context = response_json.get("canceledStateContext")
    developer_canceled = (
        isinstance(canceled_context, dict)
        and isinstance(canceled_context.get("developerInitiatedCancellation"), dict)
    )
    system_canceled = (
        isinstance(canceled_context, dict)
        and isinstance(canceled_context.get("systemInitiatedCancellation"), dict)
    )
    if developer_canceled:
        return "REVOKED"
    if system_canceled:
        return "SUSPENDED"
    if subscription_state == "SUBSCRIPTION_STATE_CANCELED":
        if expiry_time_ms is not None and expiry_time_ms > to_epoch_ms(utcnow()):
            return "CANCELED_BUT_ACTIVE"
        return "EXPIRED"
    if subscription_state == "SUBSCRIPTION_STATE_EXPIRED":
        return "EXPIRED"
    return "ERROR"


def build_google_play_verification_result_from_subscription_v2(
    package_name: str,
    requested_product_id: str,
    response_json: dict[str, Any],
) -> GooglePlayVerificationResult:
    line_item = select_google_play_subscription_line_item(
        response_json=response_json,
        requested_product_id=requested_product_id,
    )
    product_id = (
        str(line_item.get("productId", "")).strip()
        if line_item is not None
        else requested_product_id
    )
    expiry_time_ms = (
        parse_google_play_timestamp_ms(line_item.get("expiryTime"))
        if line_item is not None
        else None
    )
    acknowledgement_state = str(
        response_json.get("acknowledgementState", "")
    ).strip()
    linked_purchase_token = str(
        response_json.get("linkedPurchaseToken", "")
    ).strip() or None
    return GooglePlayVerificationResult(
        package_name=package_name,
        product_id=product_id,
        base_plan_id=google_play_line_item_base_plan_id(line_item),
        subscription_status=map_google_play_subscription_state(
            response_json=response_json,
            expiry_time_ms=expiry_time_ms,
        ),
        expiry_time_ms=expiry_time_ms,
        auto_renewing=google_play_line_item_auto_renewing(line_item),
        acknowledgement_required=(
            acknowledgement_state == "ACKNOWLEDGEMENT_STATE_PENDING"
        ),
        linked_purchase_token=linked_purchase_token,
    )


def build_google_play_token_only_verification_result_from_subscription_v2(
    package_name: str,
    response_json: dict[str, Any],
) -> GooglePlayVerificationResult:
    line_item = select_backend_derived_google_play_subscription_line_item(response_json)
    product_id = google_play_line_item_product_id(line_item)
    base_plan_id = google_play_line_item_base_plan_id(line_item)
    expiry_time_ms = google_play_line_item_expiry_time_ms(line_item)
    subscription_status = map_google_play_subscription_state(
        response_json=response_json,
        expiry_time_ms=expiry_time_ms,
    )
    if subscription_status not in PAID_CONTINUITY_STATUSES:
        raise GooglePlayVerificationRejected(
            "verified subscription is not in paid continuity"
        )
    if expiry_time_ms is None:
        raise GooglePlayVerificationRejected(
            "verified paid continuity is missing expiry"
        )
    acknowledgement_state = str(
        response_json.get("acknowledgementState", "")
    ).strip()
    linked_purchase_token = str(
        response_json.get("linkedPurchaseToken", "")
    ).strip() or None
    return GooglePlayVerificationResult(
        package_name=package_name,
        product_id=product_id,
        base_plan_id=base_plan_id,
        subscription_status=subscription_status,
        expiry_time_ms=expiry_time_ms,
        auto_renewing=google_play_line_item_auto_renewing(line_item),
        acknowledgement_required=(
            acknowledgement_state == "ACKNOWLEDGEMENT_STATE_PENDING"
        ),
        linked_purchase_token=linked_purchase_token,
    )


class GooglePlayPurchaseVerifier:
    def __init__(
        self,
        config: Optional[GooglePlayRuntimeConfig] = None,
        api_client: Optional[AndroidPublisherApiClient] = None,
    ):
        self.config = config
        self.api_client = api_client

    def _api_client(self) -> AndroidPublisherApiClient:
        return self.api_client or AndroidPublisherApiClient(
            config=self.config or GOOGLE_PLAY_RUNTIME_CONFIG
        )

    def verify_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str
    ) -> GooglePlayVerificationResult:
        response_json = self._api_client().get_subscription_v2(
            package_name=package_name,
            purchase_token=purchase_token,
        )
        return build_google_play_verification_result_from_subscription_v2(
            package_name=package_name,
            requested_product_id=product_id,
            response_json=response_json,
        )

    def verify_subscription_token_only(
        self,
        package_name: str,
        purchase_token: str
    ) -> GooglePlayVerificationResult:
        response_json = self._api_client().get_subscription_v2(
            package_name=package_name,
            purchase_token=purchase_token,
        )
        return build_google_play_token_only_verification_result_from_subscription_v2(
            package_name=package_name,
            response_json=response_json,
        )


class GooglePlayPurchaseAcknowledger:
    def __init__(
        self,
        config: Optional[GooglePlayRuntimeConfig] = None,
        api_client: Optional[AndroidPublisherApiClient] = None,
    ):
        self.config = config
        self.api_client = api_client

    def _api_client(self) -> AndroidPublisherApiClient:
        return self.api_client or AndroidPublisherApiClient(
            config=self.config or GOOGLE_PLAY_RUNTIME_CONFIG
        )

    def acknowledge_subscription(
        self,
        package_name: str,
        product_id: str,
        purchase_token: str
    ) -> bool:
        return self._api_client().acknowledge_subscription(
            package_name=package_name,
            product_id=product_id,
            purchase_token=purchase_token,
        )


class GooglePlayRtdnOidcAuthenticator:
    def __init__(
        self,
        config: Optional[GooglePlayRuntimeConfig] = None,
    ):
        self.config = config

    def resolved_config(self) -> GooglePlayRuntimeConfig:
        return self.config or GOOGLE_PLAY_RUNTIME_CONFIG

    def verify_authorization(self, authorization: Optional[str]) -> dict[str, Any]:
        config = self.resolved_config()
        errors: list[str] = []
        if google_id_token is None or google_requests is None:
            errors.append("google-auth OIDC validation support is unavailable")
        if not config.rtdn_oidc_audience:
            errors.append("missing XCPRO_GOOGLE_PLAY_RTDN_OIDC_AUDIENCE")
        if not config.rtdn_expected_service_account_email:
            errors.append(
                "missing XCPRO_GOOGLE_PLAY_RTDN_EXPECTED_SERVICE_ACCOUNT_EMAIL"
            )
        if errors:
            raise ApiHTTPException(
                status_code=503,
                code=ErrorCode.RTDN_AUTH_UNAVAILABLE,
                detail="; ".join(errors),
            )

        token = parse_rtdn_bearer_token(authorization)
        try:
            claims = google_id_token.verify_oauth2_token(
                token,
                google_requests.Request(),
                config.rtdn_oidc_audience,
            )
        except Exception:
            raise ApiHTTPException(
                status_code=401,
                code=ErrorCode.INVALID_RTDN_AUTH,
                detail="invalid RTDN OIDC bearer token",
            )

        email = str(claims.get("email", "")).strip()
        if (
            email != config.rtdn_expected_service_account_email
            or claims.get("email_verified") is False
        ):
            raise ApiHTTPException(
                status_code=401,
                code=ErrorCode.INVALID_RTDN_AUTH,
                detail="invalid RTDN OIDC service account",
            )
        return claims


GOOGLE_PLAY_PURCHASE_VERIFIER = GooglePlayPurchaseVerifier()
GOOGLE_PLAY_PURCHASE_ACKNOWLEDGER = GooglePlayPurchaseAcknowledger()
GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR = GooglePlayRtdnOidcAuthenticator()
FCM_NOTIFICATION_SENDER = FcmHttpV1Sender()


def requested_fields(model: BaseModel) -> set[str]:
    if PYDANTIC_V2:
        return set(model.model_fields_set)
    return set(model.__fields_set__)


def parse_bearer_token(authorization: Optional[str]) -> str:
    if authorization is None or not authorization.strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="missing Authorization header"
        )

    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )
    return parts[1].strip()


def resolve_bearer_identity(authorization: Optional[str]) -> ResolvedBearerIdentity:
    token = parse_bearer_token(authorization)
    identity = STATIC_BEARER_TOKENS.get(token)
    if identity is None:
        identity = PRIVATE_FOLLOW_BEARER_TOKEN_VERIFIER(token)
    if identity is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )
    return identity


def validate_entitlement_package_name(package_name: Optional[str]) -> str:
    normalized = trim_to_none(package_name)
    allowed_packages = {XCPRO_RELEASE_PACKAGE_NAME}
    if (
        PRIVATE_FOLLOW_RUNTIME_CONFIG.runtime_env != RUNTIME_ENV_PROD or
        PRIVATE_FOLLOW_RUNTIME_CONFIG.allow_debug_entitlement_package
    ):
        allowed_packages.add(XCPRO_DEBUG_PACKAGE_NAME)

    if normalized not in allowed_packages:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.INVALID_PACKAGE,
            detail="invalid package name"
        )
    return normalized


def trim_to_none(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    trimmed = raw_value.strip()
    return trimmed or None


def validate_google_play_package_context(
    header_package_name: Optional[str],
    request_package_name: str
) -> str:
    package_name = validate_entitlement_package_name(header_package_name)
    if trim_to_none(request_package_name) != package_name:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.INVALID_PACKAGE,
            detail="invalid package name"
        )
    return package_name


def validate_google_play_product_id(product_id: Optional[str]) -> Optional[str]:
    normalized = trim_to_none(product_id)
    if normalized not in TIER_BY_PRODUCT_ID:
        return None
    return normalized


def validate_google_play_base_plan_id(base_plan_id: Optional[str]) -> Optional[str]:
    normalized = trim_to_none(base_plan_id)
    if normalized not in PERIOD_BY_BASE_PLAN:
        return None
    return normalized


def validate_google_play_client_purchase_state(client_purchase_state: str) -> str:
    normalized = trim_to_none(client_purchase_state)
    if normalized not in GOOGLE_PLAY_CLIENT_PURCHASE_STATE_VALUES:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail="clientPurchaseState is invalid"
        )
    return normalized


def parse_rtdn_bearer_token(authorization: Optional[str]) -> str:
    if authorization is None or not authorization.strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.INVALID_RTDN_AUTH,
            detail="missing RTDN OIDC bearer token"
        )
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.INVALID_RTDN_AUTH,
            detail="invalid RTDN OIDC bearer token"
        )
    return parts[1].strip()


def require_test_rtdn_header_auth(rtdn_token: Optional[str]) -> None:
    if GOOGLE_PLAY_RTDN_INGEST_TOKEN is None:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.RTDN_AUTH_UNAVAILABLE,
            detail="RTDN test ingest token is not configured"
        )
    provided = trim_to_none(rtdn_token)
    if provided is None or not hmac.compare_digest(provided, GOOGLE_PLAY_RTDN_INGEST_TOKEN):
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.INVALID_RTDN_AUTH,
            detail="invalid RTDN test ingest token"
        )


def require_rtdn_ingest_auth(
    authorization: Optional[str],
    rtdn_token: Optional[str]
) -> None:
    if (
        GOOGLE_PLAY_RTDN_ALLOW_TEST_HEADER_AUTH
        and PRIVATE_FOLLOW_RUNTIME_CONFIG.runtime_env != RUNTIME_ENV_PROD
    ):
        require_test_rtdn_header_auth(rtdn_token)
        return
    GOOGLE_PLAY_RTDN_OIDC_AUTHENTICATOR.verify_authorization(authorization)


def normalize_handle(raw_handle: Optional[str]) -> str:
    normalized = (raw_handle or "").strip().lower()
    if not HANDLE_PATTERN.fullmatch(normalized):
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_HANDLE,
            detail="handle must be 3-24 chars of lowercase letters, digits, underscore, or dot"
        )
    return normalized


def normalize_display_name(raw_display_name: Optional[str]) -> str:
    trimmed = trim_to_none(raw_display_name)
    if trimmed is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.PROFILE_INCOMPLETE,
            detail="display_name is required"
        )
    return trimmed


def normalize_search_query(raw_query: Optional[str]) -> str:
    normalized = (raw_query or "").strip().lower()
    if len(normalized) < MIN_SEARCH_QUERY_LENGTH:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.SEARCH_QUERY_TOO_SHORT,
            detail=f"q must be at least {MIN_SEARCH_QUERY_LENGTH} characters"
        )
    return normalized


def validate_privacy_value(field_name: str, value: str) -> str:
    allowed_values = {
        "discoverability": DISCOVERABILITY_VALUES,
        "follow_policy": FOLLOW_POLICY_VALUES,
        "default_live_visibility": DEFAULT_LIVE_VISIBILITY_VALUES,
        "connection_list_visibility": CONNECTION_LIST_VISIBILITY_VALUES,
    }[field_name]
    if value not in allowed_values:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PRIVACY_SETTING,
            detail=f"{field_name} must be one of {sorted(allowed_values)}"
        )
    return value


def normalize_push_token_value(raw_token: str) -> str:
    token = trim_to_none(raw_token)
    if token is None or len(token) > PUSH_TOKEN_MAX_LENGTH:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PUSH_TOKEN,
            detail="token is required"
        )
    return token


def normalize_push_device_id(raw_device_id: str) -> str:
    device_id = trim_to_none(raw_device_id)
    if device_id is None or len(device_id) > PUSH_DEVICE_ID_MAX_LENGTH:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PUSH_TOKEN,
            detail="device_id is required"
        )
    return device_id


def normalize_push_platform(raw_platform: str) -> str:
    platform = (trim_to_none(raw_platform) or "").lower()
    if platform != PUSH_PLATFORM_ANDROID:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PUSH_TOKEN,
            detail="platform must be android"
        )
    return platform


def normalize_push_provider(raw_provider: str) -> str:
    provider = (trim_to_none(raw_provider) or "").lower()
    if provider != PUSH_PROVIDER_FCM:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PUSH_TOKEN,
            detail="provider must be fcm"
        )
    return provider


def normalize_push_app_version(raw_app_version: Optional[str]) -> Optional[str]:
    app_version = trim_to_none(raw_app_version)
    if app_version is not None and len(app_version) > PUSH_APP_VERSION_MAX_LENGTH:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PUSH_TOKEN,
            detail="app_version is too long"
        )
    return app_version


def build_push_token_registration_response(push_token: DevicePushToken) -> dict[str, Any]:
    return {
        "ok": True,
        "device_id": push_token.device_id,
        "platform": push_token.platform,
        "provider": push_token.provider,
        "app_version": push_token.app_version,
        "registered": True,
        "updated_at": to_iso_utc(push_token.updated_at),
    }


def upsert_device_push_token(
    db,
    user_id: str,
    request: PushTokenRegistrationRequest
) -> DevicePushToken:
    token = normalize_push_token_value(request.token)
    device_id = normalize_push_device_id(request.device_id)
    platform = normalize_push_platform(request.platform)
    provider = normalize_push_provider(request.provider)
    app_version = normalize_push_app_version(request.app_version)
    token_hash = hash_push_token(token)
    token_ciphertext = encrypt_push_token(token)
    now = utcnow()

    push_token = (
        db.query(DevicePushToken)
        .filter(
            DevicePushToken.user_id == user_id,
            DevicePushToken.platform == platform,
            DevicePushToken.provider == provider,
            DevicePushToken.device_id == device_id
        )
        .first()
    )
    if push_token is None:
        push_token = DevicePushToken(
            id=str(uuid.uuid4()),
            user_id=user_id,
            platform=platform,
            provider=provider,
            device_id=device_id,
            created_at=now
        )
        db.add(push_token)

    push_token.token_hash = token_hash
    push_token.token_ciphertext = token_ciphertext
    push_token.app_version = app_version
    push_token.updated_at = now
    push_token.revoked_at = None

    duplicate_active_rows = (
        db.query(DevicePushToken)
        .filter(
            DevicePushToken.token_hash == token_hash,
            DevicePushToken.revoked_at.is_(None),
            DevicePushToken.id != push_token.id
        )
        .all()
    )
    for duplicate in duplicate_active_rows:
        duplicate.revoked_at = now
        duplicate.updated_at = now

    return push_token


def revoke_device_push_token(
    db,
    user_id: str,
    device_id: str
) -> bool:
    normalized_device_id = normalize_push_device_id(device_id)
    push_token = (
        db.query(DevicePushToken)
        .filter(
            DevicePushToken.user_id == user_id,
            DevicePushToken.platform == PUSH_PLATFORM_ANDROID,
            DevicePushToken.provider == PUSH_PROVIDER_FCM,
            DevicePushToken.device_id == normalized_device_id,
            DevicePushToken.revoked_at.is_(None)
        )
        .first()
    )
    if push_token is None:
        return False
    now = utcnow()
    push_token.revoked_at = now
    push_token.updated_at = now
    return True


def ensure_current_user_record(
    db,
    authorization: Optional[str]
) -> CurrentUserRecord:
    identity = resolve_bearer_identity(authorization)
    return ensure_current_user_record_for_identity(db, identity)


def ensure_current_user_record_for_identity(
    db,
    identity: ResolvedBearerIdentity
) -> CurrentUserRecord:
    now = utcnow()

    auth_identity = (
        db.query(AuthIdentity)
        .filter(
            AuthIdentity.provider == identity.provider,
            AuthIdentity.provider_subject == identity.provider_subject
        )
        .first()
    )

    if auth_identity is None:
        user = User(
            id=str(uuid.uuid4()),
            created_at=now,
            updated_at=now
        )
        db.add(user)
        db.flush()

        auth_identity = AuthIdentity(
            id=str(uuid.uuid4()),
            user_id=user.id,
            provider=identity.provider,
            provider_subject=identity.provider_subject,
            provider_email=trim_to_none(identity.email),
            provider_email_verified=identity.email_verified,
            created_at=now,
            updated_at=now,
            last_seen_at=now
        )
        db.add(auth_identity)

        profile = PilotProfile(
            user_id=user.id,
            handle=None,
            handle_normalized=None,
            display_name=trim_to_none(identity.display_name),
            comp_number=None,
            created_at=now,
            updated_at=now
        )
        privacy = PrivacySetting(
            user_id=user.id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            social_notifications_enabled=DEFAULT_SOCIAL_NOTIFICATIONS_ENABLED,
            live_notifications_enabled=DEFAULT_LIVE_NOTIFICATIONS_ENABLED,
            created_at=now,
            updated_at=now
        )
        db.add(profile)
        db.add(privacy)
        db.commit()
        return CurrentUserRecord(
            user=user,
            auth_identity=auth_identity,
            profile=profile,
            privacy=privacy
        )

    user = db.query(User).filter(User.id == auth_identity.user_id).first()
    if user is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.UNAUTHENTICATED,
            detail="invalid bearer token"
        )

    profile = db.query(PilotProfile).filter(PilotProfile.user_id == user.id).first()
    if profile is None:
        profile = PilotProfile(
            user_id=user.id,
            handle=None,
            handle_normalized=None,
            display_name=trim_to_none(identity.display_name),
            comp_number=None,
            created_at=now,
            updated_at=now
        )
        db.add(profile)

    privacy = db.query(PrivacySetting).filter(PrivacySetting.user_id == user.id).first()
    if privacy is None:
        privacy = PrivacySetting(
            user_id=user.id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            social_notifications_enabled=DEFAULT_SOCIAL_NOTIFICATIONS_ENABLED,
            live_notifications_enabled=DEFAULT_LIVE_NOTIFICATIONS_ENABLED,
            created_at=now,
            updated_at=now
        )
        db.add(privacy)

    user.updated_at = now
    auth_identity.provider_email = trim_to_none(identity.email)
    if identity.email_verified is not None:
        auth_identity.provider_email_verified = identity.email_verified
    auth_identity.updated_at = now
    auth_identity.last_seen_at = now
    db.commit()
    return CurrentUserRecord(
        user=user,
        auth_identity=auth_identity,
        profile=profile,
        privacy=privacy
    )


FIREBASE_EMAIL_PASSWORD_SIGN_IN_PROVIDERS = frozenset({"password", "email-password"})
FIREBASE_GOOGLE_SIGN_IN_PROVIDER = "google.com"
FIREBASE_PHONE_SIGN_IN_PROVIDER = "phone"


def unsupported_firebase_auth_provider_exception(
    sign_in_provider: Optional[str]
) -> ApiHTTPException:
    provider = str(sign_in_provider or "").strip() or "unknown"
    return ApiHTTPException(
        status_code=403,
        code=ErrorCode.UNSUPPORTED_FIREBASE_AUTH_PROVIDER,
        detail=f"Unsupported Firebase auth method: {provider}"
    )


def firebase_exchange_auth_method(identity: ResolvedFirebaseIdentity) -> str:
    if identity.sign_in_provider in FIREBASE_EMAIL_PASSWORD_SIGN_IN_PROVIDERS:
        return "email_password"
    if identity.sign_in_provider == FIREBASE_GOOGLE_SIGN_IN_PROVIDER:
        return "firebase_google"
    if identity.sign_in_provider == FIREBASE_PHONE_SIGN_IN_PROVIDER:
        return "phone"
    raise unsupported_firebase_auth_provider_exception(identity.sign_in_provider)


def build_firebase_bearer_identity(
    identity: ResolvedFirebaseIdentity
) -> ResolvedBearerIdentity:
    return ResolvedBearerIdentity(
        provider="firebase",
        provider_subject=identity.firebase_uid,
        email=identity.email,
        email_verified=identity.email_verified,
        display_name=identity.display_name
    )


def single_firebase_google_provider_subject(
    identity: ResolvedFirebaseIdentity
) -> Optional[str]:
    google_subjects = [
        str(provider_subject or "").strip()
        for provider_subject in identity.provider_identities.get("google.com", [])
        if str(provider_subject or "").strip()
    ]
    if len(google_subjects) != 1:
        return None
    return google_subjects[0]


def find_auth_identity(db, provider: str, provider_subject: str) -> Optional[AuthIdentity]:
    return (
        db.query(AuthIdentity)
        .filter(
            AuthIdentity.provider == provider,
            AuthIdentity.provider_subject == provider_subject
        )
        .first()
    )


def create_auth_identity_for_user(
    db,
    user_id: str,
    identity: ResolvedBearerIdentity
) -> AuthIdentity:
    now = utcnow()
    auth_identity = AuthIdentity(
        id=str(uuid.uuid4()),
        user_id=user_id,
        provider=identity.provider,
        provider_subject=identity.provider_subject,
        provider_email=trim_to_none(identity.email),
        provider_email_verified=identity.email_verified,
        created_at=now,
        updated_at=now,
        last_seen_at=now
    )
    db.add(auth_identity)
    db.flush()
    return auth_identity


def find_verified_email_non_firebase_conflict(
    db,
    email: Optional[str]
) -> Optional[AuthIdentity]:
    normalized_email = trim_to_none(email)
    if normalized_email is None:
        return None
    return (
        db.query(AuthIdentity)
        .filter(
            AuthIdentity.provider != "firebase",
            AuthIdentity.provider_email.isnot(None),
            func.lower(AuthIdentity.provider_email) == normalized_email.lower()
        )
        .first()
    )


def build_auth_recovery_required_detail(
    firebase_identity: ResolvedFirebaseIdentity,
    conflict_identity: AuthIdentity
) -> dict[str, Any]:
    return {
        "message": "account recovery is required",
        "reason": "verified_email_conflict",
        "provider": "firebase",
        "providerEmail": trim_to_none(firebase_identity.email),
        "conflictProvider": conflict_identity.provider,
        "conflictProviderEmail": trim_to_none(conflict_identity.provider_email),
        "conflictProviderEmailVerified": conflict_identity.provider_email_verified,
    }


def ensure_current_user_record_for_firebase_identity(
    db,
    firebase_identity: ResolvedFirebaseIdentity
) -> CurrentUserRecord:
    bearer_identity = build_firebase_bearer_identity(firebase_identity)
    existing_firebase_identity = find_auth_identity(
        db,
        bearer_identity.provider,
        bearer_identity.provider_subject
    )
    if existing_firebase_identity is not None:
        return ensure_current_user_record_for_identity(db, bearer_identity)

    google_provider_subject = single_firebase_google_provider_subject(firebase_identity)
    if google_provider_subject is not None:
        legacy_google_identity = find_auth_identity(
            db,
            "google",
            google_provider_subject
        )
        if legacy_google_identity is not None:
            create_auth_identity_for_user(
                db,
                legacy_google_identity.user_id,
                bearer_identity
            )
            return ensure_current_user_record_for_identity(db, bearer_identity)

    verified_email_conflict = None
    if firebase_identity.email_verified:
        verified_email_conflict = find_verified_email_non_firebase_conflict(
            db,
            firebase_identity.email
        )
    if verified_email_conflict is not None:
        raise ApiHTTPException(
            status_code=409,
            code=ErrorCode.AUTH_RECOVERY_REQUIRED,
            detail=build_auth_recovery_required_detail(
                firebase_identity,
                verified_email_conflict
            )
        )

    return ensure_current_user_record_for_identity(db, bearer_identity)


def build_profile_response(profile: PilotProfile) -> dict[str, Optional[str]]:
    return {
        "user_id": profile.user_id,
        "handle": profile.handle,
        "display_name": profile.display_name,
        "comp_number": profile.comp_number,
    }


def build_user_summary(profile: PilotProfile) -> dict[str, Optional[str]]:
    return build_profile_response(profile)


def build_privacy_response(privacy: PrivacySetting) -> dict[str, Any]:
    return {
        "discoverability": privacy.discoverability,
        "follow_policy": privacy.follow_policy,
        "default_live_visibility": privacy.default_live_visibility,
        "connection_list_visibility": privacy.connection_list_visibility,
        "social_notifications_enabled": privacy.social_notifications_enabled,
        "live_notifications_enabled": privacy.live_notifications_enabled,
    }


def build_me_response(db, current_user: CurrentUserRecord) -> dict[str, Any]:
    response = build_profile_response(current_user.profile)
    response["privacy"] = build_privacy_response(current_user.privacy)
    response["relationship_limits"] = build_relationship_limits_response(
        db,
        current_user.user.id
    )
    return response


def build_google_auth_exchange_response(
    current_user: CurrentUserRecord,
    access_token: str
) -> dict[str, Any]:
    expires_at = utcnow() + timedelta(seconds=PRIVATE_FOLLOW_BEARER_TTL_SECONDS)
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "auth_method": "google",
        "user_id": current_user.user.id,
        "expires_at": to_iso_utc(expires_at)
    }


def build_firebase_auth_exchange_response(
    db,
    current_user: CurrentUserRecord,
    access_token: str,
    firebase_identity: ResolvedFirebaseIdentity
) -> dict[str, Any]:
    expires_at = utcnow() + timedelta(seconds=PRIVATE_FOLLOW_BEARER_TTL_SECONDS)
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "auth_method": firebase_exchange_auth_method(firebase_identity),
        "user_id": current_user.user.id,
        "expires_at": to_iso_utc(expires_at),
        "profile": build_me_response(db, current_user),
        "entitlement": build_entitlement_response(db, current_user)["entitlement"],
    }


def build_puretrack_entitlement_provider_state(
    db,
    current_user: CurrentUserRecord
) -> dict[str, Any]:
    status = build_puretrack_status_payload(db, current_user)
    return {
        "appKeyConfigured": status["appKeyConfigured"],
        "trafficApiAllowed": status["trafficApiAllowed"],
        "insertApiConfigured": status["insertApiConfigured"],
        "userAccess": puretrack_entitlement_user_access(status["userAccess"]),
        "verifiedAtMs": status["verifiedAtMs"],
        "validUntilMs": status["validUntilMs"],
        "errorCode": status["errorCode"]
    }


def puretrack_entitlement_user_access(user_access: str) -> str:
    if user_access == PURETRACK_PROVIDER_ACCESS_PREMIUM:
        return "PRO"
    return user_access


def build_canonical_free_entitlement_response(
    db,
    current_user: CurrentUserRecord
) -> dict[str, Any]:
    fetched_at_ms = to_epoch_ms(utcnow())
    return {
        "entitlement": {
            "accountSubject": current_user.user.id,
            "tier": "FREE",
            "billingPeriod": "NONE",
            "status": "FREE_ACTIVE",
            "source": "NONE",
            "verificationState": "FREE_CANONICAL",
            "grantedFeatures": [],
            "productId": None,
            "basePlanId": None,
            "expiryTimeMs": None,
            "autoRenewing": None,
            "willLoseAccessAtMs": None,
            "verifiedAtMs": fetched_at_ms,
            "fetchedAtMs": fetched_at_ms,
            "validUntilMs": None,
            "staleAfterMs": FREE_ENTITLEMENT_STALE_AFTER_MS,
            "hardRefreshAfterMs": FREE_ENTITLEMENT_HARD_REFRESH_AFTER_MS,
            "recoveryAction": "NONE",
            "manageSubscriptionUrl": None,
            "providerStates": {
                "skySight": {
                    "accountState": "UNKNOWN",
                    "verifiedAtMs": None,
                    "validUntilMs": None,
                    "errorCode": None
                },
                "pureTrack": build_puretrack_entitlement_provider_state(db, current_user)
            }
        },
        "auditId": None
    }


def require_entitlement_value(raw_value: Optional[str], allowed_values: frozenset[str], field_name: str) -> str:
    value = trim_to_none(raw_value)
    if value not in allowed_values:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.ENTITLEMENT_STATE_INVALID,
            detail=f"stored entitlement {field_name} is invalid"
        )
    return value


def require_stored_entitlement_contract(snapshot: AccountEntitlementSnapshot) -> dict[str, str]:
    tier = require_entitlement_value(snapshot.tier, PLAN_TIER_VALUES, "tier")
    billing_period = require_entitlement_value(
        snapshot.billing_period,
        BILLING_PERIOD_VALUES,
        "billingPeriod"
    )
    status = require_entitlement_value(snapshot.status, SUBSCRIPTION_STATUS_VALUES, "status")
    source = require_entitlement_value(snapshot.source, ENTITLEMENT_SOURCE_VALUES, "source")
    verification_state = require_entitlement_value(
        snapshot.verification_state,
        VERIFICATION_STATE_VALUES,
        "verificationState"
    )
    recovery_action = require_entitlement_value(
        snapshot.recovery_action,
        RECOVERY_ACTION_VALUES,
        "recoveryAction"
    )

    if tier == "FREE":
        if (
            billing_period != "NONE"
            or status != "FREE_ACTIVE"
            or source != "NONE"
            or verification_state != "FREE_CANONICAL"
            or snapshot.product_id is not None
            or snapshot.base_plan_id is not None
        ):
            raise ApiHTTPException(
                status_code=500,
                code=ErrorCode.ENTITLEMENT_STATE_INVALID,
                detail="stored Free entitlement is invalid"
            )
        return {
            "tier": tier,
            "billingPeriod": billing_period,
            "status": status,
            "source": source,
            "verificationState": verification_state,
            "recoveryAction": recovery_action,
        }

    expected_product_id = PRODUCT_ID_BY_TIER.get(tier)
    stored_base_plan_period = PERIOD_BY_BASE_PLAN.get(snapshot.base_plan_id)
    if source != "GOOGLE_PLAY" or expected_product_id is None or stored_base_plan_period is None:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.ENTITLEMENT_STATE_INVALID,
            detail="stored paid entitlement product context is invalid"
        )
    if snapshot.product_id != expected_product_id or stored_base_plan_period != billing_period:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.ENTITLEMENT_STATE_INVALID,
            detail="stored paid entitlement product/base plan mismatch"
        )
    if status in PAID_CONTINUITY_STATUSES and snapshot.valid_until_ms is None:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.ENTITLEMENT_STATE_INVALID,
            detail="stored paid entitlement is missing validUntilMs"
        )
    if status not in PAID_CONTINUITY_STATUSES and status not in DENIED_SUBSCRIPTION_STATUSES:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.ENTITLEMENT_STATE_INVALID,
            detail="stored paid entitlement status is invalid"
        )
    return {
        "tier": tier,
        "billingPeriod": billing_period,
        "status": status,
        "source": source,
        "verificationState": verification_state,
        "recoveryAction": recovery_action,
    }


def stored_paid_continuity_is_current(
    snapshot: AccountEntitlementSnapshot,
    values: dict[str, str],
    now_ms: int,
) -> bool:
    if values["status"] not in PAID_CONTINUITY_STATUSES:
        return False
    if snapshot.valid_until_ms is None or snapshot.valid_until_ms <= now_ms:
        return False
    if snapshot.expiry_time_ms is not None and snapshot.expiry_time_ms <= now_ms:
        return False
    return True


def effective_stored_entitlement_values(
    snapshot: AccountEntitlementSnapshot,
    now_ms: int,
) -> dict[str, str]:
    values = require_stored_entitlement_contract(snapshot)
    if (
        values["status"] in PAID_CONTINUITY_STATUSES
        and not stored_paid_continuity_is_current(snapshot, values, now_ms)
    ):
        return {
            **values,
            "status": "EXPIRED",
        }
    return values


def build_stored_entitlement_response(
    db,
    current_user: CurrentUserRecord,
    snapshot: AccountEntitlementSnapshot,
    now_ms: int,
) -> dict[str, Any]:
    values = effective_stored_entitlement_values(snapshot, now_ms)
    is_paid_continuity = values["status"] in PAID_CONTINUITY_STATUSES
    return {
        "entitlement": {
            "accountSubject": current_user.user.id,
            "tier": values["tier"],
            "billingPeriod": values["billingPeriod"],
            "status": values["status"],
            "source": values["source"],
            "verificationState": values["verificationState"],
            "grantedFeatures": [],
            "productId": snapshot.product_id,
            "basePlanId": snapshot.base_plan_id,
            "expiryTimeMs": snapshot.expiry_time_ms,
            "autoRenewing": snapshot.auto_renewing,
            "willLoseAccessAtMs": snapshot.will_lose_access_at_ms,
            "verifiedAtMs": snapshot.verified_at_ms,
            "fetchedAtMs": snapshot.fetched_at_ms,
            "validUntilMs": snapshot.valid_until_ms if is_paid_continuity else None,
            "staleAfterMs": snapshot.stale_after_ms
            or (
                PAID_CONTINUITY_STALE_AFTER_MS
                if is_paid_continuity
                else DENIED_ENTITLEMENT_STALE_AFTER_MS
            ),
            "hardRefreshAfterMs": snapshot.hard_refresh_after_ms
            or (
                PAID_CONTINUITY_HARD_REFRESH_AFTER_MS
                if is_paid_continuity
                else DENIED_ENTITLEMENT_HARD_REFRESH_AFTER_MS
            ),
            "recoveryAction": values["recoveryAction"],
            "manageSubscriptionUrl": None,
            "providerStates": {
                "skySight": {
                    "accountState": "UNKNOWN",
                    "verifiedAtMs": None,
                    "validUntilMs": None,
                    "errorCode": None
                },
                "pureTrack": build_puretrack_entitlement_provider_state(db, current_user)
            }
        },
        "auditId": None
    }


def build_entitlement_response(db, current_user: CurrentUserRecord) -> dict[str, Any]:
    snapshot = (
        db.query(AccountEntitlementSnapshot)
        .filter(AccountEntitlementSnapshot.user_id == current_user.user.id)
        .first()
    )
    if snapshot is None:
        return build_canonical_free_entitlement_response(db, current_user)
    now_ms = to_epoch_ms(utcnow())
    return build_stored_entitlement_response(db, current_user, snapshot, now_ms)


def validate_puretrack_connect_request(request: PureTrackConnectRequest) -> tuple[str, str]:
    email = trim_to_none(request.email)
    if email is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail="email is required"
        )
    return email, request.password


def puretrack_app_key_configured(
    config: Optional[PureTrackRuntimeConfig] = None
) -> bool:
    resolved_config = config or PURETRACK_RUNTIME_CONFIG
    return trim_to_none(resolved_config.app_key) is not None


def puretrack_insert_key_configured(
    config: Optional[PureTrackRuntimeConfig] = None
) -> bool:
    resolved_config = config or PURETRACK_RUNTIME_CONFIG
    return trim_to_none(resolved_config.insert_key) is not None


def puretrack_audit_id() -> str:
    return f"pt_{uuid.uuid4().hex}"


def puretrack_traffic_audit_id() -> str:
    suffix = "".join(secrets.choice(string.ascii_lowercase) for _ in range(12))
    return f"pt_traffic_{suffix}"


def puretrack_traffic_cadence_user_hash(user_id: str) -> str:
    return hash_token(user_id)[:16]


def puretrack_traffic_retry_after_header_ms(
    headers: Optional[Mapping[str, str]],
) -> Optional[int]:
    if headers is None:
        return None
    raw_value = headers.get("Retry-After")
    if raw_value is None:
        return None
    try:
        seconds = float(str(raw_value).strip())
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return int(math.ceil(seconds * 1000.0))


def build_puretrack_traffic_cadence_evidence_event(
    *,
    started_at_ms: int,
    completed_at_ms: int,
    current_user: CurrentUserRecord,
    package_name: str,
    status_code: int,
    outcome: str,
    cache_status: Optional[str] = None,
    retry_after_ms: Optional[int] = None,
) -> dict[str, Any]:
    event = {
        "event": PURETRACK_TRAFFIC_EVIDENCE_EVENT_NAME,
        "route": PURETRACK_TRAFFIC_ROUTE_PATH,
        "method": "POST",
        "serverReceivedAtMs": started_at_ms,
        "serverCompletedAtMs": completed_at_ms,
        "statusCode": status_code,
        "outcome": outcome,
        "cacheStatus": cache_status,
        "retryAfterMs": retry_after_ms,
        "userHash": puretrack_traffic_cadence_user_hash(current_user.user.id),
        "packageName": package_name,
    }
    return {key: value for key, value in event.items() if value is not None}


def log_puretrack_traffic_evidence_event(event: dict[str, Any]) -> None:
    logger.info(
        "%s %s",
        PURETRACK_TRAFFIC_EVIDENCE_EVENT_NAME,
        json.dumps(event, sort_keys=True, separators=(",", ":")),
    )


PURETRACK_TRAFFIC_EVIDENCE_SINK = log_puretrack_traffic_evidence_event


def emit_puretrack_traffic_cadence_evidence(event: dict[str, Any]) -> None:
    if not PURETRACK_TRAFFIC_EVIDENCE_ENABLED:
        return
    PURETRACK_TRAFFIC_EVIDENCE_SINK(event)


def redact_puretrack_account_label(email: str) -> str:
    normalized = email.strip()
    local, separator, domain = normalized.partition("@")
    if not separator or not domain:
        return "***"
    first_char = local[:1] or "*"
    return f"{first_char}***@{domain.lower()}"


def load_puretrack_provider_session(db, user_id: str) -> Optional[PureTrackProviderSession]:
    return (
        db.query(PureTrackProviderSession)
        .filter(PureTrackProviderSession.user_id == user_id)
        .first()
    )


def puretrack_session_connected(
    session: Optional[PureTrackProviderSession],
) -> bool:
    if session is None:
        return False
    if not session.provider_session_hash:
        return False
    if session.user_access not in {
        PURETRACK_PROVIDER_ACCESS_FREE,
        PURETRACK_PROVIDER_ACCESS_PREMIUM,
    }:
        return False
    return True


def puretrack_provider_session_material_available(
    session: Optional[PureTrackProviderSession],
    config: Optional[PureTrackRuntimeConfig] = None,
) -> bool:
    if session is None or not session.provider_session_ciphertext:
        return False
    try:
        return bool(
            decrypt_puretrack_provider_session_secret(
                session.provider_session_ciphertext,
                config,
            )
        )
    except (ApiHTTPException, ValueError):
        return False


def account_has_verified_xcpro_pro_entitlement(db, user_id: str) -> bool:
    now_ms = to_epoch_ms(utcnow())
    snapshot = (
        db.query(AccountEntitlementSnapshot)
        .filter(AccountEntitlementSnapshot.user_id == user_id)
        .first()
    )
    if snapshot is None:
        return False
    try:
        values = require_stored_entitlement_contract(snapshot)
    except ApiHTTPException as exc:
        if exc.code == ErrorCode.ENTITLEMENT_STATE_INVALID:
            return False
        raise
    return (
        values["tier"] == "PRO"
        and values["verificationState"] == "VERIFIED"
        and stored_paid_continuity_is_current(snapshot, values, now_ms)
    )


def require_puretrack_non_blank(raw_value: Optional[str], field_name: str) -> str:
    value = trim_to_none(raw_value)
    if value is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"{field_name} is required"
        )
    return value


def optional_puretrack_text(raw_value: Optional[str], field_name: str) -> Optional[str]:
    if raw_value is None:
        return None
    value = trim_to_none(raw_value)
    if value is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"{field_name} must not be blank"
        )
    return value


def require_puretrack_finite_number(
    value: float,
    field_name: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    if not math.isfinite(value):
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"{field_name} must be finite"
        )
    if minimum is not None and value < minimum:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"{field_name} out of range"
        )
    if maximum is not None and value > maximum:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"{field_name} out of range"
        )
    return value


def optional_puretrack_finite_number(
    value: Optional[float],
    field_name: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> Optional[float]:
    if value is None:
        return None
    return require_puretrack_finite_number(value, field_name, minimum, maximum)


def validate_puretrack_insert_request(
    request: PureTrackInsertPublishRequest,
) -> tuple[list[dict[str, Any]], list[str], int]:
    require_puretrack_non_blank(request.clientBatchId, "clientBatchId")
    trackers: list[dict[str, Any]] = []
    client_point_ids: list[str] = []
    for tracker_index, tracker in enumerate(request.trackers):
        tracker_prefix = f"trackers[{tracker_index}]"
        mapped_tracker: dict[str, Any] = {
            "deviceID": require_puretrack_non_blank(
                tracker.deviceID,
                f"{tracker_prefix}.deviceID"
            )
        }
        if tracker.type is not None:
            if isinstance(tracker.type, bool) or tracker.type < 0:
                raise ApiHTTPException(
                    status_code=422,
                    code=ErrorCode.VALIDATION_ERROR,
                    detail=f"{tracker_prefix}.type out of range"
                )
            mapped_tracker["type"] = tracker.type
        rego = optional_puretrack_text(tracker.rego, f"{tracker_prefix}.rego")
        if rego is not None:
            mapped_tracker["rego"] = rego
        label = optional_puretrack_text(tracker.label, f"{tracker_prefix}.label")
        if label is not None:
            mapped_tracker["label"] = label

        points = []
        for point_index, point in enumerate(tracker.points):
            point_prefix = f"{tracker_prefix}.points[{point_index}]"
            client_point_id = require_puretrack_non_blank(
                point.clientPointId,
                f"{point_prefix}.clientPointId"
            )
            if isinstance(point.ts, bool) or point.ts < 0:
                raise ApiHTTPException(
                    status_code=422,
                    code=ErrorCode.VALIDATION_ERROR,
                    detail=f"{point_prefix}.ts out of range"
                )
            mapped_point: dict[str, Any] = {
                "ts": point.ts,
                "lat": require_puretrack_finite_number(
                    point.lat,
                    f"{point_prefix}.lat",
                    -90.0,
                    90.0,
                ),
                "lng": require_puretrack_finite_number(
                    point.lng,
                    f"{point_prefix}.lng",
                    -180.0,
                    180.0,
                ),
            }
            for field_name, field_value in (
                ("alt", point.alt),
                ("speed", point.speed),
                ("vspeed", point.vspeed),
            ):
                resolved = optional_puretrack_finite_number(
                    field_value,
                    f"{point_prefix}.{field_name}"
                )
                if resolved is not None:
                    mapped_point[field_name] = resolved
            course = optional_puretrack_finite_number(
                point.course,
                f"{point_prefix}.course",
                0.0,
                360.0,
            )
            if course is not None:
                mapped_point["course"] = course
            points.append(mapped_point)
            client_point_ids.append(client_point_id)
        mapped_tracker["points"] = points
        trackers.append(mapped_tracker)
    return trackers, client_point_ids, len(client_point_ids)


def puretrack_validation_error(detail: str) -> ApiHTTPException:
    return ApiHTTPException(
        status_code=422,
        code=ErrorCode.VALIDATION_ERROR,
        detail=detail,
    )


def puretrack_traffic_distance_meters(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    haversine = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2.0) ** 2
    )
    return 2.0 * PURETRACK_TRAFFIC_EARTH_RADIUS_METERS * math.asin(
        min(1.0, math.sqrt(haversine))
    )


def validate_puretrack_traffic_bbox(
    bbox: PureTrackTrafficBbox,
) -> PureTrackTrafficBbox:
    north = require_puretrack_finite_number(bbox.north, "bbox.north", -90.0, 90.0)
    south = require_puretrack_finite_number(bbox.south, "bbox.south", -90.0, 90.0)
    east = require_puretrack_finite_number(bbox.east, "bbox.east", -180.0, 180.0)
    west = require_puretrack_finite_number(bbox.west, "bbox.west", -180.0, 180.0)
    if north <= south:
        raise puretrack_validation_error("bbox.north must be greater than bbox.south")
    if east <= west:
        raise puretrack_validation_error(
            "bbox.east must be greater than bbox.west; anti-meridian requests are not supported"
        )
    width_meters = puretrack_traffic_distance_meters(north, west, north, east)
    height_meters = puretrack_traffic_distance_meters(north, west, south, west)
    diagonal_meters = puretrack_traffic_distance_meters(north, west, south, east)
    if width_meters > PURETRACK_TRAFFIC_MAX_BBOX_WIDTH_METERS:
        raise puretrack_validation_error("bbox width exceeds PureTrack traffic limit")
    if height_meters > PURETRACK_TRAFFIC_MAX_BBOX_HEIGHT_METERS:
        raise puretrack_validation_error("bbox height exceeds PureTrack traffic limit")
    if diagonal_meters > PURETRACK_TRAFFIC_MAX_BBOX_DIAGONAL_METERS:
        raise puretrack_validation_error("bbox diagonal exceeds PureTrack traffic limit")
    return PureTrackTrafficBbox(north=north, east=east, south=south, west=west)


def validate_puretrack_traffic_id_filter(
    values: Optional[list[int]],
    field_name: str,
) -> Optional[list[int]]:
    if values is None:
        return None
    if len(values) > PURETRACK_TRAFFIC_MAX_FILTER_IDS:
        raise puretrack_validation_error(f"{field_name} has too many entries")
    unique_values: list[int] = []
    seen_values: set[int] = set()
    for value in values:
        if isinstance(value, bool):
            raise puretrack_validation_error(f"{field_name} values must be integers")
        if (
            value < PURETRACK_TRAFFIC_MIN_FILTER_ID
            or value > PURETRACK_TRAFFIC_MAX_FILTER_ID
        ):
            raise puretrack_validation_error(f"{field_name} values out of range")
        if value in seen_values:
            raise puretrack_validation_error(f"{field_name} values must be unique")
        seen_values.add(value)
        unique_values.append(value)
    return unique_values


def validate_puretrack_traffic_filters(
    filters: Optional[PureTrackTrafficFilters],
) -> PureTrackTrafficFilters:
    resolved_filters = filters or PureTrackTrafficFilters()
    category = resolved_filters.category or PURETRACK_TRAFFIC_DEFAULT_CATEGORY
    if category not in PURETRACK_TRAFFIC_ALLOWED_CATEGORIES:
        raise puretrack_validation_error("filters.category is not supported")
    object_type_ids = validate_puretrack_traffic_id_filter(
        resolved_filters.objectTypeIds,
        "filters.objectTypeIds",
    )
    source_type_ids = validate_puretrack_traffic_id_filter(
        resolved_filters.sourceTypeIds,
        "filters.sourceTypeIds",
    )
    max_age_seconds = (
        resolved_filters.maxAgeSeconds
        if resolved_filters.maxAgeSeconds is not None
        else PURETRACK_TRAFFIC_DEFAULT_MAX_AGE_SECONDS
    )
    if isinstance(max_age_seconds, bool):
        raise puretrack_validation_error("filters.maxAgeSeconds must be an integer")
    if (
        max_age_seconds < PURETRACK_TRAFFIC_MIN_MAX_AGE_SECONDS
        or max_age_seconds > PURETRACK_TRAFFIC_MAX_MAX_AGE_SECONDS
    ):
        raise puretrack_validation_error("filters.maxAgeSeconds out of range")
    return PureTrackTrafficFilters(
        category=category,
        objectTypeIds=object_type_ids,
        sourceTypeIds=source_type_ids,
        maxAgeSeconds=max_age_seconds,
    )


def validate_puretrack_traffic_request(
    request: PureTrackTrafficRequest,
) -> tuple[PureTrackTrafficBbox, PureTrackTrafficFilters]:
    bbox = validate_puretrack_traffic_bbox(request.bbox)
    filters = validate_puretrack_traffic_filters(request.filters)
    if request.clientRequestId is not None:
        optional_puretrack_text(request.clientRequestId, "clientRequestId")
    return bbox, filters


def parse_puretrack_compact_traffic_row(row: str) -> Optional[dict[str, str]]:
    if not isinstance(row, str) or not row.strip():
        return None
    fields: dict[str, str] = {}
    for raw_token in row.split(","):
        token = raw_token.strip()
        if not token:
            return None
        key = token[0]
        value = token[1:].strip()
        if not value and key not in {"!", "^"}:
            return None
        fields[key] = value or "1"
    if any(not trim_to_none(fields.get(key)) for key in ("T", "L", "G", "K")):
        return None
    return fields


def parse_puretrack_row_float(
    fields: Mapping[str, str],
    key: str,
    field_name: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
    required: bool = False,
) -> Optional[float]:
    raw_value = trim_to_none(fields.get(key))
    if raw_value is None:
        if required:
            raise ValueError(f"{field_name} is required")
        return None
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} out of range")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} out of range")
    return value


def parse_puretrack_row_int(
    fields: Mapping[str, str],
    key: str,
    field_name: str,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
    required: bool = False,
) -> Optional[int]:
    raw_value = trim_to_none(fields.get(key))
    if raw_value is None:
        if required:
            raise ValueError(f"{field_name} is required")
        return None
    try:
        if re.search(r"[.eE]", raw_value):
            numeric_value = float(raw_value)
            if not numeric_value.is_integer():
                raise ValueError
            value = int(numeric_value)
        else:
            value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} out of range")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} out of range")
    return value


def parse_puretrack_row_bool(fields: Mapping[str, str], key: str) -> bool:
    raw_value = trim_to_none(fields.get(key))
    if raw_value is None:
        return False
    normalized = raw_value.lower()
    return normalized in {"1", "true", "yes", "y"}


def sanitize_puretrack_text(
    raw_value: Optional[str],
    max_length: int,
    allow_spaces: bool = False,
) -> Optional[str]:
    value = trim_to_none(raw_value)
    if value is None:
        return None
    if re.search(r"[\r\n\t]", value):
        return None
    lowered = value.lower()
    if (
        "@" in value
        or "http://" in lowered
        or "https://" in lowered
        or "bearer" in lowered
        or "token" in lowered
        or "password" in lowered
        or "key=" in lowered
        or "access_" + "token" in lowered
    ):
        return None
    if re.search(r"\+?\d[\d ()\-.]{6,}\d", value):
        return None
    if re.match(r"^[XYZ]-", value) or re.match(r"^\d+-[A-Za-z0-9_-]{3,}$", value):
        return None
    if allow_spaces:
        if not re.match(r"^[A-Za-z0-9 ._/\-]{1,}$", value):
            return None
    elif not re.match(r"^[A-Za-z0-9._/\-]{1,}$", value):
        return None
    return value[:max_length]


def sanitize_puretrack_color(raw_value: Optional[str]) -> Optional[str]:
    value = trim_to_none(raw_value)
    if value is None:
        return None
    normalized = value[1:] if value.startswith("#") else value
    if re.fullmatch(r"[0-9A-Fa-f]{6}", normalized):
        return f"#{normalized.upper()}"
    return None


def puretrack_text_has_sensitive_value(raw_value: str) -> bool:
    value = raw_value.strip()
    lowered = value.lower()
    return (
        "@" in value
        or "http://" in lowered
        or "https://" in lowered
        or "puretrack.io" in lowered
        or "/api/" in lowered
        or "bearer" in lowered
        or "token" in lowered
        or "password" in lowered
        or "provider-session" in lowered
        or "server-side-app-key" in lowered
        or "server-side-insert-key" in lowered
        or "access_" + "token" in lowered
        or "key=" in lowered
        or bool(re.search(r"\+?\d[\d ()\-.]{6,}\d", value))
        or bool(re.match(r"^[XYZ]-", value))
        or bool(re.match(r"^\d+-[A-Za-z0-9_-]{3,}$", value))
    )


def is_opaque_puretrack_target_id(raw_value: str) -> bool:
    return bool(re.fullmatch(r"pt_[0-9a-f]{24}", raw_value))


def reject_sensitive_puretrack_visible_text(
    payload: Mapping[str, Any],
    field_name: str,
    max_length: int,
    allow_spaces: bool,
) -> None:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return
    if not isinstance(raw_value, str):
        raise ValueError(f"{field_name} must be a string")
    if len(raw_value) > max_length:
        raise ValueError(f"{field_name} is too long")
    if puretrack_text_has_sensitive_value(raw_value):
        raise ValueError(f"{field_name} contains sensitive provider data")
    if sanitize_puretrack_text(raw_value, max_length, allow_spaces) != raw_value:
        raise ValueError(f"{field_name} contains unsupported characters")


def reject_sensitive_puretrack_traffic_target_payload(payload: Any) -> Any:
    if not isinstance(payload, MappingABC):
        return payload
    target_id = payload.get("targetId")
    if not isinstance(target_id, str) or not is_opaque_puretrack_target_id(target_id):
        raise ValueError("targetId must be an opaque PureTrack target id")
    reject_sensitive_puretrack_visible_text(payload, "sourceLabel", 64, True)
    reject_sensitive_puretrack_visible_text(payload, "displayLabel", 48, False)
    reject_sensitive_puretrack_visible_text(payload, "registration", 32, False)
    reject_sensitive_puretrack_visible_text(payload, "callsign", 32, False)
    reject_sensitive_puretrack_visible_text(payload, "model", 64, True)
    color_hex = payload.get("colorHex")
    if color_hex is not None and sanitize_puretrack_color(color_hex) != color_hex:
        raise ValueError("colorHex must be #RRGGBB")
    return payload


def reject_sensitive_puretrack_traffic_response_payload(payload: Any) -> Any:
    if not isinstance(payload, MappingABC):
        return payload
    result = payload.get("result")
    if result not in {
        PURETRACK_TRAFFIC_RESULT_OK,
        PURETRACK_TRAFFIC_RESULT_EMPTY,
        PURETRACK_TRAFFIC_RESULT_DEGRADED_CACHE,
        PURETRACK_TRAFFIC_RESULT_PARTIAL,
    }:
        raise ValueError("result is not supported")
    cache_status = payload.get("cacheStatus")
    if cache_status not in {
        PURETRACK_TRAFFIC_CACHE_MISS,
        PURETRACK_TRAFFIC_CACHE_HIT,
        PURETRACK_TRAFFIC_CACHE_BYPASS,
    }:
        raise ValueError("cacheStatus is not supported")
    audit_id = payload.get("auditId")
    if not isinstance(audit_id, str) or not audit_id.startswith("pt_"):
        raise ValueError("auditId must be redacted")
    if puretrack_text_has_sensitive_value(audit_id):
        raise ValueError("auditId contains sensitive provider data")
    for field_name in (
        "serverFetchedAtMs",
        "freshUntilMs",
        "providerRowCount",
        "droppedRowCount",
        "redactedFieldCount",
    ):
        value = payload.get(field_name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{field_name} out of range")
    retry_after_ms = payload.get("retryAfterMs")
    if (
        retry_after_ms is not None
        and (
            not isinstance(retry_after_ms, int)
            or isinstance(retry_after_ms, bool)
            or retry_after_ms < 0
        )
    ):
        raise ValueError("retryAfterMs out of range")
    if payload["freshUntilMs"] < payload["serverFetchedAtMs"]:
        raise ValueError("freshUntilMs must not precede serverFetchedAtMs")
    return payload


def opaque_puretrack_target_id(raw_key: str) -> str:
    digest = hashlib.sha256(f"puretrack-target:{raw_key}".encode("utf-8")).hexdigest()
    return f"pt_{digest[:24]}"


def select_puretrack_display_label(
    registration: Optional[str],
    callsign: Optional[str],
    label: Optional[str],
) -> Optional[str]:
    return registration or callsign or sanitize_puretrack_text(label, 48, allow_spaces=False)


def map_puretrack_traffic_fields_to_target(
    fields: Mapping[str, str],
) -> tuple[dict[str, Any], int]:
    timestamp_seconds = parse_puretrack_row_int(
        fields,
        "T",
        "T",
        minimum=0,
        required=True,
    )
    latitude = parse_puretrack_row_float(
        fields,
        "L",
        "L",
        minimum=-90.0,
        maximum=90.0,
        required=True,
    )
    longitude = parse_puretrack_row_float(
        fields,
        "G",
        "G",
        minimum=-180.0,
        maximum=180.0,
        required=True,
    )
    raw_key = require_puretrack_non_blank(fields.get("K"), "K")
    object_type_id = parse_puretrack_row_int(
        fields,
        "O",
        "O",
        minimum=PURETRACK_TRAFFIC_MIN_FILTER_ID,
        maximum=PURETRACK_TRAFFIC_MAX_FILTER_ID,
    )
    source_type_id = parse_puretrack_row_int(
        fields,
        "U",
        "U",
        minimum=PURETRACK_TRAFFIC_MIN_FILTER_ID,
        maximum=PURETRACK_TRAFFIC_MAX_FILTER_ID,
    )
    stealth = parse_puretrack_row_bool(fields, "H")
    no_tracking = parse_puretrack_row_bool(fields, "Q")
    target: dict[str, Any] = {
        "targetId": opaque_puretrack_target_id(raw_key),
        "lastSeenAtMs": timestamp_seconds * 1000,
        "latitudeDeg": latitude,
        "longitudeDeg": longitude,
        "altitudeGpsMeters": parse_puretrack_row_float(fields, "A", "A"),
        "altitudePressureMeters": parse_puretrack_row_float(fields, "t", "t"),
        "courseDeg": parse_puretrack_row_float(fields, "C", "C", 0.0, 360.0),
        "groundSpeedMps": parse_puretrack_row_float(fields, "S", "S", 0.0),
        "verticalSpeedMps": parse_puretrack_row_float(fields, "V", "V"),
        "objectTypeId": object_type_id,
        "objectCategory": (
            PURETRACK_TRAFFIC_OBJECT_TYPE_CATEGORIES.get(object_type_id)
            if object_type_id is not None
            else None
        ),
        "sourceTypeId": source_type_id,
        "sourceLabel": (
            PURETRACK_TRAFFIC_SOURCE_LABELS.get(source_type_id)
            if source_type_id is not None
            else None
        ),
        "groundElevationMeters": parse_puretrack_row_float(fields, "g", "g"),
        "thermalClimbRateMps": parse_puretrack_row_float(fields, "r", "r"),
        "signalQuality": parse_puretrack_row_float(fields, "I", "I"),
        "stealth": stealth,
        "noTracking": no_tracking,
        "onGround": parse_puretrack_row_bool(fields, "8"),
        "randomId": parse_puretrack_row_bool(fields, "!"),
    }
    redacted_count = sum(1 for key in fields if key in PURETRACK_TRAFFIC_EXCLUDED_ROW_KEYS)
    if stealth or no_tracking:
        redacted_count += sum(1 for key in fields if key in PURETRACK_TRAFFIC_STEALTH_IDENTITY_KEYS)
    else:
        registration = sanitize_puretrack_text(fields.get("E"), 32)
        callsign = sanitize_puretrack_text(fields.get("m"), 32)
        model = sanitize_puretrack_text(fields.get("M"), 64, allow_spaces=True)
        color_hex = sanitize_puretrack_color(fields.get("c"))
        target["registration"] = registration
        target["callsign"] = callsign
        target["model"] = model
        target["colorHex"] = color_hex
        target["displayLabel"] = select_puretrack_display_label(
            registration,
            callsign,
            fields.get("B"),
        )
    return {key: value for key, value in target.items() if value is not None}, redacted_count


def map_puretrack_compact_row_to_traffic_target(
    row: str,
) -> tuple[Optional[dict[str, Any]], int]:
    fields = parse_puretrack_compact_traffic_row(row)
    if fields is None:
        return None, 0
    try:
        return map_puretrack_traffic_fields_to_target(fields)
    except (ApiHTTPException, ValueError):
        return None, 0


def puretrack_target_matches_filters(
    target: Mapping[str, Any],
    filters: PureTrackTrafficFilters,
) -> bool:
    if filters.objectTypeIds is not None and target.get("objectTypeId") not in filters.objectTypeIds:
        return False
    if filters.sourceTypeIds is not None and target.get("sourceTypeId") not in filters.sourceTypeIds:
        return False
    object_category = target.get("objectCategory")
    if object_category is not None and filters.category is not None:
        return object_category == filters.category
    return True


def map_puretrack_compact_rows_to_traffic_targets(
    rows: list[str],
    filters: Optional[PureTrackTrafficFilters] = None,
) -> tuple[list[dict[str, Any]], int, int]:
    resolved_filters = validate_puretrack_traffic_filters(filters)
    targets: list[dict[str, Any]] = []
    dropped_count = 0
    redacted_count = 0
    for row in rows:
        target, row_redacted_count = map_puretrack_compact_row_to_traffic_target(row)
        if target is None or not puretrack_target_matches_filters(target, resolved_filters):
            dropped_count += 1
            continue
        targets.append(target)
        redacted_count += row_redacted_count
    return targets, dropped_count, redacted_count


def pydantic_model_to_dict(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def puretrack_retry_after_header(retry_after_ms: Optional[int]) -> dict[str, str]:
    if retry_after_ms is None:
        return {}
    retry_after_seconds = max(0, math.ceil(retry_after_ms / 1000.0))
    return {"Retry-After": str(retry_after_seconds)}


def check_puretrack_traffic_rate_limit(user_id: str) -> Optional[int]:
    now_ms = to_epoch_ms(utcnow())
    refill_per_ms = PURETRACK_TRAFFIC_RATE_LIMIT_PER_MINUTE / 60_000.0
    with _puretrack_traffic_rate_limit_lock:
        tokens, updated_at_ms = _puretrack_traffic_rate_limits.get(
            user_id,
            (float(PURETRACK_TRAFFIC_RATE_LIMIT_BURST), now_ms),
        )
        elapsed_ms = max(0, now_ms - updated_at_ms)
        tokens = min(
            float(PURETRACK_TRAFFIC_RATE_LIMIT_BURST),
            tokens + elapsed_ms * refill_per_ms,
        )
        if tokens >= 1.0:
            _puretrack_traffic_rate_limits[user_id] = (tokens - 1.0, now_ms)
            return None
        retry_after_ms = max(1000, math.ceil((1.0 - tokens) / refill_per_ms))
        _puretrack_traffic_rate_limits[user_id] = (tokens, now_ms)
        return retry_after_ms


def raise_puretrack_traffic_rate_limited(retry_after_ms: int) -> None:
    raise ApiHTTPException(
        status_code=429,
        code=ErrorCode.PURETRACK_RATE_LIMITED,
        detail="PureTrack traffic rate limit applies",
        headers=puretrack_retry_after_header(retry_after_ms),
    )


def puretrack_traffic_cache_key(
    user_id: str,
    package_name: str,
    bbox: PureTrackTrafficBbox,
    filters: PureTrackTrafficFilters,
) -> str:
    payload = {
        "userId": user_id,
        "packageName": package_name,
        "bbox": pydantic_model_to_dict(bbox),
        "filters": pydantic_model_to_dict(filters),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def get_cached_puretrack_traffic_response(
    cache_key: str,
    now_ms: int,
) -> Optional[dict[str, Any]]:
    with _puretrack_traffic_cache_lock:
        cached = _puretrack_traffic_cache.get(cache_key)
        if cached is None:
            return None
        if cached["freshUntilMs"] < now_ms:
            _puretrack_traffic_cache.pop(cache_key, None)
            return None
        response = dict(cached)
        response["targets"] = list(cached["targets"])
        response["cacheStatus"] = PURETRACK_TRAFFIC_CACHE_HIT
        return response


def set_cached_puretrack_traffic_response(
    cache_key: str,
    response: PureTrackTrafficResponse,
) -> None:
    cached = pydantic_model_to_dict(response)
    cached["cacheStatus"] = PURETRACK_TRAFFIC_CACHE_MISS
    with _puretrack_traffic_cache_lock:
        _puretrack_traffic_cache[cache_key] = cached


def require_puretrack_traffic_provider_session_secret(
    db,
    current_user: CurrentUserRecord,
) -> str:
    if not puretrack_app_key_configured():
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
            detail="PureTrack Traffic API is not configured"
        )
    if not account_has_verified_xcpro_pro_entitlement(db, current_user.user.id):
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.FEATURE_ACCESS_DENIED,
            detail="PureTrack traffic requires XCPro PRO access"
        )

    session = load_puretrack_provider_session(db, current_user.user.id)
    if not puretrack_session_connected(session):
        raise ApiHTTPException(
            status_code=409,
            code=ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            detail="PureTrack provider account is not connected"
        )
    if session.user_access != PURETRACK_PROVIDER_ACCESS_PREMIUM:
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            detail="PureTrack Pro provider access is required"
        )
    if not session.provider_session_ciphertext:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            detail="PureTrack provider session material is unavailable"
        )
    try:
        provider_session_secret = decrypt_puretrack_provider_session_secret(
            session.provider_session_ciphertext
        )
    except (ApiHTTPException, ValueError):
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            detail="PureTrack provider session material is unavailable"
        )
    if not provider_session_secret:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
            detail="PureTrack provider session material is unavailable"
        )
    return provider_session_secret


def raise_puretrack_traffic_provider_error(
    provider_result: PureTrackTrafficProviderResult,
) -> None:
    retry_after_ms = provider_result.retry_after_ms
    if provider_result.error_code == ErrorCode.PURETRACK_RATE_LIMITED:
        raise ApiHTTPException(
            status_code=429,
            code=ErrorCode.PURETRACK_RATE_LIMITED,
            detail="PureTrack provider rate limit applies",
            headers=puretrack_retry_after_header(retry_after_ms),
        )
    if provider_result.error_code == ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED:
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED,
            detail="PureTrack Pro provider access is required"
        )
    if provider_result.error_code == ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE:
        raise ApiHTTPException(
            status_code=409,
            code=ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED,
            detail="PureTrack provider account must be reconnected"
        )
    if provider_result.error_code == ErrorCode.PURETRACK_TRAFFIC_REJECTED:
        raise ApiHTTPException(
            status_code=502,
            code=ErrorCode.PURETRACK_TRAFFIC_REJECTED,
            detail="PureTrack rejected the traffic request"
        )
    raise ApiHTTPException(
        status_code=503,
        code=ErrorCode.PURETRACK_PROVIDER_UNAVAILABLE,
        detail="PureTrack provider is unavailable"
    )


def mark_puretrack_provider_session_reconnect_required(
    db,
    user_id: str,
    audit_id: str,
) -> bool:
    session = load_puretrack_provider_session(db, user_id)
    if session is None:
        return False
    now = utcnow()
    session.provider_session_hash = None
    session.provider_session_ciphertext = None
    session.user_access = PURETRACK_PROVIDER_ACCESS_NONE
    session.account_label = None
    session.valid_until_ms = None
    session.error_code = ErrorCode.PURETRACK_PROVIDER_NOT_CONNECTED
    session.retry_after_ms = None
    session.audit_id = audit_id
    session.updated_at = now
    return True


def mark_puretrack_provider_access_denied(
    db,
    user_id: str,
    audit_id: str,
) -> bool:
    session = load_puretrack_provider_session(db, user_id)
    if session is None:
        return False
    now = utcnow()
    session.user_access = PURETRACK_PROVIDER_ACCESS_FREE
    session.valid_until_ms = None
    session.error_code = ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED
    session.retry_after_ms = None
    session.audit_id = audit_id
    session.updated_at = now
    return True


def persist_puretrack_traffic_provider_error_state(
    db,
    current_user: CurrentUserRecord,
    provider_result: PureTrackTrafficProviderResult,
) -> None:
    audit_id = puretrack_traffic_audit_id()
    mutated = False
    if provider_result.error_code == ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE:
        mutated = mark_puretrack_provider_session_reconnect_required(
            db,
            current_user.user.id,
            audit_id,
        )
    elif provider_result.error_code == ErrorCode.PURETRACK_PROVIDER_ACCESS_DENIED:
        mutated = mark_puretrack_provider_access_denied(
            db,
            current_user.user.id,
            audit_id,
        )
    if mutated:
        db.commit()


def fetch_puretrack_traffic(
    db,
    current_user: CurrentUserRecord,
    package_name: str,
    request: PureTrackTrafficRequest,
) -> PureTrackTrafficResponse:
    bbox, filters = validate_puretrack_traffic_request(request)
    provider_session_secret = require_puretrack_traffic_provider_session_secret(
        db,
        current_user,
    )
    retry_after_ms = check_puretrack_traffic_rate_limit(current_user.user.id)
    if retry_after_ms is not None:
        raise_puretrack_traffic_rate_limited(retry_after_ms)
    now_ms = to_epoch_ms(utcnow())
    cache_key = puretrack_traffic_cache_key(
        current_user.user.id,
        package_name,
        bbox,
        filters,
    )
    cached_response = get_cached_puretrack_traffic_response(cache_key, now_ms)
    if cached_response is not None:
        return PureTrackTrafficResponse(**cached_response)

    provider_result = PURETRACK_TRAFFIC_CLIENT.fetch(
        bbox=bbox,
        filters=filters,
        provider_session_secret=provider_session_secret,
        config=PURETRACK_RUNTIME_CONFIG,
    )
    if provider_result.error_code is not None or provider_result.rows is None:
        persist_puretrack_traffic_provider_error_state(
            db,
            current_user,
            provider_result,
        )
        raise_puretrack_traffic_provider_error(provider_result)

    targets, dropped_count, redacted_count = map_puretrack_compact_rows_to_traffic_targets(
        provider_result.rows,
        filters,
    )
    fetched_at_ms = to_epoch_ms(utcnow())
    response = PureTrackTrafficResponse(
        result=(
            PURETRACK_TRAFFIC_RESULT_OK
            if targets
            else PURETRACK_TRAFFIC_RESULT_EMPTY
        ),
        targets=[PureTrackTrafficTarget(**target) for target in targets],
        bbox=bbox,
        filtersApplied=filters,
        serverFetchedAtMs=fetched_at_ms,
        freshUntilMs=fetched_at_ms + PURETRACK_TRAFFIC_CACHE_TTL_MS,
        providerRowCount=provider_result.provider_row_count,
        droppedRowCount=dropped_count,
        redactedFieldCount=redacted_count,
        cacheStatus=PURETRACK_TRAFFIC_CACHE_MISS,
        retryAfterMs=None,
        auditId=puretrack_traffic_audit_id(),
    )
    set_cached_puretrack_traffic_response(cache_key, response)
    return response


def build_puretrack_status_payload(
    db,
    current_user: CurrentUserRecord,
    connected: Optional[bool] = None,
    user_access: Optional[str] = None,
    account_label: Optional[str] = None,
    verified_at_ms: Optional[int] = None,
    valid_until_ms: Optional[int] = None,
    error_code: Optional[str] = None,
    retry_after_ms: Optional[int] = None,
    audit_id: Optional[str] = None,
) -> dict[str, Any]:
    session = load_puretrack_provider_session(db, current_user.user.id)
    session_has_provider_state = puretrack_session_connected(session)
    provider_session_material_ready = puretrack_provider_session_material_available(session)
    session_is_connected = session_has_provider_state and provider_session_material_ready
    resolved_connected = session_is_connected if connected is None else connected
    resolved_user_access = (
        user_access
        if user_access is not None
        else (session.user_access if session is not None else PURETRACK_PROVIDER_ACCESS_UNKNOWN)
    )
    resolved_verified_at_ms = (
        verified_at_ms
        if verified_at_ms is not None
        else (session.verified_at_ms if session is not None else None)
    )
    resolved_valid_until_ms = (
        valid_until_ms
        if valid_until_ms is not None
        else None
    )
    resolved_account_label = (
        account_label
        if account_label is not None
        else (
            session.account_label
            if session is not None and resolved_connected
            else None
        )
    )
    resolved_error_code = (
        error_code
        if error_code is not None
        else (session.error_code if session is not None else None)
    )
    if (
        resolved_error_code is None
        and session_has_provider_state
        and resolved_user_access == PURETRACK_PROVIDER_ACCESS_PREMIUM
        and not provider_session_material_ready
    ):
        resolved_error_code = ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE
    resolved_retry_after_ms = (
        retry_after_ms
        if retry_after_ms is not None
        else (session.retry_after_ms if session is not None else None)
    )
    resolved_audit_id = (
        audit_id
        if audit_id is not None
        else (session.audit_id if session is not None else None)
    )
    app_key_configured = puretrack_app_key_configured()
    insert_key_configured = puretrack_insert_key_configured()
    traffic_api_allowed = (
        app_key_configured
        and resolved_connected
        and resolved_user_access == PURETRACK_PROVIDER_ACCESS_PREMIUM
        and provider_session_material_ready
        and account_has_verified_xcpro_pro_entitlement(db, current_user.user.id)
    )
    return {
        "connected": resolved_connected,
        "appKeyConfigured": app_key_configured,
        "trafficApiAllowed": traffic_api_allowed,
        "insertApiConfigured": insert_key_configured,
        "userAccess": resolved_user_access,
        "verifiedAtMs": resolved_verified_at_ms,
        "validUntilMs": resolved_valid_until_ms,
        "accountLabel": resolved_account_label,
        "errorCode": resolved_error_code,
        "retryAfterMs": resolved_retry_after_ms,
        "auditId": resolved_audit_id,
    }


def publish_puretrack_insert_batch(
    db,
    current_user: CurrentUserRecord,
    request: PureTrackInsertPublishRequest,
) -> dict[str, Any]:
    audit_id = puretrack_audit_id()
    if not puretrack_insert_key_configured():
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.PURETRACK_INSERT_KEY_UNCONFIGURED,
            detail="PureTrack Insert publishing is not configured"
        )
    if not account_has_verified_xcpro_pro_entitlement(db, current_user.user.id):
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.FEATURE_ACCESS_DENIED,
            detail="PureTrack Insert publishing requires XCPro PRO access"
        )

    trackers, client_point_ids, point_count = validate_puretrack_insert_request(request)
    provider_result = PURETRACK_INSERT_CLIENT.publish(
        trackers=trackers,
        config=PURETRACK_RUNTIME_CONFIG,
    )
    accepted_point_ids = (
        client_point_ids
        if provider_result.result == PURETRACK_INSERT_RESULT_ACCEPTED
        else []
    )
    return {
        "result": provider_result.result,
        "acceptedClientPointIds": accepted_point_ids,
        "serverReceivedPointCount": point_count,
        "providerInsertedPointCount": provider_result.provider_inserted_point_count,
        "retryAfterMs": provider_result.retry_after_ms,
        "auditId": audit_id,
    }


def save_puretrack_connected_session(
    db,
    current_user: CurrentUserRecord,
    provider_result: PureTrackProviderLoginResult,
    email: str,
    audit_id: str,
) -> None:
    if provider_result.provider_session_secret is None:
        raise ApiHTTPException(
            status_code=500,
            code=ErrorCode.PURETRACK_STATE_INVALID,
            detail="PureTrack provider session state is invalid"
        )
    provider_session_ciphertext = encrypt_puretrack_provider_session_secret(
        provider_result.provider_session_secret
    )
    now = utcnow()
    now_ms = to_epoch_ms(now)
    session = load_puretrack_provider_session(db, current_user.user.id)
    if session is None:
        session = PureTrackProviderSession(
            user_id=current_user.user.id,
            created_at=now,
            updated_at=now,
            user_access=provider_result.user_access,
        )
        db.add(session)
    session.provider_session_hash = hash_token(provider_result.provider_session_secret)
    session.provider_session_ciphertext = provider_session_ciphertext
    session.user_access = provider_result.user_access
    session.account_label = redact_puretrack_account_label(email)
    session.verified_at_ms = now_ms
    session.valid_until_ms = None
    session.error_code = None
    session.retry_after_ms = None
    session.audit_id = audit_id
    session.updated_at = now


def clear_puretrack_provider_session(db, user_id: str) -> bool:
    session = load_puretrack_provider_session(db, user_id)
    if session is None:
        return False
    db.delete(session)
    return True


def puretrack_failure_user_access(result: PureTrackProviderLoginResult) -> str:
    if result.user_access in PURETRACK_USER_ACCESS_VALUES:
        return result.user_access
    return PURETRACK_PROVIDER_ACCESS_ERROR


def connect_puretrack_account(
    db,
    current_user: CurrentUserRecord,
    request: PureTrackConnectRequest,
) -> dict[str, Any]:
    email, password = validate_puretrack_connect_request(request)
    audit_id = puretrack_audit_id()
    if not puretrack_app_key_configured():
        status = build_puretrack_status_payload(
            db,
            current_user,
            connected=False,
            user_access=PURETRACK_PROVIDER_ACCESS_UNKNOWN,
            account_label=None,
            error_code=ErrorCode.PURETRACK_APP_KEY_UNCONFIGURED,
            audit_id=audit_id,
        )
        return {
            "result": PURETRACK_CONNECT_RESULT_APP_KEY_UNCONFIGURED,
            "status": status,
        }

    provider_result = PURETRACK_PROVIDER_CLIENT.login(
        email=email,
        password=password,
        config=PURETRACK_RUNTIME_CONFIG,
    )
    if provider_result.result == PURETRACK_CONNECT_RESULT_CONNECTED:
        try:
            save_puretrack_connected_session(
                db=db,
                current_user=current_user,
                provider_result=provider_result,
                email=email,
                audit_id=audit_id,
            )
        except ApiHTTPException as exc:
            if exc.code != ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE:
                raise
            clear_puretrack_provider_session(db, current_user.user.id)
            db.flush()
            status = build_puretrack_status_payload(
                db,
                current_user,
                connected=False,
                user_access=PURETRACK_PROVIDER_ACCESS_ERROR,
                account_label=None,
                error_code=ErrorCode.PURETRACK_PROVIDER_SESSION_UNAVAILABLE,
                audit_id=audit_id,
            )
            return {
                "result": PURETRACK_CONNECT_RESULT_ERROR,
                "status": status,
            }
        db.flush()
        return {
            "result": PURETRACK_CONNECT_RESULT_CONNECTED,
            "status": build_puretrack_status_payload(db, current_user, audit_id=audit_id),
        }

    if provider_result.result == PURETRACK_CONNECT_RESULT_AUTH_REJECTED:
        clear_puretrack_provider_session(db, current_user.user.id)
        db.flush()

    status = build_puretrack_status_payload(
        db,
        current_user,
        connected=False,
        user_access=puretrack_failure_user_access(provider_result),
        account_label=None,
        error_code=provider_result.error_code,
        retry_after_ms=provider_result.retry_after_ms,
        audit_id=audit_id,
    )
    return {
        "result": provider_result.result,
        "status": status,
    }


def disconnect_puretrack_account(
    db,
    current_user: CurrentUserRecord,
) -> dict[str, Any]:
    audit_id = puretrack_audit_id()
    disconnected = clear_puretrack_provider_session(db, current_user.user.id)
    db.flush()
    status = build_puretrack_status_payload(
        db,
        current_user,
        connected=False,
        user_access=PURETRACK_PROVIDER_ACCESS_NONE,
        account_label=None,
        verified_at_ms=to_epoch_ms(utcnow()),
        valid_until_ms=None,
        error_code=None,
        retry_after_ms=None,
        audit_id=audit_id,
    )
    return {
        "result": (
            PURETRACK_DISCONNECT_RESULT_DISCONNECTED
            if disconnected
            else PURETRACK_DISCONNECT_RESULT_NOT_CONNECTED
        ),
        "status": status,
    }


def build_free_livefollow_following_limit() -> LiveFollowFollowingLimit:
    return LiveFollowFollowingLimit(
        effective_tier="FREE",
        max_following=LIVEFOLLOW_FOLLOWING_CAP_BY_TIER["FREE"]
    )


def resolve_effective_livefollow_following_limit(
    db,
    user_id: str
) -> LiveFollowFollowingLimit:
    now_ms = to_epoch_ms(utcnow())
    snapshot = (
        db.query(AccountEntitlementSnapshot)
        .filter(AccountEntitlementSnapshot.user_id == user_id)
        .first()
    )
    if snapshot is None:
        return build_free_livefollow_following_limit()

    try:
        values = require_stored_entitlement_contract(snapshot)
    except ApiHTTPException as exc:
        if exc.code == ErrorCode.ENTITLEMENT_STATE_INVALID:
            return build_free_livefollow_following_limit()
        raise

    tier = values["tier"]
    if (
        tier != "FREE"
        and values["verificationState"] == "VERIFIED"
        and stored_paid_continuity_is_current(snapshot, values, now_ms)
    ):
        return LiveFollowFollowingLimit(
            effective_tier=tier,
            max_following=LIVEFOLLOW_FOLLOWING_CAP_BY_TIER[tier]
        )
    return build_free_livefollow_following_limit()


def count_livefollow_following(db, user_id: str) -> int:
    return (
        db.query(FollowEdge)
        .filter(FollowEdge.follower_user_id == user_id)
        .count()
    )


def count_livefollow_followers(db, user_id: str) -> int:
    return (
        db.query(FollowEdge)
        .filter(FollowEdge.followed_user_id == user_id)
        .count()
    )


def get_cached_relationship_counts(db, user_id: str) -> tuple[int, int]:
    counter = (
        db.query(UserRelationshipCounter)
        .filter(UserRelationshipCounter.user_id == user_id)
        .first()
    )
    if counter is None:
        return 0, 0
    return counter.followers_count, counter.following_count


def upsert_user_relationship_counter(
    db,
    user_id: str,
    followers_count: int,
    following_count: int,
    updated_at: datetime
) -> UserRelationshipCounter:
    counter = (
        db.query(UserRelationshipCounter)
        .filter(UserRelationshipCounter.user_id == user_id)
        .first()
    )
    if counter is None:
        counter = UserRelationshipCounter(
            user_id=user_id,
            followers_count=followers_count,
            following_count=following_count,
            updated_at=updated_at
        )
        db.add(counter)
        record_social_counter_counts(followers_count, following_count)
        return counter
    counter.followers_count = followers_count
    counter.following_count = following_count
    counter.updated_at = updated_at
    record_social_counter_counts(followers_count, following_count)
    return counter


def get_or_create_user_relationship_counter(
    db,
    user_id: str,
    updated_at: datetime
) -> UserRelationshipCounter:
    counter = (
        db.query(UserRelationshipCounter)
        .filter(UserRelationshipCounter.user_id == user_id)
        .first()
    )
    if counter is not None:
        return counter
    counter = UserRelationshipCounter(
        user_id=user_id,
        followers_count=count_livefollow_followers(db, user_id),
        following_count=count_livefollow_following(db, user_id),
        updated_at=updated_at
    )
    db.add(counter)
    return counter


def adjust_user_relationship_counter(
    db,
    user_id: str,
    followers_delta: int,
    following_delta: int,
    updated_at: datetime
) -> UserRelationshipCounter:
    counter = get_or_create_user_relationship_counter(db, user_id, updated_at)
    counter.followers_count = max(0, counter.followers_count + followers_delta)
    counter.following_count = max(0, counter.following_count + following_delta)
    counter.updated_at = updated_at
    record_social_counter_counts(counter.followers_count, counter.following_count)
    return counter


def increment_relationship_counters_for_follow_edge(
    db,
    follower_user_id: str,
    followed_user_id: str,
    updated_at: datetime
) -> None:
    adjust_user_relationship_counter(
        db,
        follower_user_id,
        followers_delta=0,
        following_delta=1,
        updated_at=updated_at
    )
    adjust_user_relationship_counter(
        db,
        followed_user_id,
        followers_delta=1,
        following_delta=0,
        updated_at=updated_at
    )


def decrement_relationship_counters_for_follow_edge(
    db,
    follower_user_id: str,
    followed_user_id: str,
    updated_at: datetime
) -> None:
    adjust_user_relationship_counter(
        db,
        follower_user_id,
        followers_delta=0,
        following_delta=-1,
        updated_at=updated_at
    )
    adjust_user_relationship_counter(
        db,
        followed_user_id,
        followers_delta=-1,
        following_delta=0,
        updated_at=updated_at
    )


def recount_user_relationship_counter(
    db,
    user_id: str,
    updated_at: datetime
) -> UserRelationshipCounter:
    return upsert_user_relationship_counter(
        db,
        user_id=user_id,
        followers_count=count_livefollow_followers(db, user_id),
        following_count=count_livefollow_following(db, user_id),
        updated_at=updated_at
    )


def recount_all_user_relationship_counters(
    db,
    updated_at: datetime
) -> int:
    user_ids = [user_id for user_id, in db.query(User.id).all()]
    for user_id in user_ids:
        recount_user_relationship_counter(db, user_id, updated_at)
    return len(user_ids)


def record_live_session_viewer(
    db,
    session_id: str,
    viewer_user_id: str,
    seen_at: datetime
) -> LiveSessionViewer:
    viewer = (
        db.query(LiveSessionViewer)
        .filter(
            LiveSessionViewer.session_id == session_id,
            LiveSessionViewer.viewer_user_id == viewer_user_id
        )
        .first()
    )
    if viewer is None:
        viewer = LiveSessionViewer(
            id=str(uuid.uuid4()),
            session_id=session_id,
            viewer_user_id=viewer_user_id,
            first_seen_at=seen_at,
            last_seen_at=seen_at
        )
        db.add(viewer)
        return viewer
    viewer.last_seen_at = seen_at
    return viewer


def count_live_session_unique_viewers(db, session_id: str) -> int:
    return (
        db.query(LiveSessionViewer)
        .filter(LiveSessionViewer.session_id == session_id)
        .count()
    )


def get_live_session_spectator_stats(
    db,
    session_id: str
) -> Optional[LiveSessionSpectatorStats]:
    return (
        db.query(LiveSessionSpectatorStats)
        .filter(LiveSessionSpectatorStats.session_id == session_id)
        .first()
    )


def upsert_live_session_spectator_stats(
    db,
    session_id: str,
    snapshot: LiveSpectatorStatsSnapshot,
    updated_at: datetime
) -> LiveSessionSpectatorStats:
    stats = get_live_session_spectator_stats(db, session_id)
    if stats is None:
        stats = LiveSessionSpectatorStats(
            session_id=session_id,
            first_position_at=snapshot.first_position_at,
            last_position_at=snapshot.last_position_at,
            position_count=snapshot.position_count,
            highest_altitude_msl_meters=snapshot.highest_altitude_msl_meters,
            distance_flown_meters=snapshot.distance_flown_meters,
            current_climb_sink_ms=snapshot.current_climb_sink_ms,
            best_short_window_climb_ms=snapshot.best_short_window_climb_ms,
            updated_at=updated_at
        )
        db.add(stats)
        return stats

    stats.first_position_at = snapshot.first_position_at
    stats.last_position_at = snapshot.last_position_at
    stats.position_count = snapshot.position_count
    stats.highest_altitude_msl_meters = snapshot.highest_altitude_msl_meters
    stats.distance_flown_meters = snapshot.distance_flown_meters
    stats.current_climb_sink_ms = snapshot.current_climb_sink_ms
    stats.best_short_window_climb_ms = snapshot.best_short_window_climb_ms
    stats.updated_at = updated_at
    return stats


def live_position_to_spectator_stats_point(
    position: LivePosition
) -> LiveSpectatorStatsPoint:
    return LiveSpectatorStatsPoint(
        lat=position.lat,
        lon=position.lon,
        altitude_msl_meters=position.alt,
        timestamp=position.timestamp
    )


def build_live_spectator_stats_snapshot_for_session(
    db,
    session_id: str
) -> Optional[LiveSpectatorStatsSnapshot]:
    positions = (
        db.query(LivePosition)
        .filter(LivePosition.session_id == session_id)
        .order_by(LivePosition.timestamp.asc(), LivePosition.id.asc())
        .all()
    )
    return build_live_spectator_stats_snapshot(
        [
            live_position_to_spectator_stats_point(position)
            for position in positions
        ]
    )


def latest_live_position_for_session(db, session_id: str) -> Optional[LivePosition]:
    return (
        db.query(LivePosition)
        .filter(LivePosition.session_id == session_id)
        .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
        .first()
    )


def previous_live_position_for_spectator_stats(
    db,
    session_id: str,
    before_timestamp: datetime
) -> Optional[LivePosition]:
    return (
        db.query(LivePosition)
        .filter(
            LivePosition.session_id == session_id,
            LivePosition.timestamp < before_timestamp
        )
        .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
        .first()
    )


def best_short_window_climb_for_position(
    db,
    session_id: str,
    current_position: LivePosition
) -> Optional[float]:
    current_point = live_position_to_spectator_stats_point(current_position)
    window_start = current_position.timestamp - timedelta(
        seconds=SPECTATOR_STATS_BEST_CLIMB_MAX_WINDOW_SECONDS
    )
    window_end = current_position.timestamp - timedelta(
        seconds=SPECTATOR_STATS_BEST_CLIMB_MIN_WINDOW_SECONDS
    )
    candidates = (
        db.query(LivePosition)
        .filter(
            LivePosition.session_id == session_id,
            LivePosition.timestamp >= window_start,
            LivePosition.timestamp <= window_end
        )
        .order_by(LivePosition.timestamp.asc(), LivePosition.id.asc())
        .all()
    )
    best_climb_ms: Optional[float] = None
    for candidate in candidates:
        climb_ms = spectator_stats_climb_rate_ms(
            start=live_position_to_spectator_stats_point(candidate),
            end=current_point,
            min_delta_seconds=SPECTATOR_STATS_BEST_CLIMB_MIN_WINDOW_SECONDS,
            max_delta_seconds=SPECTATOR_STATS_BEST_CLIMB_MAX_WINDOW_SECONDS
        )
        if climb_ms is None or climb_ms <= 0.0:
            continue
        if best_climb_ms is None or climb_ms > best_climb_ms:
            best_climb_ms = climb_ms
    return best_climb_ms


def build_incremental_live_spectator_stats_snapshot(
    existing: LiveSessionSpectatorStats,
    previous_position: LivePosition,
    current_position: LivePosition,
    best_short_window_climb_ms: Optional[float]
) -> LiveSpectatorStatsSnapshot:
    previous_point = live_position_to_spectator_stats_point(previous_position)
    current_point = live_position_to_spectator_stats_point(current_position)
    delta_seconds = spectator_stats_delta_seconds(
        previous_point.timestamp,
        current_point.timestamp
    )
    distance_delta_meters = 0.0
    if delta_seconds > 0.0:
        distance_delta_meters = haversine_m(
            previous_point.lat,
            previous_point.lon,
            current_point.lat,
            current_point.lon
        )
    current_climb_sink_ms = spectator_stats_climb_rate_ms(
        start=previous_point,
        end=current_point,
        min_delta_seconds=0.000001,
        max_delta_seconds=SPECTATOR_STATS_CURRENT_CLIMB_MAX_DELTA_SECONDS
    )
    prior_best_climb_ms = existing.best_short_window_climb_ms
    best_candidates = [
        value for value in (prior_best_climb_ms, best_short_window_climb_ms)
        if value is not None
    ]
    return LiveSpectatorStatsSnapshot(
        first_position_at=existing.first_position_at,
        last_position_at=current_point.timestamp,
        position_count=existing.position_count + 1,
        highest_altitude_msl_meters=max(
            existing.highest_altitude_msl_meters,
            current_point.altitude_msl_meters
        ),
        distance_flown_meters=existing.distance_flown_meters + distance_delta_meters,
        current_climb_sink_ms=current_climb_sink_ms,
        best_short_window_climb_ms=max(best_candidates) if best_candidates else None
    )


def update_live_session_spectator_stats_for_accepted_position(
    db,
    session_id: str,
    updated_at: datetime
) -> Optional[LiveSessionSpectatorStats]:
    current_position = latest_live_position_for_session(db, session_id)
    if current_position is None:
        return rebuild_live_session_spectator_stats(db, session_id, updated_at)
    existing = get_live_session_spectator_stats(db, session_id)
    if existing is None:
        return rebuild_live_session_spectator_stats(db, session_id, updated_at)
    previous_position = previous_live_position_for_spectator_stats(
        db,
        session_id,
        before_timestamp=current_position.timestamp
    )
    if previous_position is None:
        return rebuild_live_session_spectator_stats(db, session_id, updated_at)
    snapshot = build_incremental_live_spectator_stats_snapshot(
        existing=existing,
        previous_position=previous_position,
        current_position=current_position,
        best_short_window_climb_ms=best_short_window_climb_for_position(
            db,
            session_id,
            current_position
        )
    )
    return upsert_live_session_spectator_stats(
        db,
        session_id=session_id,
        snapshot=snapshot,
        updated_at=updated_at
    )


def rebuild_live_session_spectator_stats(
    db,
    session_id: str,
    updated_at: datetime
) -> Optional[LiveSessionSpectatorStats]:
    snapshot = build_live_spectator_stats_snapshot_for_session(db, session_id)
    if snapshot is None:
        existing = get_live_session_spectator_stats(db, session_id)
        if existing is not None:
            db.delete(existing)
        return None
    return upsert_live_session_spectator_stats(
        db,
        session_id=session_id,
        snapshot=snapshot,
        updated_at=updated_at
    )


def rebuild_all_live_session_spectator_stats(
    db,
    updated_at: datetime
) -> int:
    session_ids = [session_id for session_id, in db.query(LiveSession.id).all()]
    for session_id in session_ids:
        rebuild_live_session_spectator_stats(db, session_id, updated_at)
    return len(session_ids)


def relationship_limit_status(following_count: int, max_following: int) -> str:
    if following_count < max_following:
        return RELATIONSHIP_LIMIT_UNDER
    if following_count == max_following:
        return RELATIONSHIP_LIMIT_AT
    return RELATIONSHIP_LIMIT_OVER


def build_relationship_limits_response(db, user_id: str) -> dict[str, Any]:
    following_count = count_livefollow_following(db, user_id)
    limit = resolve_effective_livefollow_following_limit(db, user_id)
    return {
        "following_count": following_count,
        "max_following": limit.max_following,
        "status": relationship_limit_status(following_count, limit.max_following)
    }


def lock_user_for_following_capacity(db, user_id: str) -> User:
    user = (
        db.query(User)
        .filter(User.id == user_id)
        .with_for_update()
        .first()
    )
    if user is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.USER_NOT_FOUND,
            detail="user not found"
        )
    return user


def ensure_livefollow_following_capacity_available(db, follower_user_id: str) -> None:
    lock_user_for_following_capacity(db, follower_user_id)
    following_count = count_livefollow_following(db, follower_user_id)
    limit = resolve_effective_livefollow_following_limit(db, follower_user_id)
    if following_count >= limit.max_following:
        raise ApiHTTPException(
            status_code=409,
            code=ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
            detail="LiveFollow following limit exceeded"
        )


def sanitize_billing_audit_detail(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized = {}
        for key, child in value.items():
            if key in {"purchaseToken", "purchase_token"}:
                sanitized[key] = "<redacted>"
            else:
                sanitized[key] = sanitize_billing_audit_detail(child)
        return sanitized
    if isinstance(value, list):
        return [sanitize_billing_audit_detail(child) for child in value]
    return value


def create_billing_audit_record(
    db,
    user_id: Optional[str],
    event_type: str,
    purchase_token_hash: Optional[str],
    result: str,
    detail: dict[str, Any]
) -> str:
    audit_id = str(uuid.uuid4())
    redacted_subject = (
        f"purchase_token_sha256:{purchase_token_hash[:12]}"
        if purchase_token_hash is not None
        else "purchase_token_sha256:none"
    )
    sanitized_detail = sanitize_billing_audit_detail(detail)
    db.add(
        BillingAuditRecord(
            audit_id=audit_id,
            user_id=user_id,
            event_type=event_type,
            redacted_subject=redacted_subject,
            purchase_token_hash=purchase_token_hash,
            result=result,
            detail_json=json.dumps(
                sanitized_detail,
                separators=(",", ":"),
                sort_keys=True,
            ),
            created_at=utcnow(),
        )
    )
    return audit_id


def build_google_play_sync_response(
    result: str,
    entitlement_response: dict[str, Any],
    acknowledgement_required: bool,
    acknowledgement_completed: bool,
    acknowledgement_retry_after_ms: Optional[int],
    recovery_action: str,
    audit_id: str
) -> dict[str, Any]:
    return {
        "result": result,
        "entitlement": entitlement_response["entitlement"],
        "acknowledgementRequired": acknowledgement_required,
        "acknowledgementCompleted": acknowledgement_completed,
        "acknowledgementRetryAfterMs": acknowledgement_retry_after_ms,
        "recoveryAction": recovery_action,
        "nextRefreshAfterMs": None,
        "auditId": audit_id,
    }


def build_invalid_google_play_sync_response(
    db,
    current_user: CurrentUserRecord,
    purchase_token_hash: str,
    result: str,
    event_type: str,
    detail: dict[str, Any]
) -> dict[str, Any]:
    audit_id = create_billing_audit_record(
        db=db,
        user_id=current_user.user.id,
        event_type=event_type,
        purchase_token_hash=purchase_token_hash,
        result=result,
        detail=detail,
    )
    db.commit()
    return build_google_play_sync_response(
        result=result,
        entitlement_response=build_entitlement_response(db, current_user),
        acknowledgement_required=False,
        acknowledgement_completed=False,
        acknowledgement_retry_after_ms=None,
        recovery_action="NONE",
        audit_id=audit_id,
    )


def validate_verified_google_play_result(
    request_package_name: str,
    request_product_id: str,
    request_base_plan_id: str,
    verified: GooglePlayVerificationResult
) -> Optional[str]:
    if verified.package_name != request_package_name:
        return "verified package mismatch"
    if verified.product_id != request_product_id:
        return "verified product mismatch"
    if verified.base_plan_id != request_base_plan_id:
        return "verified base plan mismatch"
    if verified.product_id not in TIER_BY_PRODUCT_ID:
        return "verified product is invalid"
    if verified.base_plan_id not in PERIOD_BY_BASE_PLAN:
        return "verified base plan is invalid"
    if verified.subscription_status not in (
        PAID_CONTINUITY_STATUSES | DENIED_SUBSCRIPTION_STATUSES
    ):
        return "verified subscription status is invalid"
    if verified.subscription_status in PAID_CONTINUITY_STATUSES and verified.expiry_time_ms is None:
        return "verified paid continuity is missing expiry"
    return None


def validate_backend_derived_google_play_restore_result(
    request_package_name: str,
    verified: GooglePlayVerificationResult
) -> Optional[str]:
    if verified.package_name != request_package_name:
        return "verified package mismatch"
    if verified.product_id not in TIER_BY_PRODUCT_ID:
        return "verified product is invalid"
    if verified.base_plan_id not in PERIOD_BY_BASE_PLAN:
        return "verified base plan is invalid"
    if verified.subscription_status not in PAID_CONTINUITY_STATUSES:
        return "verified subscription is not in paid continuity"
    if verified.expiry_time_ms is None:
        return "verified paid continuity is missing expiry"
    return None


def google_play_sync_result_for_status(subscription_status: str) -> str:
    if subscription_status == "PENDING":
        return "ACCEPTED_PENDING"
    if subscription_status in {"EXPIRED", "REVOKED", "SUSPENDED"}:
        return "REVOKED_OR_EXPIRED"
    if subscription_status == "ERROR":
        return "ERROR"
    return "ACCEPTED_VERIFIED"


def is_superseded_google_play_purchase(purchase: BillingGooglePurchase) -> bool:
    return purchase.google_subscription_state == "SUPERSEDED_BY_LINKED_PURCHASE"


SAFE_BILLING_AUDIT_DETAIL_KEYS = frozenset({
    "packageName",
    "productId",
    "basePlanId",
    "subscriptionStatus",
    "acknowledgementRequired",
    "linkedPurchaseTokenHash",
    "source",
    "supersededPurchaseToken",
    "linkedPurchaseOwnedByDifferentAccount",
    "ownedByDifferentAccount",
})


def safe_billing_audit_detail(detail_json: Optional[str]) -> dict[str, Any]:
    if detail_json is None:
        return {}
    try:
        parsed = json.loads(detail_json)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        key: parsed[key]
        for key in SAFE_BILLING_AUDIT_DETAIL_KEYS
        if key in parsed
    }


def build_support_entitlement_snapshot(
    snapshot: Optional[AccountEntitlementSnapshot],
    now_ms: int,
) -> Optional[dict[str, Any]]:
    if snapshot is None:
        return None
    try:
        effective_values = effective_stored_entitlement_values(snapshot, now_ms)
    except ApiHTTPException as exc:
        if exc.code != ErrorCode.ENTITLEMENT_STATE_INVALID:
            raise
        effective_values = None
    effective_status = (
        effective_values["status"]
        if effective_values is not None
        else None
    )
    effective_paid_continuity = effective_status in PAID_CONTINUITY_STATUSES
    stale_paid_continuity = (
        snapshot.status in PAID_CONTINUITY_STATUSES
        and effective_status == "EXPIRED"
    )
    return {
        "tier": snapshot.tier,
        "billingPeriod": snapshot.billing_period,
        "status": snapshot.status,
        "effectiveStatus": effective_status,
        "stalePaidContinuity": stale_paid_continuity,
        "effectiveTrafficAllowed": (
            effective_values is not None
            and effective_values["tier"] == "PRO"
            and effective_values["verificationState"] == "VERIFIED"
            and effective_paid_continuity
        ),
        "source": snapshot.source,
        "verificationState": snapshot.verification_state,
        "productId": snapshot.product_id,
        "basePlanId": snapshot.base_plan_id,
        "expiryTimeMs": snapshot.expiry_time_ms,
        "autoRenewing": snapshot.auto_renewing,
        "willLoseAccessAtMs": snapshot.will_lose_access_at_ms,
        "verifiedAtMs": snapshot.verified_at_ms,
        "fetchedAtMs": snapshot.fetched_at_ms,
        "validUntilMs": snapshot.valid_until_ms,
        "effectiveValidUntilMs": (
            snapshot.valid_until_ms
            if effective_paid_continuity
            else None
        ),
        "staleAfterMs": snapshot.stale_after_ms,
        "hardRefreshAfterMs": snapshot.hard_refresh_after_ms,
        "recoveryAction": snapshot.recovery_action,
        "updatedAt": to_iso_utc(snapshot.updated_at),
    }


def build_support_purchase_snapshot(
    purchase: Optional[BillingGooglePurchase]
) -> Optional[dict[str, Any]]:
    if purchase is None:
        return None
    acknowledgement_state = purchase.acknowledgement_state
    return {
        "productId": purchase.product_id,
        "basePlanId": purchase.base_plan_id,
        "purchaseTokenHash": purchase.purchase_token_hash,
        "linkedPurchaseTokenHash": purchase.linked_purchase_token_hash,
        "googleSubscriptionState": purchase.google_subscription_state,
        "xcproSubscriptionStatus": purchase.xcpro_subscription_status,
        "acknowledgementState": acknowledgement_state,
        "acknowledgementPending": acknowledgement_state in {
            "ACK_PENDING",
            "ACK_RETRYABLE",
            "ACK_FAILED",
        },
        "acknowledgementRetryable": acknowledgement_state == "ACK_RETRYABLE",
        "expiryTimeMs": purchase.expiry_time_ms,
        "autoRenewing": purchase.auto_renewing,
        "lastVerifiedAtMs": purchase.last_verified_at_ms,
        "supersededReason": (
            purchase.google_subscription_state
            if is_superseded_google_play_purchase(purchase)
            else None
        ),
        "updatedAt": to_iso_utc(purchase.updated_at),
    }


def build_support_event_snapshot(
    event: Optional[BillingGoogleEvent]
) -> Optional[dict[str, Any]]:
    if event is None:
        return None
    return {
        "eventType": event.event_type,
        "packageName": event.package_name,
        "productId": event.product_id,
        "purchaseTokenHash": event.purchase_token_hash,
        "publishedAt": to_iso_utc(event.published_at),
        "processedAt": to_iso_utc(event.processed_at),
        "processingResult": event.processing_result,
        "auditId": event.audit_id,
        "updatedAt": to_iso_utc(event.updated_at),
    }


def build_support_audit_snapshot(
    audit: Optional[BillingAuditRecord]
) -> Optional[dict[str, Any]]:
    if audit is None:
        return None
    return {
        "auditId": audit.audit_id,
        "eventType": audit.event_type,
        "redactedSubject": audit.redacted_subject,
        "purchaseTokenHash": audit.purchase_token_hash,
        "result": audit.result,
        "safeDetail": safe_billing_audit_detail(audit.detail_json),
        "createdAt": to_iso_utc(audit.created_at),
    }


def select_support_current_purchase(
    snapshot: Optional[AccountEntitlementSnapshot],
    purchases: list[BillingGooglePurchase],
) -> Optional[BillingGooglePurchase]:
    if not purchases:
        return None
    if snapshot is not None and snapshot.product_id is not None and snapshot.base_plan_id is not None:
        for purchase in purchases:
            if (
                purchase.product_id == snapshot.product_id
                and purchase.base_plan_id == snapshot.base_plan_id
                and purchase.google_subscription_state == snapshot.status
                and not is_superseded_google_play_purchase(purchase)
            ):
                return purchase
    for purchase in purchases:
        if not is_superseded_google_play_purchase(purchase):
            return purchase
    return purchases[0]


def build_billing_support_snapshot(db, user_id: str) -> dict[str, Any]:
    now_ms = to_epoch_ms(utcnow())
    user = db.query(User).filter(User.id == user_id).first()
    auth_identity = (
        db.query(AuthIdentity)
        .filter(AuthIdentity.user_id == user_id)
        .order_by(AuthIdentity.last_seen_at.desc(), AuthIdentity.created_at.desc())
        .first()
    )
    entitlement = (
        db.query(AccountEntitlementSnapshot)
        .filter(AccountEntitlementSnapshot.user_id == user_id)
        .first()
    )
    purchases = (
        db.query(BillingGooglePurchase)
        .filter(BillingGooglePurchase.user_id == user_id)
        .order_by(
            BillingGooglePurchase.last_verified_at_ms.desc(),
            BillingGooglePurchase.updated_at.desc(),
        )
        .all()
    )
    current_purchase = select_support_current_purchase(entitlement, purchases)
    linked_previous_purchase = None
    if current_purchase is not None and current_purchase.linked_purchase_token_hash is not None:
        for purchase in purchases:
            if purchase.purchase_token_hash == current_purchase.linked_purchase_token_hash:
                linked_previous_purchase = purchase
                break

    purchase_token_hashes = [purchase.purchase_token_hash for purchase in purchases]
    latest_event = None
    if purchase_token_hashes:
        latest_event = (
            db.query(BillingGoogleEvent)
            .filter(BillingGoogleEvent.purchase_token_hash.in_(purchase_token_hashes))
            .order_by(BillingGoogleEvent.updated_at.desc(), BillingGoogleEvent.created_at.desc())
            .first()
        )
    latest_audit = (
        db.query(BillingAuditRecord)
        .filter(BillingAuditRecord.user_id == user_id)
        .order_by(BillingAuditRecord.created_at.desc())
        .first()
    )

    return {
        "accountOwner": {
            "userId": user_id,
            "exists": user is not None,
            "authProvider": auth_identity.provider if auth_identity is not None else None,
            "authProviderSubject": (
                auth_identity.provider_subject if auth_identity is not None else None
            ),
            "authProviderEmail": (
                auth_identity.provider_email if auth_identity is not None else None
            ),
            "authProviderEmailVerified": (
                auth_identity.provider_email_verified if auth_identity is not None else None
            ),
        },
        "entitlement": build_support_entitlement_snapshot(entitlement, now_ms),
        "currentPurchase": build_support_purchase_snapshot(current_purchase),
        "linkedPreviousPurchase": build_support_purchase_snapshot(linked_previous_purchase),
        "latestGoogleEvent": build_support_event_snapshot(latest_event),
        "latestAudit": build_support_audit_snapshot(latest_audit),
        "unavailableMetadata": {
            "latestOrderId": None,
            "latestSuccessfulOrderId": None,
            "regionCode": None,
            "offerId": None,
            "testPurchaseFlag": None,
            "deferredReplacementEvidence": None,
        },
    }


def verification_state_for_google_play_status(subscription_status: str) -> str:
    if subscription_status == "PENDING":
        return "UNVERIFIED"
    if subscription_status == "RECOVERY_REQUIRED":
        return "ACCOUNT_MISMATCH"
    return "VERIFIED"


def write_entitlement_snapshot_from_google_play_result(
    db,
    user_id: str,
    verified: GooglePlayVerificationResult,
    fetched_at_ms: int
) -> None:
    tier = TIER_BY_PRODUCT_ID[verified.product_id]
    billing_period = PERIOD_BY_BASE_PLAN[verified.base_plan_id]
    is_paid_continuity = verified.subscription_status in PAID_CONTINUITY_STATUSES
    db.merge(
        AccountEntitlementSnapshot(
            user_id=user_id,
            tier=tier,
            billing_period=billing_period,
            status=verified.subscription_status,
            source="GOOGLE_PLAY",
            verification_state=verification_state_for_google_play_status(
                verified.subscription_status
            ),
            product_id=verified.product_id,
            base_plan_id=verified.base_plan_id,
            expiry_time_ms=verified.expiry_time_ms,
            auto_renewing=verified.auto_renewing,
            will_lose_access_at_ms=(
                None
                if verified.subscription_status != "CANCELED_BUT_ACTIVE"
                else verified.expiry_time_ms
            ),
            verified_at_ms=fetched_at_ms,
            fetched_at_ms=fetched_at_ms,
            valid_until_ms=verified.expiry_time_ms if is_paid_continuity else None,
            stale_after_ms=(
                PAID_CONTINUITY_STALE_AFTER_MS
                if is_paid_continuity
                else DENIED_ENTITLEMENT_STALE_AFTER_MS
            ),
            hard_refresh_after_ms=(
                PAID_CONTINUITY_HARD_REFRESH_AFTER_MS
                if is_paid_continuity
                else DENIED_ENTITLEMENT_HARD_REFRESH_AFTER_MS
            ),
            recovery_action=(
                "CHOOSE_CORRECT_ACCOUNT"
                if verified.subscription_status == "RECOVERY_REQUIRED"
                else "NONE"
            ),
            created_at=utcnow(),
            updated_at=utcnow(),
        )
    )


def upsert_google_play_purchase_from_result(
    db,
    user_id: str,
    package_name: str,
    purchase_token_hash: str,
    verified: GooglePlayVerificationResult,
    fetched_at_ms: int,
    acknowledgement_state: str
) -> BillingGooglePurchase:
    now = utcnow()
    purchase = (
        db.query(BillingGooglePurchase)
        .filter(BillingGooglePurchase.purchase_token_hash == purchase_token_hash)
        .first()
    )
    linked_hash = (
        hash_purchase_token(verified.linked_purchase_token)
        if verified.linked_purchase_token is not None
        else None
    )
    if purchase is None:
        purchase = BillingGooglePurchase(
            id=str(uuid.uuid4()),
            user_id=user_id,
            package_name=package_name,
            product_id=verified.product_id,
            base_plan_id=verified.base_plan_id,
            purchase_token_hash=purchase_token_hash,
            linked_purchase_token_hash=linked_hash,
            google_subscription_state=verified.subscription_status,
            xcpro_subscription_status=verified.subscription_status,
            acknowledgement_state=acknowledgement_state,
            expiry_time_ms=verified.expiry_time_ms,
            auto_renewing=verified.auto_renewing,
            last_verified_at_ms=fetched_at_ms,
            created_at=now,
            updated_at=now,
        )
        db.add(purchase)
        return purchase

    purchase.package_name = package_name
    purchase.product_id = verified.product_id
    purchase.base_plan_id = verified.base_plan_id
    purchase.linked_purchase_token_hash = linked_hash
    purchase.google_subscription_state = verified.subscription_status
    purchase.xcpro_subscription_status = verified.subscription_status
    purchase.acknowledgement_state = acknowledgement_state
    purchase.expiry_time_ms = verified.expiry_time_ms
    purchase.auto_renewing = verified.auto_renewing
    purchase.last_verified_at_ms = fetched_at_ms
    purchase.updated_at = now
    return purchase


def apply_linked_purchase_token_policy(
    db,
    user_id: str,
    purchase_token_hash: str,
    verified: GooglePlayVerificationResult,
    event_type: str,
) -> tuple[Optional[GooglePlayProcessingOutcome], Optional[str]]:
    if verified.linked_purchase_token is None:
        return None, None
    linked_hash = hash_purchase_token(verified.linked_purchase_token)
    if linked_hash == purchase_token_hash:
        return None, linked_hash

    linked_purchase = (
        db.query(BillingGooglePurchase)
        .filter(BillingGooglePurchase.purchase_token_hash == linked_hash)
        .first()
    )
    if linked_purchase is None:
        return None, linked_hash

    if linked_purchase.user_id != user_id:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="ACCOUNT_MISMATCH",
            detail={
                "linkedPurchaseTokenHash": linked_hash,
                "linkedPurchaseOwnedByDifferentAccount": True,
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="ACCOUNT_MISMATCH",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CHOOSE_CORRECT_ACCOUNT",
        ), linked_hash

    linked_purchase.google_subscription_state = "SUPERSEDED_BY_LINKED_PURCHASE"
    linked_purchase.xcpro_subscription_status = "REVOKED"
    linked_purchase.expiry_time_ms = to_epoch_ms(utcnow())
    linked_purchase.auto_renewing = False
    linked_purchase.updated_at = utcnow()
    return None, linked_hash


def google_play_existing_purchase_blocking_outcome(
    db,
    user_id: str,
    package_name: str,
    purchase_token_hash: str,
    event_type: str,
    product_id: Optional[str],
    base_plan_id: Optional[str],
) -> Optional[GooglePlayProcessingOutcome]:
    existing_purchase = (
        db.query(BillingGooglePurchase)
        .filter(BillingGooglePurchase.purchase_token_hash == purchase_token_hash)
        .first()
    )
    audit_product_id = product_id
    audit_base_plan_id = base_plan_id
    if existing_purchase is not None:
        audit_product_id = audit_product_id or existing_purchase.product_id
        audit_base_plan_id = audit_base_plan_id or existing_purchase.base_plan_id

    if existing_purchase is not None and existing_purchase.user_id != user_id:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="TOKEN_ALREADY_OWNED",
            detail={
                "packageName": package_name,
                "productId": audit_product_id,
                "basePlanId": audit_base_plan_id,
                "ownedByDifferentAccount": True,
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="TOKEN_ALREADY_OWNED",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CHOOSE_CORRECT_ACCOUNT",
        )

    if existing_purchase is not None and is_superseded_google_play_purchase(existing_purchase):
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="SUPERSEDED_PURCHASE_IGNORED",
            detail={
                "packageName": package_name,
                "productId": audit_product_id,
                "basePlanId": audit_base_plan_id,
                "supersededPurchaseToken": True,
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="SUPERSEDED_PURCHASE_IGNORED",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="NONE",
        )
    return None


def process_verified_google_play_purchase_for_user(
    db,
    user_id: str,
    package_name: str,
    purchase_token_hash: str,
    purchase_token: str,
    verified: GooglePlayVerificationResult,
    event_type: str
) -> GooglePlayProcessingOutcome:
    existing_outcome = google_play_existing_purchase_blocking_outcome(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        event_type=event_type,
        product_id=verified.product_id,
        base_plan_id=verified.base_plan_id,
    )
    if existing_outcome is not None:
        return existing_outcome

    linked_outcome, linked_hash = apply_linked_purchase_token_policy(
        db=db,
        user_id=user_id,
        purchase_token_hash=purchase_token_hash,
        verified=verified,
        event_type=event_type,
    )
    if linked_outcome is not None:
        return linked_outcome

    result = google_play_sync_result_for_status(verified.subscription_status)
    should_acknowledge = (
        verified.acknowledgement_required
        and verified.subscription_status in PAID_CONTINUITY_STATUSES
    )
    acknowledgement_state = (
        "ACK_PENDING"
        if verified.acknowledgement_required
        else "NOT_REQUIRED"
    )
    fetched_at_ms = to_epoch_ms(utcnow())
    purchase = upsert_google_play_purchase_from_result(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        verified=verified,
        fetched_at_ms=fetched_at_ms,
        acknowledgement_state=acknowledgement_state,
    )
    write_entitlement_snapshot_from_google_play_result(
        db=db,
        user_id=user_id,
        verified=verified,
        fetched_at_ms=fetched_at_ms,
    )
    audit_id = create_billing_audit_record(
        db=db,
        user_id=user_id,
        event_type=event_type,
        purchase_token_hash=purchase_token_hash,
        result=result,
        detail={
            "packageName": package_name,
            "productId": verified.product_id,
            "basePlanId": verified.base_plan_id,
            "subscriptionStatus": verified.subscription_status,
            "acknowledgementRequired": verified.acknowledgement_required,
            "linkedPurchaseTokenHash": linked_hash,
        },
    )
    db.commit()

    acknowledgement_completed = False
    acknowledgement_retry_after_ms = None
    acknowledgement_transient_failure = False
    if should_acknowledge:
        try:
            acknowledgement_completed = GOOGLE_PLAY_PURCHASE_ACKNOWLEDGER.acknowledge_subscription(
                package_name=package_name,
                product_id=verified.product_id,
                purchase_token=purchase_token,
            )
        except GooglePlayVerificationTemporarilyUnavailable:
            acknowledgement_completed = False
            acknowledgement_transient_failure = True

        purchase = (
            db.query(BillingGooglePurchase)
            .filter(BillingGooglePurchase.id == purchase.id)
            .first()
        )
        if purchase is not None:
            purchase.acknowledgement_state = (
                "ACKNOWLEDGED"
                if acknowledgement_completed
                else (
                    "ACK_RETRYABLE"
                    if acknowledgement_transient_failure
                    else "ACK_FAILED"
                )
            )
            purchase.updated_at = utcnow()
            db.commit()
        if not acknowledgement_completed:
            acknowledgement_retry_after_ms = DENIED_ENTITLEMENT_STALE_AFTER_MS

    return GooglePlayProcessingOutcome(
        result=result,
        audit_id=audit_id,
        acknowledgement_required=verified.acknowledgement_required,
        acknowledgement_completed=acknowledgement_completed,
        acknowledgement_retry_after_ms=acknowledgement_retry_after_ms,
        recovery_action="NONE",
    )


def process_google_play_purchase_for_user(
    db,
    user_id: str,
    package_name: str,
    product_id: str,
    base_plan_id: str,
    purchase_token_hash: str,
    purchase_token: str,
    event_type: str
) -> GooglePlayProcessingOutcome:
    existing_outcome = google_play_existing_purchase_blocking_outcome(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        event_type=event_type,
        product_id=product_id,
        base_plan_id=base_plan_id,
    )
    if existing_outcome is not None:
        return existing_outcome

    try:
        verified = GOOGLE_PLAY_PURCHASE_VERIFIER.verify_subscription(
            package_name=package_name,
            product_id=product_id,
            purchase_token=purchase_token,
        )
    except GooglePlayVerificationTemporarilyUnavailable:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="VERIFICATION_TEMPORARILY_UNAVAILABLE",
            detail={
                "packageName": package_name,
                "productId": product_id,
                "basePlanId": base_plan_id,
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="VERIFICATION_TEMPORARILY_UNAVAILABLE",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=DENIED_ENTITLEMENT_STALE_AFTER_MS,
            recovery_action="RETRY_LATER",
        )
    except GooglePlayVerificationRejected as exc:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="ERROR",
            detail={
                "packageName": package_name,
                "productId": product_id,
                "basePlanId": base_plan_id,
                "error": str(exc),
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="ERROR",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CONTACT_SUPPORT",
        )

    contract_error = validate_verified_google_play_result(
        request_package_name=package_name,
        request_product_id=product_id,
        request_base_plan_id=base_plan_id,
        verified=verified,
    )
    if contract_error is not None:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="ERROR",
            detail={
                "packageName": package_name,
                "productId": product_id,
                "basePlanId": base_plan_id,
                "error": contract_error,
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="ERROR",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CONTACT_SUPPORT",
        )

    return process_verified_google_play_purchase_for_user(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        purchase_token=purchase_token,
        verified=verified,
        event_type=event_type,
    )


def process_google_play_restore_for_user(
    db,
    user_id: str,
    package_name: str,
    purchase_token_hash: str,
    purchase_token: str,
    event_type: str
) -> GooglePlayProcessingOutcome:
    existing_outcome = google_play_existing_purchase_blocking_outcome(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        event_type=event_type,
        product_id=None,
        base_plan_id=None,
    )
    if existing_outcome is not None:
        return existing_outcome

    try:
        verified = GOOGLE_PLAY_PURCHASE_VERIFIER.verify_subscription_token_only(
            package_name=package_name,
            purchase_token=purchase_token,
        )
    except GooglePlayVerificationTemporarilyUnavailable:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="VERIFICATION_TEMPORARILY_UNAVAILABLE",
            detail={
                "packageName": package_name,
                "source": "RESTORE",
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="VERIFICATION_TEMPORARILY_UNAVAILABLE",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=DENIED_ENTITLEMENT_STALE_AFTER_MS,
            recovery_action="RETRY_LATER",
        )
    except GooglePlayVerificationRejected as exc:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="ERROR",
            detail={
                "packageName": package_name,
                "error": str(exc),
                "source": "RESTORE",
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="ERROR",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CONTACT_SUPPORT",
        )

    contract_error = validate_backend_derived_google_play_restore_result(
        request_package_name=package_name,
        verified=verified,
    )
    if contract_error is not None:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=user_id,
            event_type=event_type,
            purchase_token_hash=purchase_token_hash,
            result="ERROR",
            detail={
                "packageName": package_name,
                "productId": verified.product_id,
                "basePlanId": verified.base_plan_id,
                "error": contract_error,
                "source": "RESTORE",
            },
        )
        db.commit()
        return GooglePlayProcessingOutcome(
            result="ERROR",
            audit_id=audit_id,
            acknowledgement_required=False,
            acknowledgement_completed=False,
            acknowledgement_retry_after_ms=None,
            recovery_action="CONTACT_SUPPORT",
        )

    return process_verified_google_play_purchase_for_user(
        db=db,
        user_id=user_id,
        package_name=package_name,
        purchase_token_hash=purchase_token_hash,
        purchase_token=purchase_token,
        verified=verified,
        event_type=event_type,
    )


def decode_pubsub_rtdn_payload(envelope: PubSubPushEnvelope) -> dict[str, Any]:
    message_id = trim_to_none(envelope.message.messageId) or trim_to_none(
        envelope.message.message_id
    )
    if message_id is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_RTDN_ENVELOPE,
            detail="Pub/Sub message id is required"
        )
    try:
        decoded_bytes = base64.b64decode(envelope.message.data, validate=True)
        decoded_json = json.loads(decoded_bytes.decode("utf-8"))
    except Exception:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_RTDN_ENVELOPE,
            detail="Pub/Sub message data is invalid"
        )
    package_name = trim_to_none(decoded_json.get("packageName"))
    test_notification = decoded_json.get("testNotification")
    if isinstance(test_notification, dict):
        if package_name is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_RTDN_ENVELOPE,
                detail="RTDN packageName is required"
            )
        return {
            "messageId": message_id,
            "publishedAt": envelope.message.publishTime or envelope.message.publish_time,
            "packageName": package_name,
            "productId": None,
            "purchaseToken": None,
            "purchaseTokenHash": None,
            "eventType": "TEST_NOTIFICATION",
        }

    subscription_notification = decoded_json.get("subscriptionNotification")
    if not isinstance(subscription_notification, dict):
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_RTDN_ENVELOPE,
            detail="subscription or test notification is required"
        )
    product_id = trim_to_none(subscription_notification.get("subscriptionId"))
    purchase_token = trim_to_none(subscription_notification.get("purchaseToken"))
    notification_type = str(
        subscription_notification.get("notificationType", "UNKNOWN")
    ).strip() or "UNKNOWN"
    if package_name is None or purchase_token is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_RTDN_ENVELOPE,
            detail="RTDN packageName and purchaseToken are required"
        )
    return {
        "messageId": message_id,
        "publishedAt": envelope.message.publishTime or envelope.message.publish_time,
        "packageName": package_name,
        "productId": product_id,
        "purchaseToken": purchase_token,
        "purchaseTokenHash": hash_purchase_token(purchase_token),
        "eventType": f"SUBSCRIPTION_NOTIFICATION_{notification_type}",
    }


def record_google_play_rtdn_event(
    db,
    decoded: dict[str, Any]
) -> tuple[BillingGoogleEvent, bool]:
    now = utcnow()
    event = BillingGoogleEvent(
        id=str(uuid.uuid4()),
        pubsub_message_id=decoded["messageId"],
        event_type=decoded["eventType"],
        package_name=decoded["packageName"],
        product_id=decoded["productId"],
        purchase_token_hash=decoded["purchaseTokenHash"],
        published_at=to_utc_naive(decoded["publishedAt"]) if decoded["publishedAt"] else None,
        processed_at=None,
        processing_result="RECORDED",
        audit_id=None,
        created_at=now,
        updated_at=now,
    )
    db.add(event)
    try:
        db.commit()
        return event, False
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(BillingGoogleEvent)
            .filter(BillingGoogleEvent.pubsub_message_id == decoded["messageId"])
            .first()
        )
        return existing, True


def should_reprocess_google_play_rtdn_event(
    event: Optional[BillingGoogleEvent]
) -> bool:
    if event is None:
        return False
    return (
        event.processed_at is None
        or event.processing_result in GOOGLE_PLAY_RTDN_RETRYABLE_RESULTS
    )


def process_google_play_rtdn_event(
    db,
    event: BillingGoogleEvent,
    decoded: dict[str, Any]
) -> GooglePlayRtdnIngestionResponse:
    try:
        validate_entitlement_package_name(decoded["packageName"])
    except ApiHTTPException:
        event.processing_result = "INVALID_PACKAGE"
        event.processed_at = utcnow()
        event.updated_at = utcnow()
        db.commit()
        return GooglePlayRtdnIngestionResponse(
            result="INVALID_PACKAGE",
            deduped=False,
            auditId=None,
        )

    if decoded["eventType"] == "TEST_NOTIFICATION":
        event.processing_result = "TEST_NOTIFICATION"
        event.processed_at = utcnow()
        event.updated_at = utcnow()
        db.commit()
        return GooglePlayRtdnIngestionResponse(
            result="TEST_NOTIFICATION",
            deduped=False,
            auditId=None,
        )

    purchase = (
        db.query(BillingGooglePurchase)
        .filter(BillingGooglePurchase.purchase_token_hash == decoded["purchaseTokenHash"])
        .first()
    )
    if purchase is None:
        audit_id = create_billing_audit_record(
            db=db,
            user_id=None,
            event_type=decoded["eventType"],
            purchase_token_hash=decoded["purchaseTokenHash"],
            result="TOKEN_NOT_OWNED",
            detail={
                "packageName": decoded["packageName"],
                "productId": decoded["productId"],
                "source": "RTDN",
            },
        )
        event.processing_result = "TOKEN_NOT_OWNED"
        event.audit_id = audit_id
        event.processed_at = utcnow()
        event.updated_at = utcnow()
        db.commit()
        return GooglePlayRtdnIngestionResponse(
            result="TOKEN_NOT_OWNED",
            deduped=False,
            auditId=audit_id,
        )

    product_id = purchase.product_id
    if validate_google_play_product_id(product_id) is None:
        event.processing_result = "INVALID_PRODUCT"
        event.processed_at = utcnow()
        event.updated_at = utcnow()
        db.commit()
        return GooglePlayRtdnIngestionResponse(
            result="INVALID_PRODUCT",
            deduped=False,
            auditId=None,
        )

    outcome = process_google_play_purchase_for_user(
        db=db,
        user_id=purchase.user_id,
        package_name=decoded["packageName"],
        product_id=product_id,
        base_plan_id=purchase.base_plan_id,
        purchase_token_hash=decoded["purchaseTokenHash"],
        purchase_token=decoded["purchaseToken"],
        event_type=decoded["eventType"],
    )
    event = db.query(BillingGoogleEvent).filter(BillingGoogleEvent.id == event.id).first()
    if event is not None:
        event.processing_result = outcome.result
        event.audit_id = outcome.audit_id
        event.processed_at = (
            None
            if outcome.result == "VERIFICATION_TEMPORARILY_UNAVAILABLE"
            else utcnow()
        )
        event.updated_at = utcnow()
        db.commit()
    if outcome.result == "VERIFICATION_TEMPORARILY_UNAVAILABLE":
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.GOOGLE_PLAY_VERIFICATION_UNAVAILABLE,
            detail="Google Play verification temporarily unavailable"
        )
    return GooglePlayRtdnIngestionResponse(
        result=outcome.result,
        deduped=False,
        auditId=outcome.audit_id,
    )


def ensure_profile_complete(
    profile: PilotProfile,
    detail: str = "handle and display_name are required"
) -> None:
    if profile.handle is None or profile.display_name is None:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.PROFILE_INCOMPLETE,
            detail=detail
        )


def build_relationship_state(
    lookup: RelationshipLookup,
    other_user_id: str
) -> str:
    if other_user_id in lookup.outgoing_pending:
        return SEARCH_RELATIONSHIP_OUTGOING_PENDING
    if other_user_id in lookup.incoming_pending:
        return SEARCH_RELATIONSHIP_INCOMING_PENDING

    current_follows_other = other_user_id in lookup.following
    other_follows_current = other_user_id in lookup.followed_by
    if current_follows_other and other_follows_current:
        return SEARCH_RELATIONSHIP_MUTUAL
    if current_follows_other:
        return SEARCH_RELATIONSHIP_FOLLOWING
    if other_follows_current:
        return SEARCH_RELATIONSHIP_FOLLOWED_BY
    return SEARCH_RELATIONSHIP_NONE


def load_relationship_lookup(
    db,
    current_user_id: str,
    other_user_ids: list[str]
) -> RelationshipLookup:
    if not other_user_ids:
        empty = frozenset()
        return RelationshipLookup(
            outgoing_pending=empty,
            incoming_pending=empty,
            following=empty,
            followed_by=empty
        )

    outgoing_pending = frozenset(
        row[0]
        for row in (
            db.query(FollowRequest.target_user_id)
            .filter(
                FollowRequest.requester_user_id == current_user_id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING,
                FollowRequest.target_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    incoming_pending = frozenset(
        row[0]
        for row in (
            db.query(FollowRequest.requester_user_id)
            .filter(
                FollowRequest.target_user_id == current_user_id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING,
                FollowRequest.requester_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    following = frozenset(
        row[0]
        for row in (
            db.query(FollowEdge.followed_user_id)
            .filter(
                FollowEdge.follower_user_id == current_user_id,
                FollowEdge.followed_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    followed_by = frozenset(
        row[0]
        for row in (
            db.query(FollowEdge.follower_user_id)
            .filter(
                FollowEdge.followed_user_id == current_user_id,
                FollowEdge.follower_user_id.in_(other_user_ids)
            )
            .all()
        )
    )
    return RelationshipLookup(
        outgoing_pending=outgoing_pending,
        incoming_pending=incoming_pending,
        following=following,
        followed_by=followed_by
    )


def parse_relationship_list_page_params(
    limit: int,
    cursor: Optional[str]
) -> tuple[int, int]:
    if limit < 1 or limit > RELATIONSHIP_LIST_MAX_LIMIT:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"limit must be between 1 and {RELATIONSHIP_LIST_MAX_LIMIT}"
        )
    if cursor is None:
        return limit, 0

    trimmed_cursor = cursor.strip()
    if not trimmed_cursor or not trimmed_cursor.isdigit():
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail="cursor must be a non-negative offset"
        )
    return limit, int(trimmed_cursor)


def normalize_bulk_relationship_status_user_ids(user_ids: list[str]) -> list[str]:
    if len(user_ids) > BULK_RELATIONSHIP_STATUS_MAX_IDS:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail=f"user_ids must contain at most {BULK_RELATIONSHIP_STATUS_MAX_IDS} ids"
        )

    normalized_user_ids: list[str] = []
    seen_user_ids: set[str] = set()
    for raw_user_id in user_ids:
        user_id = raw_user_id.strip()
        if not user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_ids must not contain blank ids"
            )
        if user_id not in seen_user_ids:
            normalized_user_ids.append(user_id)
            seen_user_ids.add(user_id)
    return normalized_user_ids


def approved_follow_edge_exists(
    db,
    follower_user_id: str,
    followed_user_id: str
) -> bool:
    return (
        db.query(FollowEdge)
        .filter(
            FollowEdge.follower_user_id == follower_user_id,
            FollowEdge.followed_user_id == followed_user_id
        )
        .first()
        is not None
    )


def load_blocked_counterpart_user_ids(db, user_id: str) -> set[str]:
    block_rows = (
        db.query(UserBlock.blocker_user_id, UserBlock.blocked_user_id)
        .filter(
            or_(
                UserBlock.blocker_user_id == user_id,
                UserBlock.blocked_user_id == user_id
            )
        )
        .all()
    )
    blocked_user_ids: set[str] = set()
    for blocker_user_id, blocked_user_id in block_rows:
        if blocker_user_id == user_id:
            blocked_user_ids.add(blocked_user_id)
        else:
            blocked_user_ids.add(blocker_user_id)
    return blocked_user_ids


def filter_blocked_counterparts(query, counterpart_column, blocked_user_ids: set[str]):
    if not blocked_user_ids:
        return query
    return query.filter(~counterpart_column.in_(blocked_user_ids))


def blocked_relationship_exists(db, first_user_id: str, second_user_id: str) -> bool:
    return (
        db.query(UserBlock)
        .filter(
            or_(
                and_(
                    UserBlock.blocker_user_id == first_user_id,
                    UserBlock.blocked_user_id == second_user_id
                ),
                and_(
                    UserBlock.blocker_user_id == second_user_id,
                    UserBlock.blocked_user_id == first_user_id
                )
            )
        )
        .first()
        is not None
    )


def build_relationship_policy_context(
    db,
    current_user_id: str,
    target_user_id: str
) -> RelationshipPolicyContext:
    if current_user_id == target_user_id:
        return RelationshipPolicyContext(
            current_user_id=current_user_id,
            target_user_id=target_user_id,
            is_blocked=False,
            current_follows_target=False,
            target_follows_current=False
        )
    return RelationshipPolicyContext(
        current_user_id=current_user_id,
        target_user_id=target_user_id,
        is_blocked=blocked_relationship_exists(db, current_user_id, target_user_id),
        current_follows_target=approved_follow_edge_exists(
            db,
            current_user_id,
            target_user_id
        ),
        target_follows_current=approved_follow_edge_exists(
            db,
            target_user_id,
            current_user_id
        )
    )


def relationship_policy_allows_profile_discovery(
    context: RelationshipPolicyContext,
    target_privacy: PrivacySetting
) -> bool:
    if context.is_self:
        return True
    if context.is_blocked:
        return False
    return target_privacy.discoverability == DEFAULT_DISCOVERABILITY


def relationship_policy_allows_connection_list(
    context: RelationshipPolicyContext,
    target_privacy: PrivacySetting
) -> bool:
    if context.is_self:
        return True
    if context.is_blocked:
        return False
    if target_privacy.connection_list_visibility == "public":
        return True
    if target_privacy.connection_list_visibility == "mutuals_only":
        return context.is_mutual
    return False


def relationship_policy_allows_follower_live_access(
    context: RelationshipPolicyContext
) -> bool:
    if context.is_self:
        return True
    if context.is_blocked:
        return False
    return context.current_follows_target


def notification_event_enabled_for_recipient(
    db,
    recipient_user_id: str,
    event_type: str
) -> bool:
    privacy = (
        db.query(PrivacySetting)
        .filter(PrivacySetting.user_id == recipient_user_id)
        .first()
    )
    if privacy is None:
        return DEFAULT_SOCIAL_NOTIFICATIONS_ENABLED
    if event_type in SOCIAL_NOTIFICATION_EVENT_TYPES:
        return privacy.social_notifications_enabled
    return privacy.live_notifications_enabled


def ensure_relationship_not_blocked(db, first_user_id: str, second_user_id: str) -> None:
    if blocked_relationship_exists(db, first_user_id, second_user_id):
        raise ApiHTTPException(
            status_code=409,
            code=ErrorCode.BLOCKED_RELATIONSHIP,
            detail="blocked relationship"
        )


def enqueue_follow_notification_event(
    db,
    event_type: str,
    recipient_user_id: str,
    actor_user_id: str,
    follow_request_id: str,
    occurred_at: datetime
) -> Optional[NotificationOutboxEvent]:
    if recipient_user_id == actor_user_id:
        return None
    if blocked_relationship_exists(db, recipient_user_id, actor_user_id):
        return None
    if not notification_event_enabled_for_recipient(db, recipient_user_id, event_type):
        return None

    dedupe_key = ":".join(
        [
            event_type,
            follow_request_id,
            recipient_user_id,
            to_iso_utc(occurred_at),
        ]
    )
    existing_event = (
        db.query(NotificationOutboxEvent)
        .filter(NotificationOutboxEvent.dedupe_key == dedupe_key)
        .first()
    )
    if existing_event is not None:
        return existing_event

    payload = {
        "event_type": event_type,
        "recipient_user_id": recipient_user_id,
        "actor_user_id": actor_user_id,
        "follow_request_id": follow_request_id,
    }
    notification_event = NotificationOutboxEvent(
        id=str(uuid.uuid4()),
        event_type=event_type,
        recipient_user_id=recipient_user_id,
        actor_user_id=actor_user_id,
        follow_request_id=follow_request_id,
        dedupe_key=dedupe_key,
        status=NOTIFICATION_OUTBOX_STATUS_PENDING,
        attempt_count=0,
        last_attempt_at=None,
        sent_at=None,
        last_error=None,
        last_error_retryable=None,
        payload_json=json.dumps(payload, sort_keys=True),
        created_at=occurred_at,
        updated_at=occurred_at,
    )
    db.add(notification_event)
    return notification_event


def truncate_notification_error(message: str) -> str:
    return message[:NOTIFICATION_OUTBOX_ERROR_MAX_LENGTH]


def build_fcm_data_payload(event: NotificationOutboxEvent) -> dict[str, str]:
    payload = json.loads(event.payload_json)
    return {
        "event_type": str(payload.get("event_type", event.event_type)),
        "recipient_user_id": str(payload.get("recipient_user_id", event.recipient_user_id)),
        "actor_user_id": str(payload.get("actor_user_id", event.actor_user_id)),
        "follow_request_id": str(payload.get("follow_request_id", event.follow_request_id or "")),
    }


def mark_notification_delivery_result(
    event: NotificationOutboxEvent,
    now: datetime,
    status: str,
    error: Optional[str],
    retryable: Optional[bool],
) -> None:
    event.status = status
    event.updated_at = now
    event.last_error = truncate_notification_error(error) if error else None
    event.last_error_retryable = retryable
    if status == NOTIFICATION_OUTBOX_STATUS_SENT:
        event.sent_at = now


def deliver_notification_event(
    db,
    event: NotificationOutboxEvent,
    sender: Any,
    now: datetime,
) -> dict[str, int]:
    event.attempt_count += 1
    event.last_attempt_at = now
    event.updated_at = now

    active_tokens = (
        db.query(DevicePushToken)
        .filter(
            DevicePushToken.user_id == event.recipient_user_id,
            DevicePushToken.platform == PUSH_PLATFORM_ANDROID,
            DevicePushToken.provider == PUSH_PROVIDER_FCM,
            DevicePushToken.revoked_at.is_(None),
        )
        .order_by(DevicePushToken.created_at)
        .all()
    )
    if not active_tokens:
        mark_notification_delivery_result(
            event,
            now,
            NOTIFICATION_OUTBOX_STATUS_FAILED,
            "no active push tokens",
            False,
        )
        return {"token_attempts": 0, "tokens_sent": 0}

    payload = build_fcm_data_payload(event)
    token_attempts = 0
    tokens_sent = 0
    retryable_errors = 0
    terminal_errors = 0
    last_error: Optional[str] = None
    last_retryable: Optional[bool] = None
    for push_token in active_tokens:
        token_attempts += 1
        try:
            sender.send_message(decrypt_push_token(push_token.token_ciphertext), payload)
            tokens_sent += 1
        except FcmDeliveryTemporarilyUnavailable as exc:
            retryable_errors += 1
            last_error = str(exc) or "FCM temporarily unavailable"
            last_retryable = True
        except (FcmDeliveryRejected, ValueError) as exc:
            terminal_errors += 1
            last_error = str(exc) or "FCM message rejected"
            last_retryable = False
        except ApiHTTPException as exc:
            retryable_errors += 1
            last_error = str(exc.detail)
            last_retryable = True

    if tokens_sent > 0:
        mark_notification_delivery_result(
            event,
            now,
            NOTIFICATION_OUTBOX_STATUS_SENT,
            None,
            None,
        )
    elif retryable_errors > 0:
        mark_notification_delivery_result(
            event,
            now,
            NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED,
            last_error or "FCM temporarily unavailable",
            True,
        )
    else:
        mark_notification_delivery_result(
            event,
            now,
            NOTIFICATION_OUTBOX_STATUS_FAILED,
            last_error or "FCM message rejected",
            last_retryable if last_retryable is not None else False,
        )

    return {"token_attempts": token_attempts, "tokens_sent": tokens_sent}


def normalize_notification_delivery_limit(limit: int) -> int:
    return max(1, min(limit, NOTIFICATION_DELIVERY_MAX_LIMIT))


def deliver_pending_notification_events(
    limit: int = NOTIFICATION_DELIVERY_DEFAULT_LIMIT,
    sender: Optional[Any] = None,
) -> dict[str, int]:
    db = SessionLocal()
    try:
        resolved_limit = normalize_notification_delivery_limit(limit)
        events = (
            db.query(NotificationOutboxEvent)
            .filter(NotificationOutboxEvent.status.in_(NOTIFICATION_OUTBOX_DELIVERABLE_STATUSES))
            .order_by(NotificationOutboxEvent.created_at)
            .limit(resolved_limit)
            .all()
        )
        record_notification_fanout_metric(len(events))
        delivery_sender = sender or FCM_NOTIFICATION_SENDER
        summary = {
            "events_attempted": 0,
            "events_sent": 0,
            "events_retryable_failed": 0,
            "events_failed": 0,
            "token_attempts": 0,
            "tokens_sent": 0,
        }
        for event in events:
            now = utcnow()
            token_summary = deliver_notification_event(
                db=db,
                event=event,
                sender=delivery_sender,
                now=now,
            )
            summary["events_attempted"] += 1
            summary["token_attempts"] += token_summary["token_attempts"]
            summary["tokens_sent"] += token_summary["tokens_sent"]
            if event.status == NOTIFICATION_OUTBOX_STATUS_SENT:
                summary["events_sent"] += 1
            elif event.status == NOTIFICATION_OUTBOX_STATUS_RETRYABLE_FAILED:
                summary["events_retryable_failed"] += 1
            elif event.status == NOTIFICATION_OUTBOX_STATUS_FAILED:
                summary["events_failed"] += 1
        db.commit()
        return summary
    finally:
        db.close()


def ensure_can_read_relationship_list(
    db,
    current_user_id: str,
    target_user_id: str,
    target_privacy: PrivacySetting,
    error_code: str
) -> None:
    context = build_relationship_policy_context(db, current_user_id, target_user_id)
    if relationship_policy_allows_connection_list(context, target_privacy):
        return
    raise ApiHTTPException(
        status_code=403,
        code=error_code,
        detail="not authorized to view relationship list"
    )


def load_livefollow_follow_count_maps(
    db,
    user_ids: list[str]
) -> tuple[dict[str, int], dict[str, int]]:
    if not user_ids:
        return {}, {}

    unique_user_ids = sorted(set(user_ids))
    followers_count_by_user_id: dict[str, int] = {}
    following_count_by_user_id: dict[str, int] = {}
    missing_user_ids: list[str] = []
    for user_id in unique_user_ids:
        cached_counts = get_cached_social_counts(user_id)
        if cached_counts is None:
            missing_user_ids.append(user_id)
            continue
        followers_count_by_user_id[user_id] = cached_counts[0]
        following_count_by_user_id[user_id] = cached_counts[1]

    if missing_user_ids:
        counters = (
            db.query(UserRelationshipCounter)
            .filter(UserRelationshipCounter.user_id.in_(missing_user_ids))
            .all()
        )
        counters_by_user_id = {
            counter.user_id: counter
            for counter in counters
        }
        for user_id in missing_user_ids:
            counter = counters_by_user_id.get(user_id)
            followers_count = counter.followers_count if counter is not None else 0
            following_count = counter.following_count if counter is not None else 0
            followers_count_by_user_id[user_id] = followers_count
            following_count_by_user_id[user_id] = following_count
            set_cached_social_counts(user_id, followers_count, following_count)
    return followers_count_by_user_id, following_count_by_user_id


def build_relationship_list_item_response(
    profile: PilotProfile,
    lookup: RelationshipLookup,
    followers_count_by_user_id: dict[str, int],
    following_count_by_user_id: dict[str, int],
    favorite_user_ids: set[str]
) -> dict[str, Any]:
    response = build_user_summary(profile)
    response["followers_count"] = followers_count_by_user_id.get(profile.user_id, 0)
    response["following_count"] = following_count_by_user_id.get(profile.user_id, 0)
    response["relationship_state"] = build_relationship_state(lookup, profile.user_id)
    response["is_favorite"] = profile.user_id in favorite_user_ids
    return response


def build_relationship_list_page_response(
    db,
    current_user_id: str,
    profiles: list[PilotProfile],
    total: int,
    limit: int,
    offset: int
) -> dict[str, Any]:
    user_ids = [profile.user_id for profile in profiles]
    lookup = load_relationship_lookup(db, current_user_id, user_ids)
    followers_count_by_user_id, following_count_by_user_id = (
        load_livefollow_follow_count_maps(db, user_ids)
    )
    favorite_user_ids = load_favorite_follow_user_ids(db, current_user_id, user_ids)
    next_offset = offset + len(profiles)
    next_cursor = str(next_offset) if next_offset < total else None
    return {
        "total": total,
        "items": [
            build_relationship_list_item_response(
                profile,
                lookup,
                followers_count_by_user_id,
                following_count_by_user_id,
                favorite_user_ids
            )
            for profile in profiles
        ],
        "next_cursor": next_cursor
    }


def get_searchable_target_or_404(db, user_id: str) -> tuple[PilotProfile, PrivacySetting]:
    profile = db.query(PilotProfile).filter(PilotProfile.user_id == user_id).first()
    if profile is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.USER_NOT_FOUND,
            detail="user not found"
        )
    ensure_profile_complete(profile)
    privacy = db.query(PrivacySetting).filter(PrivacySetting.user_id == user_id).first()
    if privacy is None:
        now = utcnow()
        privacy = PrivacySetting(
            user_id=user_id,
            discoverability=DEFAULT_DISCOVERABILITY,
            follow_policy=DEFAULT_FOLLOW_POLICY,
            default_live_visibility=DEFAULT_LIVE_VISIBILITY,
            connection_list_visibility=DEFAULT_CONNECTION_LIST_VISIBILITY,
            social_notifications_enabled=DEFAULT_SOCIAL_NOTIFICATIONS_ENABLED,
            live_notifications_enabled=DEFAULT_LIVE_NOTIFICATIONS_ENABLED,
            created_at=now,
            updated_at=now
        )
        db.add(privacy)
        db.flush()
    return profile, privacy


def get_user_record_or_404(db, user_id: str) -> User:
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.USER_NOT_FOUND,
            detail="user not found"
        )
    return user


def remove_relationship_rows_between_users(
    db,
    first_user_id: str,
    second_user_id: str,
    updated_at: datetime
) -> None:
    follow_edges = (
        db.query(FollowEdge)
        .filter(
            or_(
                and_(
                    FollowEdge.follower_user_id == first_user_id,
                    FollowEdge.followed_user_id == second_user_id
                ),
                and_(
                    FollowEdge.follower_user_id == second_user_id,
                    FollowEdge.followed_user_id == first_user_id
                )
            )
        )
        .all()
    )
    for edge in follow_edges:
        decrement_relationship_counters_for_follow_edge(
            db,
            edge.follower_user_id,
            edge.followed_user_id,
            updated_at
        )
        db.delete(edge)

    pending_requests = (
        db.query(FollowRequest)
        .filter(
            FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING,
            or_(
                and_(
                    FollowRequest.requester_user_id == first_user_id,
                    FollowRequest.target_user_id == second_user_id
                ),
                and_(
                    FollowRequest.requester_user_id == second_user_id,
                    FollowRequest.target_user_id == first_user_id
                )
            )
        )
        .all()
    )
    for request in pending_requests:
        db.delete(request)

    favorite_rows = (
        db.query(FavoriteFollow)
        .filter(
            or_(
                and_(
                    FavoriteFollow.user_id == first_user_id,
                    FavoriteFollow.favorite_user_id == second_user_id
                ),
                and_(
                    FavoriteFollow.user_id == second_user_id,
                    FavoriteFollow.favorite_user_id == first_user_id
                )
            )
        )
        .all()
    )
    for favorite in favorite_rows:
        db.delete(favorite)


def follow_edge_exists(db, follower_user_id: str, followed_user_id: str) -> bool:
    return (
        db.query(FollowEdge)
        .filter(
            FollowEdge.follower_user_id == follower_user_id,
            FollowEdge.followed_user_id == followed_user_id
        )
        .first()
        is not None
    )


def load_favorite_follow_user_ids(
    db,
    user_id: str,
    candidate_user_ids: list[str]
) -> set[str]:
    if not candidate_user_ids:
        return set()
    rows = (
        db.query(FavoriteFollow.favorite_user_id)
        .filter(
            FavoriteFollow.user_id == user_id,
            FavoriteFollow.favorite_user_id.in_(sorted(set(candidate_user_ids)))
        )
        .all()
    )
    return {row[0] for row in rows}


def remove_favorite_follow(db, user_id: str, favorite_user_id: str) -> bool:
    favorite = (
        db.query(FavoriteFollow)
        .filter(
            FavoriteFollow.user_id == user_id,
            FavoriteFollow.favorite_user_id == favorite_user_id
        )
        .first()
    )
    if favorite is None:
        return False
    db.delete(favorite)
    return True


def ensure_follow_edge(
    db,
    follower_user_id: str,
    followed_user_id: str,
    now: datetime
) -> None:
    lock_user_for_following_capacity(db, follower_user_id)
    edge = (
        db.query(FollowEdge)
        .filter(
            FollowEdge.follower_user_id == follower_user_id,
            FollowEdge.followed_user_id == followed_user_id
        )
        .first()
    )
    if edge is None:
        following_count = count_livefollow_following(db, follower_user_id)
        limit = resolve_effective_livefollow_following_limit(db, follower_user_id)
        if following_count >= limit.max_following:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.LIVEFOLLOW_FOLLOWING_LIMIT_EXCEEDED,
                detail="LiveFollow following limit exceeded"
            )
        increment_relationship_counters_for_follow_edge(
            db,
            follower_user_id,
            followed_user_id,
            now
        )
        invalidate_social_cache_for_users(follower_user_id, followed_user_id)
        db.add(
            FollowEdge(
                follower_user_id=follower_user_id,
                followed_user_id=followed_user_id,
                created_at=now,
                updated_at=now
            )
        )
        return
    edge.updated_at = now


def build_search_result_response(
    profile: PilotProfile,
    lookup: RelationshipLookup
) -> dict[str, Optional[str]]:
    response = build_user_summary(profile)
    response["relationship_state"] = build_relationship_state(lookup, profile.user_id)
    return response


def build_follow_request_response(
    current_user_id: str,
    follow_request: FollowRequest,
    counterpart_profile: PilotProfile,
    lookup: RelationshipLookup
) -> dict[str, Any]:
    direction = (
        "outgoing"
        if follow_request.requester_user_id == current_user_id
        else "incoming"
    )
    return {
        "request_id": follow_request.id,
        "status": follow_request.status,
        "direction": direction,
        "created_at": to_iso_utc(follow_request.created_at),
        "updated_at": to_iso_utc(follow_request.updated_at),
        "counterpart": build_user_summary(counterpart_profile),
        "relationship_state": build_relationship_state(lookup, counterpart_profile.user_id)
    }


def apply_profile_patch(
    profile: PilotProfile,
    request: MeProfilePatchRequest
) -> None:
    fields = requested_fields(request)
    if "handle" in fields:
        if request.handle is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_HANDLE,
                detail="handle is required"
            )
        normalized_handle = normalize_handle(request.handle)
        profile.handle = normalized_handle
        profile.handle_normalized = normalized_handle

    if "display_name" in fields:
        profile.display_name = normalize_display_name(request.display_name)

    if "comp_number" in fields:
        profile.comp_number = trim_to_none(request.comp_number)

    ensure_profile_complete(profile)

    profile.updated_at = utcnow()


def apply_privacy_patch(
    privacy: PrivacySetting,
    request: MePrivacyPatchRequest
) -> None:
    fields = requested_fields(request)
    if "discoverability" in fields:
        if request.discoverability is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="discoverability is required"
            )
        privacy.discoverability = validate_privacy_value(
            "discoverability",
            request.discoverability.strip()
        )
    if "follow_policy" in fields:
        if request.follow_policy is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="follow_policy is required"
            )
        privacy.follow_policy = validate_privacy_value(
            "follow_policy",
            request.follow_policy.strip()
        )
    if "default_live_visibility" in fields:
        if request.default_live_visibility is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="default_live_visibility is required"
            )
        privacy.default_live_visibility = validate_privacy_value(
            "default_live_visibility",
            request.default_live_visibility.strip()
        )
    if "connection_list_visibility" in fields:
        if request.connection_list_visibility is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="connection_list_visibility is required"
            )
        privacy.connection_list_visibility = validate_privacy_value(
            "connection_list_visibility",
            request.connection_list_visibility.strip()
        )
    if "social_notifications_enabled" in fields:
        if request.social_notifications_enabled is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="social_notifications_enabled is required"
            )
        privacy.social_notifications_enabled = request.social_notifications_enabled
    if "live_notifications_enabled" in fields:
        if request.live_notifications_enabled is None:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.INVALID_PRIVACY_SETTING,
                detail="live_notifications_enabled is required"
            )
        privacy.live_notifications_enabled = request.live_notifications_enabled
    privacy.updated_at = utcnow()


def get_session_or_404(db, session_id: str) -> LiveSession:
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.SESSION_NOT_FOUND,
            detail="session not found"
        )
    return session


def require_write_access(session: LiveSession, x_session_token: Optional[str]) -> None:
    if not x_session_token:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.MISSING_SESSION_TOKEN,
            detail="missing X-Session-Token header"
        )

    if not session.write_token_hash:
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.SESSION_TOKEN_UNAVAILABLE,
            detail="write token unavailable for this session"
        )

    if not secrets.compare_digest(session.write_token_hash, hash_token(x_session_token)):
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.INVALID_SESSION_TOKEN,
            detail="invalid session token"
        )


def compute_effective_status(session: LiveSession) -> str:
    if session.status == "ended":
        return "ended"

    if session.last_position_at is None:
        return "active"

    age = utcnow() - session.last_position_at
    if age > timedelta(seconds=STALE_AFTER_SECONDS):
        return "stale"

    return "active"


def validate_live_visibility(raw_value: Optional[str]) -> str:
    normalized = (raw_value or "").strip()
    if normalized not in DEFAULT_LIVE_VISIBILITY_VALUES:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_PRIVACY_SETTING,
            detail=f"visibility must be one of {sorted(DEFAULT_LIVE_VISIBILITY_VALUES)}"
        )
    return normalized


def effective_live_visibility(session: LiveSession) -> str:
    return (session.visibility or LIVE_VISIBILITY_PUBLIC).strip() or LIVE_VISIBILITY_PUBLIC


def public_share_code_for(session: LiveSession) -> Optional[str]:
    if effective_live_visibility(session) != LIVE_VISIBILITY_PUBLIC:
        return None
    return session.share_code


def is_public_live_session(session: LiveSession) -> bool:
    return effective_live_visibility(session) == LIVE_VISIBILITY_PUBLIC


def can_user_view_live_session(
    db,
    viewer_user_id: str,
    session: LiveSession
) -> bool:
    owner_user_id = session.owner_user_id
    if owner_user_id == viewer_user_id:
        return True

    visibility = effective_live_visibility(session)
    if visibility == LIVE_VISIBILITY_PUBLIC:
        return True
    if owner_user_id is None:
        return False
    if visibility != LIVE_VISIBILITY_FOLLOWERS:
        return False

    context = build_relationship_policy_context(db, viewer_user_id, owner_user_id)
    return relationship_policy_allows_follower_live_access(context)


def record_authorized_live_session_viewer_if_needed(
    db,
    session: LiveSession,
    viewer_user_id: str,
    seen_at: datetime
) -> bool:
    if session.status == "ended":
        return False
    if session.owner_user_id is None or session.owner_user_id == viewer_user_id:
        return False

    record_live_session_viewer(db, session.id, viewer_user_id, seen_at)
    return True


def get_owned_live_session_or_404(
    db,
    session_id: str,
    owner_user_id: str
) -> LiveSession:
    session = (
        db.query(LiveSession)
        .filter(
            LiveSession.id == session_id,
            LiveSession.owner_user_id == owner_user_id
        )
        .first()
    )
    if session is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.SESSION_NOT_FOUND,
            detail="session not found"
        )
    return session


def select_latest_owned_live_session(
    db,
    owner_user_id: str
) -> Optional[LiveSession]:
    return (
        db.query(LiveSession)
        .filter(
            LiveSession.owner_user_id == owner_user_id,
            LiveSession.status != "ended"
        )
        .order_by(
            LiveSession.last_position_at.desc(),
            LiveSession.created_at.desc(),
            LiveSession.id.asc()
        )
        .first()
    )


def end_active_owned_live_sessions(
    db,
    owner_user_id: str,
    now: datetime
) -> list[str]:
    sessions = (
        db.query(LiveSession)
        .filter(
            LiveSession.owner_user_id == owner_user_id,
            LiveSession.status != "ended"
        )
        .all()
    )
    ended_session_ids = []
    for session in sessions:
        session.status = "ended"
        session.ended_at = now
        ended_session_ids.append(session.id)
    return ended_session_ids


def build_live_session_command_response(
    session: LiveSession,
    write_token: Optional[str] = None
) -> dict[str, Any]:
    response = {
        "session_id": session.id,
        "status": compute_effective_status(session),
        "visibility": effective_live_visibility(session),
        "owner_user_id": session.owner_user_id,
        "share_code": public_share_code_for(session)
    }
    if write_token is not None:
        response["write_token"] = write_token
    return response


def validate_position_payload(p: Position, position_ts: datetime) -> None:
    validate_lat_lon(
        p.lat,
        p.lon,
        "position",
        ErrorCode.POSITION_COORDINATE_OUT_OF_RANGE
    )

    if p.alt < MIN_REASONABLE_ALT_M or p.alt > MAX_REASONABLE_ALT_M:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_ALT_OUT_OF_RANGE,
            detail="position.alt out of range"
        )

    if p.speed < 0 or p.speed > MAX_REASONABLE_SPEED:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_SPEED_OUT_OF_RANGE,
            detail="position.speed out of range"
        )

    if p.heading < 0 or p.heading > 360:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_HEADING_OUT_OF_RANGE,
            detail="position.heading out of range"
        )

    if position_ts > utcnow() + timedelta(seconds=MAX_POSITION_FUTURE_SKEW_SECONDS):
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.POSITION_TIMESTAMP_IN_FUTURE,
            detail="position.timestamp too far in the future"
        )


def validate_task_payload(req: TaskUpsertRequest) -> dict:
    if req.clear_task:
        if req.task_name is not None or req.task is not None:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_CLEAR_PAYLOAD_INVALID,
                detail="clear_task cannot be combined with task_name or task"
            )
        return {"clear_task": True}

    task_name = (req.task_name or "").strip()
    if not task_name:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.TASK_NAME_REQUIRED,
            detail="task_name is required"
        )

    task = req.task or {}
    turnpoints = task.get("turnpoints")

    if not isinstance(turnpoints, list) or len(turnpoints) < 2:
        raise ApiHTTPException(
            status_code=400,
            code=ErrorCode.TASK_TURNPOINTS_INVALID,
            detail="task.turnpoints must contain at least 2 items"
        )

    for idx, tp in enumerate(turnpoints):
        if not isinstance(tp, dict):
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_INVALID,
                detail=f"task.turnpoints[{idx}] must be an object"
            )

        name = str(tp.get("name", "")).strip()
        tp_type = str(tp.get("type", "")).strip()

        if not name:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_NAME_REQUIRED,
                detail=f"task.turnpoints[{idx}].name is required"
            )
        if not tp_type:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_TYPE_REQUIRED,
                detail=f"task.turnpoints[{idx}].type is required"
            )
        if "lat" not in tp or "lon" not in tp:
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_TURNPOINT_COORDINATES_REQUIRED,
                detail=f"task.turnpoints[{idx}] requires lat/lon"
            )

        lat = parse_number(
            tp.get("lat"),
            f"task.turnpoints[{idx}].lat",
            ErrorCode.INVALID_NUMERIC_VALUE
        )
        lon = parse_number(
            tp.get("lon"),
            f"task.turnpoints[{idx}].lon",
            ErrorCode.INVALID_NUMERIC_VALUE
        )
        validate_lat_lon(
            lat,
            lon,
            f"task.turnpoints[{idx}]",
            ErrorCode.TASK_COORDINATE_OUT_OF_RANGE
        )

        if "radius_m" in tp and tp.get("radius_m") is not None:
            validate_radius(
                tp.get("radius_m"),
                f"task.turnpoints[{idx}].radius_m",
                ErrorCode.TASK_RADIUS_OUT_OF_RANGE
            )

    for boundary_name in ["start", "finish"]:
        boundary = task.get(boundary_name)
        if boundary is None:
            continue

        if not isinstance(boundary, dict):
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_BOUNDARY_INVALID,
                detail=f"task.{boundary_name} must be an object"
            )

        if "type" in boundary and not str(boundary.get("type", "")).strip():
            raise ApiHTTPException(
                status_code=400,
                code=ErrorCode.TASK_BOUNDARY_TYPE_INVALID,
                detail=f"task.{boundary_name}.type is invalid"
            )

        if "radius_m" in boundary and boundary.get("radius_m") is not None:
            validate_radius(
                boundary.get("radius_m"),
                f"task.{boundary_name}.radius_m",
                ErrorCode.TASK_BOUNDARY_RADIUS_OUT_OF_RANGE
            )

    return {
        "task_name": task_name,
        "task": task
    }


def payload_clears_task(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("clear_task") is True


def get_cached_latest(session_id: str) -> Optional[dict]:
    latest_raw = redis_client.get(f"live:latest:{session_id}")
    if not latest_raw:
        return None

    latest = json.loads(latest_raw)
    if isinstance(latest, dict) and "agl_meters" not in latest:
        latest["agl_meters"] = None
    return latest


LIVE_RESPONSE_CACHE_VARIANT_V1_SESSION = "v1_session"
LIVE_RESPONSE_CACHE_VARIANT_V1_SHARE = "v1_share"
LIVE_RESPONSE_CACHE_VARIANT_V2_SESSION = "v2_session"
LIVE_RESPONSE_CACHE_VARIANT_V2_USER = "v2_user"
LIVE_RESPONSE_CACHE_VARIANTS = (
    LIVE_RESPONSE_CACHE_VARIANT_V1_SESSION,
    LIVE_RESPONSE_CACHE_VARIANT_V1_SHARE,
    LIVE_RESPONSE_CACHE_VARIANT_V2_SESSION,
    LIVE_RESPONSE_CACHE_VARIANT_V2_USER,
)
LIVE_RESPONSE_CACHE_SINGLE_FLIGHT_WAIT_SECONDS = 2.0
_live_response_cache_locks: dict[str, threading.Lock] = {}
_live_response_cache_locks_guard = threading.Lock()

SOCIAL_CACHE_SCOPE_COUNTS = "counts"
SOCIAL_CACHE_SCOPE_FOLLOWERS_PREVIEW = "followers_preview"
SOCIAL_CACHE_SCOPE_FOLLOWING_PREVIEW = "following_preview"
SOCIAL_CACHE_SCOPES = (
    SOCIAL_CACHE_SCOPE_COUNTS,
    SOCIAL_CACHE_SCOPE_FOLLOWERS_PREVIEW,
    SOCIAL_CACHE_SCOPE_FOLLOWING_PREVIEW,
)
SOCIAL_CACHE_TTL_SECONDS = 60


@dataclass
class LiveReadMetric:
    request_count: int = 0
    cache_hit_count: int = 0
    total_response_ms: float = 0.0


@dataclass
class SocialGraphMetrics:
    largest_followers_count: int = 0
    largest_following_count: int = 0
    relationship_list_request_count: int = 0
    slow_relationship_list_query_count: int = 0
    total_relationship_list_response_ms: float = 0.0
    max_offset_pagination_depth: int = 0
    active_list_refresh_request_count: int = 0
    bulk_relationship_status_request_count: int = 0
    bulk_relationship_status_user_id_count: int = 0
    notification_fanout_batch_count: int = 0
    max_notification_fanout_size: int = 0


_live_read_metrics: dict[str, LiveReadMetric] = {}
_live_read_metrics_lock = threading.Lock()
_live_read_rate_limited_count = 0
LIVE_READ_RATE_LIMIT_CONFIG = load_live_read_rate_limit_config_from_env()
LIVE_READ_RATE_LIMIT_WINDOW_SECONDS = LIVE_READ_RATE_LIMIT_CONFIG.window_seconds
LIVE_READ_RATE_LIMIT_GLOBAL = LIVE_READ_RATE_LIMIT_CONFIG.global_limit
LIVE_READ_RATE_LIMIT_PER_USER = LIVE_READ_RATE_LIMIT_CONFIG.per_user_limit
LIVE_READ_RATE_LIMIT_PER_IP = LIVE_READ_RATE_LIMIT_CONFIG.per_ip_limit
LIVE_READ_RATE_LIMIT_PER_SESSION = LIVE_READ_RATE_LIMIT_CONFIG.per_session_limit
_live_read_rate_limit_events: dict[str, list[float]] = {}
_live_read_rate_limit_lock = threading.Lock()
SOCIAL_GRAPH_SLOW_QUERY_MS = 500.0
_social_graph_metrics = SocialGraphMetrics()
_social_graph_metrics_lock = threading.Lock()


def live_response_cache_key(session_id: str, variant: str) -> str:
    return f"live:response:{variant}:{session_id}"


def live_response_cache_keys(session_id: str) -> list[str]:
    return [
        live_response_cache_key(session_id, variant)
        for variant in LIVE_RESPONSE_CACHE_VARIANTS
    ]


def live_response_cache_rebuild_lock(cache_key: str) -> threading.Lock:
    with _live_response_cache_locks_guard:
        lock = _live_response_cache_locks.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _live_response_cache_locks[cache_key] = lock
        return lock


def serialize_live_response_cache_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def deserialize_live_response_cache_payload(raw_value: Optional[str]) -> Optional[dict[str, Any]]:
    if not raw_value:
        return None
    parsed = json.loads(raw_value)
    return parsed if isinstance(parsed, dict) else None


def set_cached_live_response(cache_key: str, payload: dict[str, Any]) -> None:
    redis_client.set(cache_key, serialize_live_response_cache_payload(payload))


def get_cached_live_response(cache_key: str) -> Optional[dict[str, Any]]:
    return deserialize_live_response_cache_payload(redis_client.get(cache_key))


def social_cache_key(user_id: str, scope: str) -> str:
    if scope not in SOCIAL_CACHE_SCOPES:
        raise ValueError(f"unknown social cache scope: {scope}")
    return f"social:{scope}:{user_id}"


def social_cache_keys(user_id: str) -> list[str]:
    return [
        social_cache_key(user_id, scope)
        for scope in SOCIAL_CACHE_SCOPES
    ]


def serialize_social_cache_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def deserialize_social_cache_payload(raw_value: Optional[str]) -> Optional[dict[str, Any]]:
    if not raw_value:
        return None
    parsed = json.loads(raw_value)
    return parsed if isinstance(parsed, dict) else None


def set_cached_social_payload(
    cache_key: str,
    payload: dict[str, Any],
    ttl_seconds: int = SOCIAL_CACHE_TTL_SECONDS
) -> None:
    serialized_payload = serialize_social_cache_payload(payload)
    if ttl_seconds > 0:
        redis_client.set(cache_key, serialized_payload, ex=ttl_seconds)
    else:
        redis_client.set(cache_key, serialized_payload)


def get_cached_social_payload(cache_key: str) -> Optional[dict[str, Any]]:
    return deserialize_social_cache_payload(redis_client.get(cache_key))


def parse_cached_social_counts_payload(
    user_id: str,
    payload: Optional[dict[str, Any]]
) -> Optional[tuple[int, int]]:
    if payload is None or payload.get("user_id") != user_id:
        return None
    followers_count = payload.get("followers_count")
    following_count = payload.get("following_count")
    if (
        isinstance(followers_count, bool)
        or isinstance(following_count, bool)
        or not isinstance(followers_count, int)
        or not isinstance(following_count, int)
        or followers_count < 0
        or following_count < 0
    ):
        return None
    return followers_count, following_count


def get_cached_social_counts(user_id: str) -> Optional[tuple[int, int]]:
    return parse_cached_social_counts_payload(
        user_id,
        get_cached_social_payload(
            social_cache_key(user_id, SOCIAL_CACHE_SCOPE_COUNTS)
        )
    )


def set_cached_social_counts(
    user_id: str,
    followers_count: int,
    following_count: int
) -> None:
    set_cached_social_payload(
        social_cache_key(user_id, SOCIAL_CACHE_SCOPE_COUNTS),
        {
            "user_id": user_id,
            "followers_count": followers_count,
            "following_count": following_count,
        }
    )


def invalidate_social_cache_for_users(*user_ids: str) -> None:
    cache_keys: list[str] = []
    seen_user_ids: set[str] = set()
    for user_id in user_ids:
        normalized_user_id = (user_id or "").strip()
        if not normalized_user_id or normalized_user_id in seen_user_ids:
            continue
        seen_user_ids.add(normalized_user_id)
        cache_keys.extend(social_cache_keys(normalized_user_id))
    if cache_keys:
        redis_client.delete(*cache_keys)


def live_response_with_dynamic_status(
    session: LiveSession,
    response: dict[str, Any]
) -> dict[str, Any]:
    adjusted_response = dict(response)
    adjusted_response["status"] = compute_effective_status(session)
    return adjusted_response


def build_cached_live_response_with_metadata(
    db,
    session: LiveSession,
    cache_key: str,
    owner_profile: Optional[PilotProfile] = None
) -> tuple[dict[str, Any], bool]:
    cached_response = get_cached_live_response(cache_key)
    if cached_response is not None:
        return live_response_with_dynamic_status(session, cached_response), True

    rebuild_lock = live_response_cache_rebuild_lock(cache_key)
    lock_acquired = rebuild_lock.acquire(
        timeout=LIVE_RESPONSE_CACHE_SINGLE_FLIGHT_WAIT_SECONDS
    )
    if not lock_acquired:
        cached_response = get_cached_live_response(cache_key)
        if cached_response is not None:
            return live_response_with_dynamic_status(session, cached_response), True
        response = build_live_response(db, session, owner_profile)
        set_cached_live_response(cache_key, response)
        return response, False
    try:
        cached_response = get_cached_live_response(cache_key)
        if cached_response is not None:
            return live_response_with_dynamic_status(session, cached_response), True
        response = build_live_response(db, session, owner_profile)
        set_cached_live_response(cache_key, response)
        return response, False
    finally:
        rebuild_lock.release()


def build_cached_live_response(
    db,
    session: LiveSession,
    cache_key: str,
    owner_profile: Optional[PilotProfile] = None
) -> dict[str, Any]:
    response, _cache_hit = build_cached_live_response_with_metadata(
        db,
        session,
        cache_key,
        owner_profile
    )
    return response


def build_measured_live_response(
    db,
    session: LiveSession,
    cache_key: str,
    owner_profile: Optional[PilotProfile] = None
) -> dict[str, Any]:
    started_at = time.perf_counter()
    response, cache_hit = build_cached_live_response_with_metadata(
        db,
        session,
        cache_key,
        owner_profile
    )
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    record_live_read_metric(
        session_id=session.id,
        cache_hit=cache_hit,
        response_ms=elapsed_ms
    )
    return response


def record_live_read_metric(
    session_id: str,
    cache_hit: bool,
    response_ms: float
) -> None:
    with _live_read_metrics_lock:
        metric = _live_read_metrics.get(session_id)
        if metric is None:
            metric = LiveReadMetric()
            _live_read_metrics[session_id] = metric
        metric.request_count += 1
        if cache_hit:
            metric.cache_hit_count += 1
        metric.total_response_ms += max(response_ms, 0.0)


def record_live_read_rate_limited() -> None:
    global _live_read_rate_limited_count
    with _live_read_metrics_lock:
        _live_read_rate_limited_count += 1


def reset_live_read_metrics() -> None:
    global _live_read_rate_limited_count
    with _live_read_metrics_lock:
        _live_read_metrics.clear()
        _live_read_rate_limited_count = 0


def live_read_metrics_snapshot(top_limit: int = 5) -> dict[str, Any]:
    with _live_read_metrics_lock:
        metrics = [
            {
                "session_id": session_id,
                "request_count": metric.request_count,
                "cache_hit_count": metric.cache_hit_count,
                "cache_hit_rate": (
                    metric.cache_hit_count / metric.request_count
                    if metric.request_count
                    else 0.0
                ),
                "average_response_ms": (
                    metric.total_response_ms / metric.request_count
                    if metric.request_count
                    else 0.0
                ),
            }
            for session_id, metric in _live_read_metrics.items()
        ]
        total_requests = sum(metric["request_count"] for metric in metrics)
        total_cache_hits = sum(metric["cache_hit_count"] for metric in metrics)
        return {
            "request_count": total_requests,
            "cache_hit_count": total_cache_hits,
            "cache_hit_rate": (
                total_cache_hits / total_requests
                if total_requests
                else 0.0
            ),
            "rate_limited_429_count": _live_read_rate_limited_count,
            "top_sessions_by_read_volume": sorted(
                metrics,
                key=lambda metric: (
                    -metric["request_count"],
                    metric["session_id"]
                )
            )[:top_limit],
        }


def reset_social_graph_metrics() -> None:
    global _social_graph_metrics
    with _social_graph_metrics_lock:
        _social_graph_metrics = SocialGraphMetrics()


def record_social_counter_counts(followers_count: int, following_count: int) -> None:
    with _social_graph_metrics_lock:
        _social_graph_metrics.largest_followers_count = max(
            _social_graph_metrics.largest_followers_count,
            max(followers_count, 0)
        )
        _social_graph_metrics.largest_following_count = max(
            _social_graph_metrics.largest_following_count,
            max(following_count, 0)
        )


def record_relationship_list_query_metric(offset: int, response_ms: float) -> None:
    with _social_graph_metrics_lock:
        _social_graph_metrics.relationship_list_request_count += 1
        _social_graph_metrics.total_relationship_list_response_ms += max(response_ms, 0.0)
        _social_graph_metrics.max_offset_pagination_depth = max(
            _social_graph_metrics.max_offset_pagination_depth,
            max(offset, 0)
        )
        if response_ms >= SOCIAL_GRAPH_SLOW_QUERY_MS:
            _social_graph_metrics.slow_relationship_list_query_count += 1


def record_active_list_refresh_metric(offset: int) -> None:
    with _social_graph_metrics_lock:
        _social_graph_metrics.active_list_refresh_request_count += 1
        _social_graph_metrics.max_offset_pagination_depth = max(
            _social_graph_metrics.max_offset_pagination_depth,
            max(offset, 0)
        )


def record_bulk_relationship_status_metric(requested_user_count: int) -> None:
    with _social_graph_metrics_lock:
        _social_graph_metrics.bulk_relationship_status_request_count += 1
        _social_graph_metrics.bulk_relationship_status_user_id_count += max(
            requested_user_count,
            0
        )


def record_notification_fanout_metric(fanout_size: int) -> None:
    with _social_graph_metrics_lock:
        _social_graph_metrics.notification_fanout_batch_count += 1
        _social_graph_metrics.max_notification_fanout_size = max(
            _social_graph_metrics.max_notification_fanout_size,
            max(fanout_size, 0)
        )


def social_graph_metrics_snapshot() -> dict[str, Any]:
    with _social_graph_metrics_lock:
        relationship_request_count = (
            _social_graph_metrics.relationship_list_request_count
        )
        return {
            "largest_followers_count": _social_graph_metrics.largest_followers_count,
            "largest_following_count": _social_graph_metrics.largest_following_count,
            "relationship_list_request_count": relationship_request_count,
            "slow_relationship_list_query_count": (
                _social_graph_metrics.slow_relationship_list_query_count
            ),
            "average_relationship_list_response_ms": (
                _social_graph_metrics.total_relationship_list_response_ms
                / relationship_request_count
                if relationship_request_count
                else 0.0
            ),
            "max_offset_pagination_depth": (
                _social_graph_metrics.max_offset_pagination_depth
            ),
            "active_list_refresh_request_count": (
                _social_graph_metrics.active_list_refresh_request_count
            ),
            "bulk_relationship_status_request_count": (
                _social_graph_metrics.bulk_relationship_status_request_count
            ),
            "bulk_relationship_status_user_id_count": (
                _social_graph_metrics.bulk_relationship_status_user_id_count
            ),
            "notification_fanout_batch_count": (
                _social_graph_metrics.notification_fanout_batch_count
            ),
            "max_notification_fanout_size": (
                _social_graph_metrics.max_notification_fanout_size
            ),
        }


def reset_live_read_rate_limits() -> None:
    with _live_read_rate_limit_lock:
        _live_read_rate_limit_events.clear()


def live_read_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first_forwarded = forwarded_for.split(",", 1)[0].strip()
        if first_forwarded:
            return first_forwarded
    if request.client is not None and request.client.host:
        return request.client.host
    return "unknown"


def live_read_rate_limit_scopes(
    session_id: str,
    user_id: Optional[str],
    client_ip: str
) -> list[tuple[str, int]]:
    scopes = []
    if LIVE_READ_RATE_LIMIT_GLOBAL > 0:
        scopes.append(("global", LIVE_READ_RATE_LIMIT_GLOBAL))
    if LIVE_READ_RATE_LIMIT_PER_SESSION > 0:
        scopes.append((f"session:{session_id}", LIVE_READ_RATE_LIMIT_PER_SESSION))
    if user_id and LIVE_READ_RATE_LIMIT_PER_USER > 0:
        scopes.append((f"user:{user_id}", LIVE_READ_RATE_LIMIT_PER_USER))
    if client_ip and LIVE_READ_RATE_LIMIT_PER_IP > 0:
        scopes.append((f"ip:{client_ip}", LIVE_READ_RATE_LIMIT_PER_IP))
    return scopes


def live_read_retry_after_seconds(events: list[float], now: float) -> int:
    if not events:
        return 1
    retry_after = (min(events) + LIVE_READ_RATE_LIMIT_WINDOW_SECONDS) - now
    return max(1, int(math.ceil(retry_after)))


def enforce_live_read_rate_limit(
    session_id: str,
    user_id: Optional[str],
    client_ip: str
) -> None:
    scopes = live_read_rate_limit_scopes(session_id, user_id, client_ip)
    if not scopes:
        return

    now = time.monotonic()
    window_start = now - LIVE_READ_RATE_LIMIT_WINDOW_SECONDS
    with _live_read_rate_limit_lock:
        for scope_key, limit in scopes:
            events = _live_read_rate_limit_events.setdefault(scope_key, [])
            events[:] = [event_time for event_time in events if event_time > window_start]
            if len(events) >= limit:
                retry_after_seconds = live_read_retry_after_seconds(events, now)
                record_live_read_rate_limited()
                raise ApiHTTPException(
                    status_code=429,
                    code=ErrorCode.LIVEFOLLOW_RATE_LIMITED,
                    detail="LiveFollow live-read rate limit exceeded.",
                    headers={"Retry-After": str(retry_after_seconds)}
                )
        for scope_key, _limit in scopes:
            _live_read_rate_limit_events[scope_key].append(now)


def invalidate_cached_live_responses(session_id: str) -> None:
    for cache_key in live_response_cache_keys(session_id):
        redis_client.delete(cache_key)


def build_live_list_display_label(session: LiveSession) -> str:
    # Public UI label only. Share code is server-owned; no stronger identity is implied.
    return f"Live {session.share_code}"


def build_live_active_item(session: LiveSession) -> dict:
    return {
        "session_id": session.id,
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "latest": get_cached_latest(session.id),
        "display_label": build_live_list_display_label(session)
    }


def build_live_owner_display_label(
    profile: Optional[PilotProfile],
    session: LiveSession
) -> str:
    if profile is not None:
        if profile.display_name:
            return profile.display_name
        if profile.handle:
            return profile.handle
    public_share_code = public_share_code_for(session)
    if public_share_code:
        return f"Live {public_share_code}"
    if session.owner_user_id:
        return session.owner_user_id
    return session.id


def build_live_spectator_stats_response(
    stats: Optional[LiveSessionSpectatorStats]
) -> Optional[dict[str, Any]]:
    if stats is None:
        return None
    duration_seconds = int(
        max(
            0.0,
            spectator_stats_delta_seconds(
                stats.first_position_at,
                stats.last_position_at
            )
        )
    )
    return {
        "current_climb_sink_ms": stats.current_climb_sink_ms,
        "highest_altitude_msl_meters": stats.highest_altitude_msl_meters,
        "best_short_window_climb_ms": stats.best_short_window_climb_ms,
        "distance_flown_meters": stats.distance_flown_meters,
        "flight_duration_seconds": duration_seconds
    }


def build_authorized_live_active_item(
    session: LiveSession,
    owner_profile: Optional[PilotProfile]
) -> dict[str, Any]:
    return {
        "session_id": session.id,
        "user_id": session.owner_user_id,
        "visibility": effective_live_visibility(session),
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "latest": get_cached_latest(session.id),
        "display_label": build_live_owner_display_label(owner_profile, session),
        "profile": build_profile_response(owner_profile) if owner_profile is not None else None
    }


def build_live_response(
    db,
    session: LiveSession,
    owner_profile: Optional[PilotProfile] = None
):
    latest = get_cached_latest(session.id)

    positions = (
        db.query(LivePosition)
        .filter(LivePosition.session_id == session.id)
        .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
        .limit(10)
        .all()
    )

    task = db.query(LiveTask).filter(LiveTask.session_id == session.id).first()
    task_revision_data = None

    if task:
        revision = (
            db.query(LiveTaskRevision)
            .filter(
                LiveTaskRevision.task_id == task.id,
                LiveTaskRevision.revision == task.current_revision
            )
            .first()
        )
        if revision:
            payload = json.loads(revision.payload_json)
            if not payload_clears_task(payload):
                task_revision_data = {
                    "task_id": task.id,
                    "current_revision": task.current_revision,
                    "updated_at": to_iso_utc(task.updated_at),
                    "payload": payload
                }

    return {
        "session": session.id,
        "share_code": public_share_code_for(session),
        "status": compute_effective_status(session),
        "visibility": effective_live_visibility(session),
        "owner_user_id": session.owner_user_id,
        "display_label": build_live_owner_display_label(owner_profile, session),
        "profile": build_profile_response(owner_profile) if owner_profile is not None else None,
        "created_at": to_iso_utc(session.created_at),
        "last_position_at": to_iso_utc(session.last_position_at),
        "ended_at": to_iso_utc(session.ended_at),
        "latest": latest,
        "spectator_stats": build_live_spectator_stats_response(
            get_live_session_spectator_stats(db, session.id)
        ),
        "positions": [
            {
                "lat": p.lat,
                "lon": p.lon,
                "alt": p.alt,
                "agl_meters": p.agl_meters,
                "speed": p.speed,
                "heading": p.heading,
                "timestamp": to_iso_utc(p.timestamp)
            }
            for p in reversed(positions)
        ],
        "task": task_revision_data
    }


@app.get("/")
def root():
    return {"status": "XCPro backend running"}


@app.post("/api/v1/session/start")
def start_session():
    db = SessionLocal()
    try:
        session_id = str(uuid.uuid4())
        write_token = generate_write_token()

        share_code = generate_share_code()
        while db.query(LiveSession).filter(LiveSession.share_code == share_code).first():
            share_code = generate_share_code()

        row = LiveSession(
            id=session_id,
            share_code=share_code,
            owner_user_id=None,
            visibility=LIVE_VISIBILITY_PUBLIC,
            created_at=utcnow(),
            status="active",
            last_position_at=None,
            ended_at=None,
            write_token_hash=hash_token(write_token)
        )
        db.add(row)
        db.commit()

        return {
            "session_id": session_id,
            "share_code": share_code,
            "status": "active",
            "write_token": write_token
        }
    finally:
        db.close()


@app.post("/api/v1/position")
def post_position(
    p: Position,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, p.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
            )

        position_ts = to_utc_naive(p.timestamp)
        validate_position_payload(p, position_ts)

        last_position = (
            db.query(LivePosition)
            .filter(LivePosition.session_id == p.session_id)
            .order_by(LivePosition.timestamp.desc(), LivePosition.id.desc())
            .first()
        )

        if last_position:
            if position_ts < last_position.timestamp:
                raise ApiHTTPException(
                    status_code=409,
                    code=ErrorCode.POSITION_OUT_OF_ORDER,
                    detail="out-of-order position timestamp"
                )

            exact_duplicate = (
                position_ts == last_position.timestamp and
                p.lat == last_position.lat and
                p.lon == last_position.lon and
                p.alt == last_position.alt and
                p.agl_meters == last_position.agl_meters and
                p.speed == last_position.speed and
                p.heading == last_position.heading
            )

            if exact_duplicate:
                return {"ok": True, "deduped": True}

            if position_ts == last_position.timestamp:
                raise ApiHTTPException(
                    status_code=409,
                    code=ErrorCode.POSITION_CONFLICTING_DUPLICATE_TIMESTAMP,
                    detail="conflicting duplicate timestamp"
                )

            delta_seconds = (position_ts - last_position.timestamp).total_seconds()
            if delta_seconds > 0:
                jump_m = haversine_m(last_position.lat, last_position.lon, p.lat, p.lon)
                implied_kmh = (jump_m / delta_seconds) * 3.6
                if implied_kmh > MAX_IMPOSSIBLE_GROUND_SPEED_KMH:
                    raise ApiHTTPException(
                        status_code=400,
                        code=ErrorCode.POSITION_IMPOSSIBLE_JUMP,
                        detail=f"impossible jump detected ({implied_kmh:.1f} km/h)"
                    )

        row = LivePosition(
            session_id=p.session_id,
            lat=p.lat,
            lon=p.lon,
            alt=p.alt,
            agl_meters=p.agl_meters,
            speed=p.speed,
            heading=p.heading,
            timestamp=position_ts
        )
        db.add(row)

        accepted_at = utcnow()
        session.last_position_at = accepted_at
        db.flush()
        update_live_session_spectator_stats_for_accepted_position(
            db,
            p.session_id,
            accepted_at
        )
        db.commit()

        latest = {
            "lat": p.lat,
            "lon": p.lon,
            "alt": p.alt,
            "agl_meters": p.agl_meters,
            "speed": p.speed,
            "heading": p.heading,
            "timestamp": to_iso_utc(position_ts)
        }
        redis_client.set(f"live:latest:{p.session_id}", json.dumps(latest))
        invalidate_cached_live_responses(p.session_id)

        return {"ok": True}
    finally:
        db.close()


@app.post("/api/v1/task/upsert")
def task_upsert(
    req: TaskUpsertRequest,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, req.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
            )

        payload = validate_task_payload(req)
        now = utcnow()
        cleared = payload_clears_task(payload)

        task = db.query(LiveTask).filter(LiveTask.session_id == req.session_id).first()

        if not task:
            task = LiveTask(
                id=str(uuid.uuid4()),
                session_id=req.session_id,
                created_at=now,
                updated_at=now,
                current_revision=1
            )
            db.add(task)
            db.flush()

            revision = LiveTaskRevision(
                task_id=task.id,
                revision=1,
                created_at=now,
                payload_json=json.dumps(payload, sort_keys=True)
            )
            db.add(revision)
            db.commit()
            invalidate_cached_live_responses(req.session_id)

            return {
                "ok": True,
                "task_id": task.id,
                "revision": 1,
                "cleared": cleared
            }

        current_revision = (
            db.query(LiveTaskRevision)
            .filter(
                LiveTaskRevision.task_id == task.id,
                LiveTaskRevision.revision == task.current_revision
            )
            .first()
        )

        if current_revision:
            current_payload = json.loads(current_revision.payload_json)
            if current_payload == payload:
                return {
                    "ok": True,
                    "task_id": task.id,
                    "revision": task.current_revision,
                    "deduped": True,
                    "cleared": payload_clears_task(current_payload)
                }

        revision_number = task.current_revision + 1
        task.current_revision = revision_number
        task.updated_at = now

        revision = LiveTaskRevision(
            task_id=task.id,
            revision=revision_number,
            created_at=now,
            payload_json=json.dumps(payload, sort_keys=True)
        )
        db.add(revision)
        db.commit()
        invalidate_cached_live_responses(req.session_id)

        return {
            "ok": True,
            "task_id": task.id,
            "revision": revision_number,
            "cleared": cleared
        }
    finally:
        db.close()


@app.post("/api/v1/session/end")
def end_session(
    req: SessionEndRequest,
    x_session_token: Optional[str] = Header(default=None, alias="X-Session-Token")
):
    db = SessionLocal()
    try:
        session = get_session_or_404(db, req.session_id)
        require_write_access(session, x_session_token)

        if session.status == "ended":
            return {
                "ok": True,
                "session_id": session.id,
                "status": "ended",
                "ended_at": to_iso_utc(session.ended_at),
                "unique_watchers_count": count_live_session_unique_viewers(db, session.id)
            }

        session.status = "ended"
        session.ended_at = utcnow()
        db.commit()
        invalidate_cached_live_responses(session.id)

        return {
            "ok": True,
            "session_id": session.id,
            "status": "ended",
            "ended_at": to_iso_utc(session.ended_at),
            "unique_watchers_count": count_live_session_unique_viewers(db, session.id)
        }
    finally:
        db.close()


@app.post("/api/v2/live/session/start")
def start_authenticated_live_session(
    request: Optional[LiveSessionStartRequest] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        visibility = validate_live_visibility(
            request.visibility
            if request is not None and request.visibility is not None
            else current_user.privacy.default_live_visibility
        )
        now = utcnow()
        ended_session_ids = end_active_owned_live_sessions(db, current_user.user.id, now)

        session_id = str(uuid.uuid4())
        write_token = generate_write_token()
        share_code = generate_share_code()
        while db.query(LiveSession).filter(LiveSession.share_code == share_code).first():
            share_code = generate_share_code()

        row = LiveSession(
            id=session_id,
            share_code=share_code,
            owner_user_id=current_user.user.id,
            visibility=visibility,
            created_at=now,
            status="active",
            last_position_at=None,
            ended_at=None,
            write_token_hash=hash_token(write_token)
        )
        db.add(row)
        db.commit()
        for ended_session_id in ended_session_ids:
            invalidate_cached_live_responses(ended_session_id)

        return build_live_session_command_response(row, write_token=write_token)
    finally:
        db.close()


@app.patch("/api/v2/live/session/{session_id}/visibility")
def patch_authenticated_live_session_visibility(
    session_id: str,
    request: LiveSessionVisibilityPatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = get_owned_live_session_or_404(db, session_id, current_user.user.id)
        if session.status == "ended":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.SESSION_ALREADY_ENDED,
                detail="session already ended"
        )
        session.visibility = validate_live_visibility(request.visibility)
        db.commit()
        invalidate_cached_live_responses(session.id)
        return build_live_session_command_response(session)
    finally:
        db.close()


@app.post("/api/v2/auth/google/exchange")
def exchange_google_auth_token(
    request: GoogleAuthExchangeRequest
):
    trimmed_token = request.google_id_token.strip()
    if not trimmed_token:
        raise ApiHTTPException(
            status_code=422,
            code=ErrorCode.INVALID_GOOGLE_ID_TOKEN,
            detail="google_id_token is required"
        )

    if not GOOGLE_SERVER_CLIENT_IDS:
        raise ApiHTTPException(
            status_code=503,
            code=ErrorCode.AUTH_UNAVAILABLE,
            detail="Google server client ID is not configured"
        )

    identity = GOOGLE_ID_TOKEN_VERIFIER(trimmed_token)
    if identity is None:
        raise ApiHTTPException(
            status_code=401,
            code=ErrorCode.INVALID_GOOGLE_ID_TOKEN,
            detail="invalid Google ID token"
        )

    db = SessionLocal()
    try:
        current_user = ensure_current_user_record_for_identity(db, identity)
        access_token = issue_private_follow_bearer(identity)
        return build_google_auth_exchange_response(current_user, access_token)
    finally:
        db.close()


@app.post("/api/v2/auth/firebase/exchange")
def exchange_firebase_auth_token(
    request: FirebaseAuthExchangeRequest,
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    trimmed_token = request.firebase_id_token.strip()
    if not trimmed_token:
        raise invalid_firebase_id_token_exception(
            status_code=422,
            detail="firebase_id_token is required"
        )

    if not callable(FIREBASE_ID_TOKEN_VERIFIER):
        raise firebase_auth_unavailable_exception(
            "Firebase Auth verifier is unavailable"
        )

    firebase_identity = FIREBASE_ID_TOKEN_VERIFIER(trimmed_token)
    if firebase_identity is None:
        raise invalid_firebase_id_token_exception()

    if (
        firebase_identity.sign_in_provider in FIREBASE_EMAIL_PASSWORD_SIGN_IN_PROVIDERS and
        not firebase_identity.email_verified
    ):
        raise ApiHTTPException(
            status_code=403,
            code=ErrorCode.EMAIL_VERIFICATION_REQUIRED,
            detail="email verification is required"
        )

    firebase_exchange_auth_method(firebase_identity)
    validate_entitlement_package_name(package_name)
    bearer_identity = build_firebase_bearer_identity(firebase_identity)
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record_for_firebase_identity(
            db,
            firebase_identity
        )
        access_token = issue_private_follow_bearer(bearer_identity)
        return build_firebase_auth_exchange_response(
            db,
            current_user,
            access_token,
            firebase_identity
        )
    finally:
        db.close()


@app.get("/api/v2/me")
def get_current_user_me(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        return build_me_response(db, current_user)
    finally:
        db.close()


@app.post("/api/v2/me/push-tokens")
def register_current_user_push_token(
    request: PushTokenRegistrationRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        push_token = upsert_device_push_token(db, current_user.user.id, request)
        db.commit()
        db.refresh(push_token)
        return build_push_token_registration_response(push_token)
    finally:
        db.close()


@app.delete("/api/v2/me/push-tokens/{device_id}")
def revoke_current_user_push_token(
    device_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        revoked = revoke_device_push_token(db, current_user.user.id, device_id)
        db.commit()
        return {
            "ok": True,
            "device_id": normalize_push_device_id(device_id),
            "platform": PUSH_PLATFORM_ANDROID,
            "provider": PUSH_PROVIDER_FCM,
            "revoked": revoked,
        }
    finally:
        db.close()


@app.get("/api/v1/subscriptions/entitlements")
def get_subscription_entitlements(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validate_entitlement_package_name(package_name)
        return build_entitlement_response(db, current_user)
    finally:
        db.close()


@app.get("/api/v1/puretrack/status", response_model=PureTrackStatusResponse)
def get_puretrack_status(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validate_entitlement_package_name(package_name)
        return build_puretrack_status_payload(db, current_user)
    finally:
        db.close()


@app.post("/api/v1/puretrack/connect", response_model=PureTrackConnectResponse)
def connect_puretrack(
    request: PureTrackConnectRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validate_entitlement_package_name(package_name)
        response = connect_puretrack_account(db, current_user, request)
        db.commit()
        return response
    finally:
        db.close()


@app.post("/api/v1/puretrack/disconnect", response_model=PureTrackDisconnectResponse)
def disconnect_puretrack(
    request: PureTrackDisconnectRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validate_entitlement_package_name(package_name)
        response = disconnect_puretrack_account(db, current_user)
        db.commit()
        return response
    finally:
        db.close()


@app.post("/api/v1/puretrack/insert", response_model=PureTrackInsertPublishResponse)
def insert_puretrack_points(
    request: PureTrackInsertPublishRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validate_entitlement_package_name(package_name)
        return publish_puretrack_insert_batch(db, current_user, request)
    finally:
        db.close()


@app.post("/api/v1/puretrack/traffic", response_model=PureTrackTrafficResponse)
def get_puretrack_traffic(
    request: PureTrackTrafficRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    started_at_ms = to_epoch_ms(utcnow())
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        validated_package_name = validate_entitlement_package_name(package_name)
        try:
            response = fetch_puretrack_traffic(
                db,
                current_user,
                validated_package_name,
                request,
            )
        except ApiHTTPException as exc:
            emit_puretrack_traffic_cadence_evidence(
                build_puretrack_traffic_cadence_evidence_event(
                    started_at_ms=started_at_ms,
                    completed_at_ms=to_epoch_ms(utcnow()),
                    current_user=current_user,
                    package_name=validated_package_name,
                    status_code=exc.status_code,
                    outcome=exc.code,
                    retry_after_ms=puretrack_traffic_retry_after_header_ms(exc.headers),
                )
            )
            raise
        except Exception:
            emit_puretrack_traffic_cadence_evidence(
                build_puretrack_traffic_cadence_evidence_event(
                    started_at_ms=started_at_ms,
                    completed_at_ms=to_epoch_ms(utcnow()),
                    current_user=current_user,
                    package_name=validated_package_name,
                    status_code=500,
                    outcome="unexpected_error",
                )
            )
            raise

        emit_puretrack_traffic_cadence_evidence(
            build_puretrack_traffic_cadence_evidence_event(
                started_at_ms=started_at_ms,
                completed_at_ms=to_epoch_ms(utcnow()),
                current_user=current_user,
                package_name=validated_package_name,
                status_code=200,
                outcome=response.result,
                cache_status=response.cacheStatus,
                retry_after_ms=response.retryAfterMs,
            )
        )
        return response
    finally:
        db.close()


@app.post(
    "/api/v1/subscriptions/googleplay/sync",
    response_model=GooglePlaySyncResponse
)
def sync_google_play_subscription(
    request: GooglePlaySyncRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        purchase_token_hash = hash_purchase_token(request.purchaseToken)
        resolved_package_name = validate_google_play_package_context(
            header_package_name=package_name,
            request_package_name=request.packageName,
        )
        validate_google_play_client_purchase_state(request.clientPurchaseState)

        product_id = validate_google_play_product_id(request.productId)
        if product_id is None:
            return build_invalid_google_play_sync_response(
                db=db,
                current_user=current_user,
                purchase_token_hash=purchase_token_hash,
                result="INVALID_PRODUCT",
                event_type="GOOGLE_PLAY_SYNC",
                detail={
                    "packageName": resolved_package_name,
                    "productId": request.productId,
                    "basePlanId": request.basePlanId,
                },
            )
        base_plan_id = validate_google_play_base_plan_id(request.basePlanId)
        if base_plan_id is None:
            return build_invalid_google_play_sync_response(
                db=db,
                current_user=current_user,
                purchase_token_hash=purchase_token_hash,
                result="INVALID_BASE_PLAN",
                event_type="GOOGLE_PLAY_SYNC",
                detail={
                    "packageName": resolved_package_name,
                    "productId": product_id,
                    "basePlanId": request.basePlanId,
                },
            )

        outcome = process_google_play_purchase_for_user(
            db=db,
            user_id=current_user.user.id,
            package_name=resolved_package_name,
            product_id=product_id,
            base_plan_id=base_plan_id,
            purchase_token_hash=purchase_token_hash,
            purchase_token=request.purchaseToken,
            event_type="GOOGLE_PLAY_SYNC",
        )
        return build_google_play_sync_response(
            result=outcome.result,
            entitlement_response=build_entitlement_response(db, current_user),
            acknowledgement_required=outcome.acknowledgement_required,
            acknowledgement_completed=outcome.acknowledgement_completed,
            acknowledgement_retry_after_ms=outcome.acknowledgement_retry_after_ms,
            recovery_action=outcome.recovery_action,
            audit_id=outcome.audit_id,
        )
    finally:
        db.close()


@app.post(
    "/api/v1/subscriptions/googleplay/restore",
    response_model=GooglePlaySyncResponse
)
def restore_google_play_subscription(
    request: GooglePlayRestoreRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    package_name: Optional[str] = Header(default=None, alias="X-XCPro-Package-Name")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        purchase_token_hash = hash_purchase_token(request.purchaseToken)
        resolved_package_name = validate_google_play_package_context(
            header_package_name=package_name,
            request_package_name=request.packageName,
        )
        validate_google_play_client_purchase_state(request.clientPurchaseState)

        outcome = process_google_play_restore_for_user(
            db=db,
            user_id=current_user.user.id,
            package_name=resolved_package_name,
            purchase_token_hash=purchase_token_hash,
            purchase_token=request.purchaseToken,
            event_type="GOOGLE_PLAY_RESTORE",
        )
        return build_google_play_sync_response(
            result=outcome.result,
            entitlement_response=build_entitlement_response(db, current_user),
            acknowledgement_required=outcome.acknowledgement_required,
            acknowledgement_completed=outcome.acknowledgement_completed,
            acknowledgement_retry_after_ms=outcome.acknowledgement_retry_after_ms,
            recovery_action=outcome.recovery_action,
            audit_id=outcome.audit_id,
        )
    finally:
        db.close()


@app.post(
    "/api/v1/subscriptions/googleplay/rtdn",
    response_model=GooglePlayRtdnIngestionResponse
)
def ingest_google_play_rtdn(
    envelope: PubSubPushEnvelope,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    rtdn_token: Optional[str] = Header(default=None, alias="X-XCPro-RTDN-Token")
):
    require_rtdn_ingest_auth(authorization, rtdn_token)
    decoded = decode_pubsub_rtdn_payload(envelope)
    db = SessionLocal()
    try:
        event, deduped = record_google_play_rtdn_event(db, decoded)
        if deduped and not should_reprocess_google_play_rtdn_event(event):
            return GooglePlayRtdnIngestionResponse(
                result=event.processing_result if event is not None else "DUPLICATE",
                deduped=True,
                auditId=event.audit_id if event is not None else None,
            )
        return process_google_play_rtdn_event(db, event, decoded)
    finally:
        db.close()


@app.patch("/api/v2/me/profile")
def patch_current_user_profile(
    request: MeProfilePatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        apply_profile_patch(current_user.profile, request)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.HANDLE_ALREADY_TAKEN,
                detail="handle already taken"
            )
        return build_profile_response(current_user.profile)
    finally:
        db.close()


@app.patch("/api/v2/me/privacy")
def patch_current_user_privacy(
    request: MePrivacyPatchRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        apply_privacy_patch(current_user.privacy, request)
        db.commit()
        return build_privacy_response(current_user.privacy)
    finally:
        db.close()


@app.get("/api/v2/users/search")
def search_private_follow_users(
    q: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        normalized_query = normalize_search_query(q)
        search_query = (
            db.query(PilotProfile, PrivacySetting)
            .join(PrivacySetting, PrivacySetting.user_id == PilotProfile.user_id)
            .filter(
                PilotProfile.user_id != current_user.user.id,
                PilotProfile.handle_normalized.isnot(None),
                PilotProfile.display_name.isnot(None),
                PilotProfile.handle_normalized.like(f"%{normalized_query}%")
            )
        )
        exact_match_rank = case(
            (PilotProfile.handle_normalized == normalized_query, 0),
            else_=1
        )
        prefix_match_rank = case(
            (PilotProfile.handle_normalized.like(f"{normalized_query}%"), 0),
            else_=1
        )
        ordered_profile_rows = (
            search_query
            .order_by(
                exact_match_rank,
                prefix_match_rank,
                PilotProfile.handle_normalized.asc(),
                PilotProfile.user_id.asc()
            )
            .limit(SEARCH_RESULT_LIMIT)
            .all()
        )
        ordered_profiles = [
            profile
            for profile, privacy in ordered_profile_rows
            if relationship_policy_allows_profile_discovery(
                build_relationship_policy_context(
                    db,
                    current_user.user.id,
                    profile.user_id
                ),
                privacy
            )
        ]
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [profile.user_id for profile in ordered_profiles]
        )
        return {
            "users": [
                build_search_result_response(profile, lookup)
                for profile in ordered_profiles
            ]
        }
    finally:
        db.close()


@app.post("/api/v2/users/relationship-status/bulk")
def get_bulk_relationship_status(
    request: BulkRelationshipStatusRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        requested_user_ids = normalize_bulk_relationship_status_user_ids(request.user_ids)
        record_bulk_relationship_status_metric(len(requested_user_ids))
        if not requested_user_ids:
            return {"items": []}

        profile_rows = (
            db.query(PilotProfile, PrivacySetting)
            .join(PrivacySetting, PrivacySetting.user_id == PilotProfile.user_id)
            .filter(
                PilotProfile.user_id.in_(requested_user_ids),
                PilotProfile.user_id != current_user.user.id,
                PilotProfile.handle_normalized.isnot(None),
                PilotProfile.display_name.isnot(None)
            )
            .all()
        )
        visible_user_ids = {
            profile.user_id
            for profile, privacy in profile_rows
            if relationship_policy_allows_profile_discovery(
                build_relationship_policy_context(
                    db,
                    current_user.user.id,
                    profile.user_id
                ),
                privacy
            )
        }
        ordered_visible_user_ids = [
            user_id
            for user_id in requested_user_ids
            if user_id in visible_user_ids
        ]
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            ordered_visible_user_ids
        )
        return {
            "items": [
                {
                    "user_id": user_id,
                    "relationship_state": build_relationship_state(lookup, user_id)
                }
                for user_id in ordered_visible_user_ids
            ]
        }
    finally:
        db.close()


@app.get("/api/v2/users/{user_id}/followers")
def list_user_followers(
    user_id: str,
    limit: int = RELATIONSHIP_LIST_DEFAULT_LIMIT,
    cursor: Optional[str] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        started_at = time.perf_counter()
        current_user = ensure_current_user_record(db, authorization)
        _target_profile, target_privacy = get_searchable_target_or_404(db, user_id)
        ensure_can_read_relationship_list(
            db,
            current_user.user.id,
            user_id,
            target_privacy,
            ErrorCode.NOT_AUTHORIZED_TO_VIEW_FOLLOWERS
        )
        page_limit, offset = parse_relationship_list_page_params(limit, cursor)
        blocked_user_ids = load_blocked_counterpart_user_ids(db, current_user.user.id)
        followers_query = (
            db.query(PilotProfile)
            .join(FollowEdge, FollowEdge.follower_user_id == PilotProfile.user_id)
            .filter(FollowEdge.followed_user_id == user_id)
        )
        followers_query = filter_blocked_counterparts(
            followers_query,
            FollowEdge.follower_user_id,
            blocked_user_ids
        )
        total = followers_query.count()
        profiles = (
            followers_query
            .order_by(FollowEdge.created_at.desc(), FollowEdge.follower_user_id.asc())
            .offset(offset)
            .limit(page_limit)
            .all()
        )
        response = build_relationship_list_page_response(
            db,
            current_user.user.id,
            profiles,
            total,
            page_limit,
            offset
        )
        record_relationship_list_query_metric(
            offset=offset,
            response_ms=(time.perf_counter() - started_at) * 1000.0
        )
        return response
    finally:
        db.close()


@app.get("/api/v2/users/{user_id}/following")
def list_user_following(
    user_id: str,
    limit: int = RELATIONSHIP_LIST_DEFAULT_LIMIT,
    cursor: Optional[str] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        started_at = time.perf_counter()
        current_user = ensure_current_user_record(db, authorization)
        _target_profile, target_privacy = get_searchable_target_or_404(db, user_id)
        ensure_can_read_relationship_list(
            db,
            current_user.user.id,
            user_id,
            target_privacy,
            ErrorCode.NOT_AUTHORIZED_TO_VIEW_FOLLOWING
        )
        page_limit, offset = parse_relationship_list_page_params(limit, cursor)
        blocked_user_ids = load_blocked_counterpart_user_ids(db, current_user.user.id)
        following_query = (
            db.query(PilotProfile)
            .join(FollowEdge, FollowEdge.followed_user_id == PilotProfile.user_id)
            .filter(FollowEdge.follower_user_id == user_id)
        )
        following_query = filter_blocked_counterparts(
            following_query,
            FollowEdge.followed_user_id,
            blocked_user_ids
        )
        total = following_query.count()
        if current_user.user.id == user_id:
            following_query = following_query.outerjoin(
                FavoriteFollow,
                and_(
                    FavoriteFollow.user_id == current_user.user.id,
                    FavoriteFollow.favorite_user_id == FollowEdge.followed_user_id
                )
            )
            order_by = (
                case((FavoriteFollow.favorite_user_id.isnot(None), 0), else_=1),
                FavoriteFollow.updated_at.desc(),
                FollowEdge.created_at.desc(),
                FollowEdge.followed_user_id.asc(),
            )
        else:
            order_by = (
                FollowEdge.created_at.desc(),
                FollowEdge.followed_user_id.asc(),
            )
        profiles = (
            following_query
            .order_by(*order_by)
            .offset(offset)
            .limit(page_limit)
            .all()
        )
        response = build_relationship_list_page_response(
            db,
            current_user.user.id,
            profiles,
            total,
            page_limit,
            offset
        )
        record_relationship_list_query_metric(
            offset=offset,
            response_ms=(time.perf_counter() - started_at) * 1000.0
        )
        return response
    finally:
        db.close()


@app.put("/api/v2/me/favorites/{user_id}")
def favorite_follow(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        target_user_id = user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_id is required"
            )
        if target_user_id == current_user.user.id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="cannot favorite yourself"
            )
        get_user_record_or_404(db, target_user_id)
        if not follow_edge_exists(db, current_user.user.id, target_user_id):
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FAVORITE_REQUIRES_FOLLOWING,
                detail="favorite requires following this user"
            )
        now = utcnow()
        favorite = (
            db.query(FavoriteFollow)
            .filter(
                FavoriteFollow.user_id == current_user.user.id,
                FavoriteFollow.favorite_user_id == target_user_id
            )
            .first()
        )
        created = favorite is None
        if favorite is None:
            favorite = FavoriteFollow(
                user_id=current_user.user.id,
                favorite_user_id=target_user_id,
                created_at=now,
                updated_at=now
            )
            db.add(favorite)
        else:
            favorite.updated_at = now
        invalidate_social_cache_for_users(current_user.user.id)
        db.commit()
        return {
            "ok": True,
            "user_id": target_user_id,
            "favorite": True,
            "created": created,
        }
    finally:
        db.close()


@app.delete("/api/v2/me/favorites/{user_id}")
def unfavorite_follow(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        target_user_id = user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_id is required"
            )
        get_user_record_or_404(db, target_user_id)
        removed = remove_favorite_follow(db, current_user.user.id, target_user_id)
        if removed:
            invalidate_social_cache_for_users(current_user.user.id)
        db.commit()
        return {
            "ok": True,
            "user_id": target_user_id,
            "favorite": False,
            "removed": removed,
        }
    finally:
        db.close()


@app.delete("/api/v2/users/{user_id}/follow")
def unfollow_user(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        target_user_id = user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_id is required"
            )
        get_user_record_or_404(db, target_user_id)

        edge = (
            db.query(FollowEdge)
            .filter(
                FollowEdge.follower_user_id == current_user.user.id,
                FollowEdge.followed_user_id == target_user_id
            )
            .first()
        )
        removed = edge is not None
        if edge is not None:
            decrement_relationship_counters_for_follow_edge(
                db,
                edge.follower_user_id,
                edge.followed_user_id,
                utcnow()
            )
            remove_favorite_follow(db, edge.follower_user_id, edge.followed_user_id)
            invalidate_social_cache_for_users(edge.follower_user_id, edge.followed_user_id)
            db.delete(edge)
            db.commit()

        lookup = load_relationship_lookup(db, current_user.user.id, [target_user_id])
        return {
            "ok": True,
            "removed": removed,
            "relationship_state": build_relationship_state(lookup, target_user_id)
        }
    finally:
        db.close()


@app.delete("/api/v2/me/followers/{user_id}")
def remove_follower(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follower_user_id = user_id.strip()
        if not follower_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_id is required"
            )
        get_user_record_or_404(db, follower_user_id)

        edge = (
            db.query(FollowEdge)
            .filter(
                FollowEdge.follower_user_id == follower_user_id,
                FollowEdge.followed_user_id == current_user.user.id
            )
            .first()
        )
        removed = edge is not None
        if edge is not None:
            decrement_relationship_counters_for_follow_edge(
                db,
                edge.follower_user_id,
                edge.followed_user_id,
                utcnow()
            )
            remove_favorite_follow(db, edge.follower_user_id, edge.followed_user_id)
            invalidate_social_cache_for_users(edge.follower_user_id, edge.followed_user_id)
            db.delete(edge)
            db.commit()

        lookup = load_relationship_lookup(db, current_user.user.id, [follower_user_id])
        return {
            "ok": True,
            "removed": removed,
            "relationship_state": build_relationship_state(lookup, follower_user_id)
        }
    finally:
        db.close()


@app.post("/api/v2/blocks")
def block_user(
    request: BlockCreateRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        target_user_id = request.target_user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="target_user_id is required"
            )
        if target_user_id == current_user.user.id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.BLOCK_SELF,
                detail="cannot block yourself"
            )
        get_user_record_or_404(db, target_user_id)

        block = (
            db.query(UserBlock)
            .filter(
                UserBlock.blocker_user_id == current_user.user.id,
                UserBlock.blocked_user_id == target_user_id
            )
            .first()
        )
        now = utcnow()
        if block is None:
            db.add(
                UserBlock(
                    blocker_user_id=current_user.user.id,
                    blocked_user_id=target_user_id,
                    created_at=now
                )
            )
        remove_relationship_rows_between_users(
            db,
            current_user.user.id,
            target_user_id,
            now
        )
        invalidate_social_cache_for_users(current_user.user.id, target_user_id)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            existing_block = (
                db.query(UserBlock)
                .filter(
                    UserBlock.blocker_user_id == current_user.user.id,
                    UserBlock.blocked_user_id == target_user_id
                )
                .first()
            )
            if existing_block is None:
                raise
            remove_relationship_rows_between_users(
                db,
                current_user.user.id,
                target_user_id,
                utcnow()
            )
            invalidate_social_cache_for_users(current_user.user.id, target_user_id)
            db.commit()
        return {
            "ok": True,
            "blocked": True,
            "target_user_id": target_user_id
        }
    finally:
        db.close()


@app.delete("/api/v2/blocks/{user_id}")
def unblock_user(
    user_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        target_user_id = user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="user_id is required"
            )
        get_user_record_or_404(db, target_user_id)

        block = (
            db.query(UserBlock)
            .filter(
                UserBlock.blocker_user_id == current_user.user.id,
                UserBlock.blocked_user_id == target_user_id
            )
            .first()
        )
        removed = block is not None
        if block is not None:
            db.delete(block)
            invalidate_social_cache_for_users(current_user.user.id, target_user_id)
            db.commit()
        return {
            "ok": True,
            "removed": removed,
            "target_user_id": target_user_id
        }
    finally:
        db.close()


@app.post("/api/v2/follow-requests")
def create_follow_request(
    request: FollowRequestCreateRequest,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        ensure_profile_complete(
            current_user.profile,
            detail="complete your profile before sending follow requests"
        )
        target_user_id = request.target_user_id.strip()
        if not target_user_id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.VALIDATION_ERROR,
                detail="target_user_id is required"
            )
        if target_user_id == current_user.user.id:
            raise ApiHTTPException(
                status_code=422,
                code=ErrorCode.FOLLOW_REQUEST_SELF,
                detail="cannot follow yourself"
            )

        target_profile, target_privacy = get_searchable_target_or_404(db, target_user_id)
        ensure_relationship_not_blocked(db, current_user.user.id, target_user_id)
        existing_edge = (
            db.query(FollowEdge)
            .filter(
                FollowEdge.follower_user_id == current_user.user.id,
                FollowEdge.followed_user_id == target_user_id
            )
            .first()
        )
        if existing_edge is not None:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.ALREADY_FOLLOWING,
                detail="already following"
            )
        if target_privacy.follow_policy == "closed":
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_CLOSED,
                detail="this pilot is not accepting followers"
            )

        follow_request = (
            db.query(FollowRequest)
            .filter(
                FollowRequest.requester_user_id == current_user.user.id,
                FollowRequest.target_user_id == target_user_id
            )
            .first()
        )
        if follow_request is not None and follow_request.status == FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_ALREADY_EXISTS,
                detail="follow request already pending"
            )

        now = utcnow()
        final_status = (
            FOLLOW_REQUEST_STATUS_ACCEPTED
            if target_privacy.follow_policy == "auto_approve"
            else FOLLOW_REQUEST_STATUS_PENDING
        )
        ensure_livefollow_following_capacity_available(db, current_user.user.id)
        responded_at = now if final_status == FOLLOW_REQUEST_STATUS_ACCEPTED else None
        if follow_request is None:
            follow_request = FollowRequest(
                id=str(uuid.uuid4()),
                requester_user_id=current_user.user.id,
                target_user_id=target_user_id,
                status=final_status,
                responded_at=responded_at,
                created_at=now,
                updated_at=now
            )
            db.add(follow_request)
        else:
            follow_request.status = final_status
            follow_request.responded_at = responded_at
            follow_request.created_at = now
            follow_request.updated_at = now

        if final_status == FOLLOW_REQUEST_STATUS_ACCEPTED:
            target_already_follows_requester = approved_follow_edge_exists(
                db,
                target_user_id,
                current_user.user.id
            )
            ensure_follow_edge(db, current_user.user.id, target_user_id, now)
            enqueue_follow_notification_event(
                db=db,
                event_type=(
                    NOTIFICATION_EVENT_FOLLOW_MUTUAL
                    if target_already_follows_requester
                    else NOTIFICATION_EVENT_FOLLOW_NEW_FOLLOWER
                ),
                recipient_user_id=target_user_id,
                actor_user_id=current_user.user.id,
                follow_request_id=follow_request.id,
                occurred_at=now
            )
        else:
            enqueue_follow_notification_event(
                db=db,
                event_type=NOTIFICATION_EVENT_FOLLOW_REQUEST_RECEIVED,
                recipient_user_id=target_user_id,
                actor_user_id=current_user.user.id,
                follow_request_id=follow_request.id,
                occurred_at=now
            )

        db.commit()
        lookup = load_relationship_lookup(db, current_user.user.id, [target_user_id])
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            target_profile,
            lookup
        )
    finally:
        db.close()


@app.get("/api/v2/follow-requests/incoming")
def list_incoming_follow_requests(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    limit: int = RELATIONSHIP_LIST_DEFAULT_LIMIT,
    cursor: Optional[str] = None
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        page_limit, offset = parse_relationship_list_page_params(limit, cursor)
        blocked_user_ids = load_blocked_counterpart_user_ids(db, current_user.user.id)
        incoming_query = (
            db.query(FollowRequest, PilotProfile)
            .join(PilotProfile, PilotProfile.user_id == FollowRequest.requester_user_id)
            .filter(
                FollowRequest.target_user_id == current_user.user.id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING
            )
        )
        incoming_query = filter_blocked_counterparts(
            incoming_query,
            FollowRequest.requester_user_id,
            blocked_user_ids
        )
        total = incoming_query.count()
        request_rows = (
            incoming_query
            .order_by(FollowRequest.updated_at.desc(), FollowRequest.id.desc())
            .offset(offset)
            .limit(page_limit)
            .all()
        )
        counterpart_ids = [profile.user_id for _request, profile in request_rows]
        lookup = load_relationship_lookup(db, current_user.user.id, counterpart_ids)
        next_offset = offset + page_limit
        return {
            "requests": [
                build_follow_request_response(
                    current_user.user.id,
                    follow_request,
                    profile,
                    lookup
                )
                for follow_request, profile in request_rows
            ],
            "total": total,
            "next_cursor": str(next_offset) if next_offset < total else None
        }
    finally:
        db.close()


@app.get("/api/v2/follow-requests/outgoing")
def list_outgoing_follow_requests(
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    limit: int = RELATIONSHIP_LIST_DEFAULT_LIMIT,
    cursor: Optional[str] = None
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        page_limit, offset = parse_relationship_list_page_params(limit, cursor)
        blocked_user_ids = load_blocked_counterpart_user_ids(db, current_user.user.id)
        outgoing_query = (
            db.query(FollowRequest, PilotProfile)
            .join(PilotProfile, PilotProfile.user_id == FollowRequest.target_user_id)
            .filter(
                FollowRequest.requester_user_id == current_user.user.id,
                FollowRequest.status == FOLLOW_REQUEST_STATUS_PENDING
            )
        )
        outgoing_query = filter_blocked_counterparts(
            outgoing_query,
            FollowRequest.target_user_id,
            blocked_user_ids
        )
        total = outgoing_query.count()
        request_rows = (
            outgoing_query
            .order_by(FollowRequest.updated_at.desc(), FollowRequest.id.desc())
            .offset(offset)
            .limit(page_limit)
            .all()
        )
        counterpart_ids = [profile.user_id for _request, profile in request_rows]
        lookup = load_relationship_lookup(db, current_user.user.id, counterpart_ids)
        next_offset = offset + page_limit
        return {
            "requests": [
                build_follow_request_response(
                    current_user.user.id,
                    follow_request,
                    profile,
                    lookup
                )
                for follow_request, profile in request_rows
            ],
            "total": total,
            "next_cursor": str(next_offset) if next_offset < total else None
        }
    finally:
        db.close()


def get_follow_request_for_target_or_404(
    db,
    request_id: str,
    target_user_id: str
) -> FollowRequest:
    follow_request = (
        db.query(FollowRequest)
        .filter(
            FollowRequest.id == request_id,
            FollowRequest.target_user_id == target_user_id
        )
        .first()
    )
    if follow_request is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.FOLLOW_REQUEST_NOT_FOUND,
            detail="follow request not found"
        )
    return follow_request


def get_follow_request_for_requester_or_404(
    db,
    request_id: str,
    requester_user_id: str
) -> FollowRequest:
    follow_request = (
        db.query(FollowRequest)
        .filter(
            FollowRequest.id == request_id,
            FollowRequest.requester_user_id == requester_user_id
        )
        .first()
    )
    if follow_request is None:
        raise ApiHTTPException(
            status_code=404,
            code=ErrorCode.FOLLOW_REQUEST_NOT_FOUND,
            detail="follow request not found"
        )
    return follow_request


@app.delete("/api/v2/follow-requests/{request_id}")
def cancel_outgoing_follow_request(
    request_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follow_request = get_follow_request_for_requester_or_404(
            db,
            request_id,
            current_user.user.id
        )
        if follow_request.status != FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
                detail="follow request is not pending"
            )

        db.delete(follow_request)
        db.commit()
        return {
            "ok": True,
            "request_id": request_id
        }
    finally:
        db.close()


@app.post("/api/v2/follow-requests/{request_id}/accept")
def accept_follow_request(
    request_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follow_request = get_follow_request_for_target_or_404(
            db,
            request_id,
            current_user.user.id
        )
        if follow_request.status != FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
                detail="follow request is not pending"
            )
        ensure_relationship_not_blocked(
            db,
            follow_request.requester_user_id,
            follow_request.target_user_id
        )

        now = utcnow()
        target_already_follows_requester = approved_follow_edge_exists(
            db,
            follow_request.target_user_id,
            follow_request.requester_user_id
        )
        ensure_follow_edge(
            db,
            follow_request.requester_user_id,
            follow_request.target_user_id,
            now
        )
        follow_request.status = FOLLOW_REQUEST_STATUS_ACCEPTED
        follow_request.responded_at = now
        follow_request.updated_at = now
        enqueue_follow_notification_event(
            db=db,
            event_type=(
                NOTIFICATION_EVENT_FOLLOW_MUTUAL
                if target_already_follows_requester
                else NOTIFICATION_EVENT_FOLLOW_REQUEST_ACCEPTED
            ),
            recipient_user_id=follow_request.requester_user_id,
            actor_user_id=follow_request.target_user_id,
            follow_request_id=follow_request.id,
            occurred_at=now
        )
        db.commit()

        requester_profile, _requester_privacy = get_searchable_target_or_404(
            db,
            follow_request.requester_user_id
        )
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [follow_request.requester_user_id]
        )
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            requester_profile,
            lookup
        )
    finally:
        db.close()


@app.post("/api/v2/follow-requests/{request_id}/decline")
def decline_follow_request(
    request_id: str,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        follow_request = get_follow_request_for_target_or_404(
            db,
            request_id,
            current_user.user.id
        )
        if follow_request.status != FOLLOW_REQUEST_STATUS_PENDING:
            raise ApiHTTPException(
                status_code=409,
                code=ErrorCode.FOLLOW_REQUEST_NOT_PENDING,
                detail="follow request is not pending"
            )

        now = utcnow()
        follow_request.status = FOLLOW_REQUEST_STATUS_DECLINED
        follow_request.responded_at = now
        follow_request.updated_at = now
        db.commit()

        requester_profile, _requester_privacy = get_searchable_target_or_404(
            db,
            follow_request.requester_user_id
        )
        lookup = load_relationship_lookup(
            db,
            current_user.user.id,
            [follow_request.requester_user_id]
        )
        return build_follow_request_response(
            current_user.user.id,
            follow_request,
            requester_profile,
            lookup
        )
    finally:
        db.close()


@app.get("/api/v2/live/following/active")
def get_following_active_live_sessions(
    limit: int = RELATIONSHIP_LIST_DEFAULT_LIMIT,
    cursor: Optional[str] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        page_limit, offset = parse_relationship_list_page_params(limit, cursor)
        record_active_list_refresh_metric(offset)
        blocked_user_ids = load_blocked_counterpart_user_ids(db, current_user.user.id)
        following_active_query = (
            db.query(LiveSession, PilotProfile)
            .join(
                FollowEdge,
                FollowEdge.followed_user_id == LiveSession.owner_user_id
            )
            .outerjoin(PilotProfile, PilotProfile.user_id == LiveSession.owner_user_id)
            .filter(
                FollowEdge.follower_user_id == current_user.user.id,
                LiveSession.owner_user_id.isnot(None),
                LiveSession.owner_user_id != current_user.user.id,
                LiveSession.status != "ended",
                LiveSession.last_position_at.isnot(None),
                LiveSession.visibility.in_(
                    [LIVE_VISIBILITY_FOLLOWERS, LIVE_VISIBILITY_PUBLIC]
                )
            )
        )
        following_active_query = filter_blocked_counterparts(
            following_active_query,
            LiveSession.owner_user_id,
            blocked_user_ids
        )
        total = following_active_query.count()
        rows = (
            following_active_query
            .order_by(
                LiveSession.last_position_at.desc(),
                LiveSession.created_at.desc(),
                LiveSession.id.asc()
            )
            .offset(offset)
            .limit(page_limit)
            .all()
        )
        next_offset = offset + len(rows)
        next_cursor = str(next_offset) if next_offset < total else None
        return {
            "total": total,
            "items": [
                build_authorized_live_active_item(session, owner_profile)
                for session, owner_profile in rows
            ],
            "next_cursor": next_cursor,
            "generated_at": to_iso_utc(utcnow())
        }
    finally:
        db.close()


@app.get("/api/v2/live/users/{user_id}")
def get_live_session_for_user(
    user_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = select_latest_owned_live_session(db, user_id)
        if session is None or not can_user_view_live_session(db, current_user.user.id, session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )
        enforce_live_read_rate_limit(
            session_id=session.id,
            user_id=current_user.user.id,
            client_ip=live_read_client_ip(request)
        )
        if record_authorized_live_session_viewer_if_needed(
            db,
            session,
            current_user.user.id,
            utcnow()
        ):
            db.commit()
        owner_profile = (
            db.query(PilotProfile)
            .filter(PilotProfile.user_id == session.owner_user_id)
            .first()
        )
        return build_measured_live_response(
            db,
            session,
            live_response_cache_key(session.id, LIVE_RESPONSE_CACHE_VARIANT_V2_USER),
            owner_profile
        )
    finally:
        db.close()


@app.get("/api/v2/live/session/{session_id}")
def get_authenticated_live_session(
    session_id: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
):
    db = SessionLocal()
    try:
        current_user = ensure_current_user_record(db, authorization)
        session = get_session_or_404(db, session_id)
        if not can_user_view_live_session(db, current_user.user.id, session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )
        enforce_live_read_rate_limit(
            session_id=session.id,
            user_id=current_user.user.id,
            client_ip=live_read_client_ip(request)
        )
        if record_authorized_live_session_viewer_if_needed(
            db,
            session,
            current_user.user.id,
            utcnow()
        ):
            db.commit()
        owner_profile = (
            db.query(PilotProfile)
            .filter(PilotProfile.user_id == session.owner_user_id)
            .first()
        )
        return build_measured_live_response(
            db,
            session,
            live_response_cache_key(session.id, LIVE_RESPONSE_CACHE_VARIANT_V2_SESSION),
            owner_profile
        )
    finally:
        db.close()


@app.get("/api/v1/live/active")
def get_active_live_sessions():
    db = SessionLocal()
    try:
        # Conservative inclusion: only sessions with at least one accepted position are listed.
        sessions = (
            db.query(LiveSession)
            .filter(
                LiveSession.status != "ended",
                LiveSession.visibility == LIVE_VISIBILITY_PUBLIC,
                LiveSession.last_position_at.isnot(None)
            )
            .order_by(
                LiveSession.last_position_at.desc(),
                LiveSession.created_at.desc(),
                LiveSession.id.asc()
            )
            .all()
        )

        return [build_live_active_item(session) for session in sessions]
    finally:
        db.close()


@app.get("/api/v1/live/{session_id}")
def get_live(session_id: str, request: Request):
    db = SessionLocal()
    try:
        session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
        if not session or not is_public_live_session(session):
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )

        enforce_live_read_rate_limit(
            session_id=session.id,
            user_id=None,
            client_ip=live_read_client_ip(request)
        )
        return build_measured_live_response(
            db,
            session,
            live_response_cache_key(session.id, LIVE_RESPONSE_CACHE_VARIANT_V1_SESSION)
        )
    finally:
        db.close()


@app.get("/api/v1/live/share/{share_code}")
def get_live_by_share_code(share_code: str, request: Request):
    db = SessionLocal()
    try:
        session = (
            db.query(LiveSession)
            .filter(
                LiveSession.share_code == share_code,
                LiveSession.visibility == LIVE_VISIBILITY_PUBLIC
            )
            .first()
        )
        if not session:
            raise ApiHTTPException(
                status_code=404,
                code=ErrorCode.SESSION_NOT_FOUND,
                detail="not found"
            )

        enforce_live_read_rate_limit(
            session_id=session.id,
            user_id=None,
            client_ip=live_read_client_ip(request)
        )
        return build_measured_live_response(
            db,
            session,
            live_response_cache_key(session.id, LIVE_RESPONSE_CACHE_VARIANT_V1_SHARE)
        )
    finally:
        db.close()
