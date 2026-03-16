"""OIDC + mTLS authentication module for the Federated Data Space Connector.

This module provides the :class:`KeycloakAuthBackend` class that authenticates
requests using two complementary mechanisms:

  1. **OIDC token validation** – JWT access tokens issued by Keycloak are
     validated locally using cached JWK signing keys fetched from the
     ``/realms/{realm}/protocol/openid-connect/certs`` endpoint.  Per-request
     token introspection is explicitly avoided (spec requirement 3).
  2. **mTLS client certificate extraction** – for service-to-service trust,
     the distinguished name (DN) from the client's TLS certificate is
     extracted from request headers set by the TLS-terminating reverse proxy.

Key design decisions:
  - JWK keys are fetched once and cached with a configurable TTL.  A forced
    refresh is triggered when a token's ``kid`` (key ID) does not match any
    cached key, handling key rotation gracefully.
  - Keycloak 26.x URL format is used throughout – **no** ``/auth`` prefix
    (the legacy prefix was removed in Keycloak 26.x).
  - Token claims map directly to the custom protocol mappers configured in
    the Keycloak realm export:
      * ``participant_id`` – unique participant identifier
      * ``participant_type`` – participant category (``dso``, ``aggregator``,
        ``prosumer``, ``catalog``)
      * ``roles`` – realm-level roles (e.g. ``dso-operator``, ``admin``)
  - The :class:`AuthenticatedUser` Pydantic model provides a structured
    representation of the authenticated identity for downstream consumers
    (middleware, policy engine, audit logger).
  - All timestamps are timezone-aware UTC.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

import httpx
from jose import jwt
from jose.exceptions import ExpiredSignatureError, JWTClaimsError, JWTError
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AuthError(Exception):
    """Base exception for authentication errors."""


class JWKFetchError(AuthError):
    """Raised when JWK keys cannot be fetched from the Keycloak JWKS endpoint."""

    def __init__(self, url: str, detail: str) -> None:
        self.url = url
        self.detail = detail
        super().__init__(f"Failed to fetch JWK keys from '{url}': {detail}")


class TokenValidationError(AuthError):
    """Raised when a JWT token fails validation."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"Token validation failed: {detail}")


class TokenExpiredError(TokenValidationError):
    """Raised when a JWT token has expired."""

    def __init__(self) -> None:
        super().__init__("token has expired")


class SigningKeyNotFoundError(AuthError):
    """Raised when the token's signing key cannot be found in the JWKS."""

    def __init__(self, kid: str) -> None:
        self.kid = kid
        super().__init__(
            f"Signing key with kid='{kid}' not found in JWKS after refresh"
        )


class CertificateExtractionError(AuthError):
    """Raised when the mTLS client certificate DN cannot be extracted."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"Client certificate extraction failed: {detail}")


# ---------------------------------------------------------------------------
# Authenticated user model
# ---------------------------------------------------------------------------


class AuthenticatedUser(BaseModel):
    """Identity extracted from a validated OIDC token and/or mTLS certificate.

    This model is the output of :meth:`KeycloakAuthBackend.authenticate_token`
    and carries all the identity information needed by the policy engine,
    contract manager, and audit logger.
    """

    subject: str = Field(
        ..., description="OIDC subject identifier (Keycloak user ID)"
    )
    participant_id: str = Field(
        ...,
        description="Unique participant identifier from the 'participant_id' token claim",
    )
    participant_type: str = Field(
        ...,
        description="Participant category from the 'participant_type' token claim "
        "(e.g. 'dso', 'aggregator', 'prosumer')",
    )
    username: str = Field(
        ..., description="Human-readable username from the 'preferred_username' claim"
    )
    email: Optional[str] = Field(
        default=None, description="Email address from the 'email' claim"
    )
    roles: list[str] = Field(
        default_factory=list,
        description="Realm-level roles from the 'roles' token claim "
        "(e.g. ['dso-operator', 'admin'])",
    )
    organization: Optional[str] = Field(
        default=None,
        description="Organization from the 'organization' claim, or derived from "
        "participant_type if not present",
    )
    certificate_dn: Optional[str] = Field(
        default=None,
        description="Distinguished name from the mTLS client certificate "
        "(set by extract_client_certificate_dn)",
    )
    token_claims: dict[str, Any] = Field(
        default_factory=dict,
        description="Full decoded JWT claims for downstream inspection",
    )


# ---------------------------------------------------------------------------
# Header names for mTLS client certificate extraction
# ---------------------------------------------------------------------------

# Common headers set by TLS-terminating reverse proxies to convey the client
# certificate distinguished name.  Checked in order of preference.
_CLIENT_CERT_DN_HEADERS: list[str] = [
    "X-SSL-Client-DN",
    "X-Client-DN",
    "X-Forwarded-Client-Cert-DN",
]

# Envoy-style header that carries the full client certificate chain info.
_ENVOY_CLIENT_CERT_HEADER: str = "X-Forwarded-Client-Cert"


# ---------------------------------------------------------------------------
# Supported JWT signing algorithms
# ---------------------------------------------------------------------------

_SUPPORTED_ALGORITHMS: list[str] = ["RS256", "RS384", "RS512"]


# ---------------------------------------------------------------------------
# KeycloakAuthBackend
# ---------------------------------------------------------------------------


class KeycloakAuthBackend:
    """OIDC authentication backend using Keycloak JWK key validation.

    Validates JWT access tokens locally using cached JWK signing keys fetched
    from the Keycloak JWKS endpoint.  Does **not** perform per-request token
    introspection.

    Usage::

        backend = KeycloakAuthBackend(
            server_url="http://localhost:8080",
            realm="dataspace",
            audience="dso-node",
        )

        # Validate a Bearer token from a request
        user = backend.authenticate_token(token)
        print(user.participant_id, user.roles)

        # Extract mTLS client certificate DN from request headers
        cert_dn = KeycloakAuthBackend.extract_client_certificate_dn(
            headers={"X-SSL-Client-DN": "CN=dso-node,O=DataSpace,C=NL"}
        )

    Args:
        server_url: Keycloak server base URL.  Must **not** include the
            ``/auth`` prefix (Keycloak 26.x dropped it).  Defaults to the
            ``KEYCLOAK_SERVER_URL`` environment variable, or
            ``http://localhost:8080``.
        realm: Keycloak realm name.  Defaults to the ``KEYCLOAK_REALM``
            environment variable, or ``dataspace``.
        audience: Expected ``aud`` claim in the JWT.  When ``None``, audience
            validation is skipped.  Defaults to the ``KEYCLOAK_CLIENT_ID``
            environment variable, or ``None``.
        jwk_cache_ttl_seconds: How long (in seconds) to cache JWK keys before
            re-fetching.  Defaults to 300 (5 minutes).
    """

    def __init__(
        self,
        *,
        server_url: Optional[str] = None,
        realm: Optional[str] = None,
        audience: Optional[str] = None,
        jwk_cache_ttl_seconds: int = 300,
    ) -> None:
        self._server_url = (
            server_url
            or os.environ.get("KEYCLOAK_SERVER_URL", "http://localhost:8080")
        ).rstrip("/")
        self._realm = realm or os.environ.get("KEYCLOAK_REALM", "dataspace")
        self._audience = audience or os.environ.get("KEYCLOAK_CLIENT_ID")
        self._jwk_cache_ttl = jwk_cache_ttl_seconds

        # JWK key cache: stores the raw JWKS response and fetch timestamp.
        self._jwks_cache: Optional[dict[str, Any]] = None
        self._jwks_cache_timestamp: float = 0.0

    # -- public properties ---------------------------------------------------

    @property
    def server_url(self) -> str:
        """Return the configured Keycloak server URL."""
        return self._server_url

    @property
    def realm(self) -> str:
        """Return the configured Keycloak realm name."""
        return self._realm

    @property
    def jwks_url(self) -> str:
        """Return the full JWKS endpoint URL (Keycloak 26.x format, no /auth prefix)."""
        return (
            f"{self._server_url}/realms/{self._realm}"
            f"/protocol/openid-connect/certs"
        )

    @property
    def issuer(self) -> str:
        """Return the expected token issuer URL for the configured realm."""
        return f"{self._server_url}/realms/{self._realm}"

    # -- token authentication ------------------------------------------------

    def authenticate_token(self, token: str) -> AuthenticatedUser:
        """Validate a JWT access token and extract the authenticated user identity.

        The token is validated against cached JWK signing keys from Keycloak.
        If the token's ``kid`` does not match any cached key, the cache is
        refreshed once to handle key rotation.

        Args:
            token: The raw JWT access token string (without the ``Bearer``
                prefix).

        Returns:
            An :class:`AuthenticatedUser` with identity, roles, and
            organisation extracted from the token claims.

        Raises:
            TokenExpiredError: If the token has expired.
            TokenValidationError: If the token is invalid (bad signature,
                wrong issuer, missing required claims, etc.).
            SigningKeyNotFoundError: If the token's signing key is not in
                the JWKS even after a refresh.
            JWKFetchError: If JWK keys cannot be fetched from Keycloak.
        """
        # Decode the token header to get the kid (key ID) without verification.
        try:
            unverified_header = jwt.get_unverified_header(token)
        except JWTError as exc:
            raise TokenValidationError(f"malformed token header: {exc}") from exc

        kid = unverified_header.get("kid")
        if not kid:
            raise TokenValidationError("token header missing 'kid' field")

        # Find the signing key matching the kid.
        signing_key = self._find_signing_key(kid)

        # Decode and validate the token.
        options: dict[str, Any] = {
            "verify_aud": self._audience is not None,
            "verify_iss": True,
            "verify_exp": True,
            "verify_iat": True,
        }
        try:
            claims = jwt.decode(
                token,
                signing_key,
                algorithms=_SUPPORTED_ALGORITHMS,
                audience=self._audience,
                issuer=self.issuer,
                options=options,
            )
        except ExpiredSignatureError as exc:
            raise TokenExpiredError() from exc
        except JWTClaimsError as exc:
            raise TokenValidationError(f"invalid claims: {exc}") from exc
        except JWTError as exc:
            raise TokenValidationError(f"token verification failed: {exc}") from exc

        # Extract identity from validated claims.
        return self._extract_user(claims)

    # -- mTLS certificate extraction -----------------------------------------

    @staticmethod
    def extract_client_certificate_dn(
        headers: dict[str, str],
    ) -> Optional[str]:
        """Extract the mTLS client certificate distinguished name from request headers.

        TLS-terminating reverse proxies (nginx, envoy, traefik) typically
        forward the client certificate DN via a well-known header.  This
        method checks several common header names and returns the first
        non-empty value found.

        For Envoy, the ``X-Forwarded-Client-Cert`` header is parsed to
        extract the ``By=`` or ``Subject=`` field.

        Args:
            headers: A dictionary of HTTP request headers.  Keys should be
                case-insensitive header names (as provided by most ASGI
                frameworks).

        Returns:
            The client certificate distinguished name string, or ``None``
            if no certificate header is present.
        """
        # Normalise header keys to title-case for consistent lookup.
        normalised: dict[str, str] = {
            k.title(): v for k, v in headers.items()
        }

        # Check direct DN headers first.
        for header_name in _CLIENT_CERT_DN_HEADERS:
            value = normalised.get(header_name.title())
            if value:
                return value.strip()

        # Check Envoy-style header (contains key=value pairs separated by ;).
        xfcc = normalised.get(_ENVOY_CLIENT_CERT_HEADER.title())
        if xfcc:
            dn = _parse_xfcc_subject(xfcc)
            if dn:
                return dn

        return None

    # -- private helpers: JWK key management ---------------------------------

    def _fetch_jwk_keys(self) -> dict[str, Any]:
        """Fetch the JWKS document from the Keycloak JWKS endpoint.

        Returns:
            The parsed JWKS JSON response containing signing keys.

        Raises:
            JWKFetchError: If the HTTP request fails or returns a non-200
                status code.
        """
        url = self.jwks_url
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(url)
            if response.status_code != 200:
                raise JWKFetchError(
                    url,
                    f"HTTP {response.status_code}: {response.text[:200]}",
                )
            return response.json()
        except httpx.HTTPError as exc:
            raise JWKFetchError(url, str(exc)) from exc
        except ValueError as exc:
            raise JWKFetchError(url, f"invalid JSON response: {exc}") from exc

    def _get_signing_keys(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return the cached JWKS document, refreshing if the cache is stale.

        Args:
            force_refresh: If ``True``, bypass the TTL and fetch fresh keys
                immediately.  Used when a token's ``kid`` does not match
                any cached key.

        Returns:
            The JWKS document containing signing keys.

        Raises:
            JWKFetchError: If keys cannot be fetched.
        """
        now = time.monotonic()
        cache_expired = (now - self._jwks_cache_timestamp) >= self._jwk_cache_ttl

        if self._jwks_cache is None or cache_expired or force_refresh:
            self._jwks_cache = self._fetch_jwk_keys()
            self._jwks_cache_timestamp = time.monotonic()

        return self._jwks_cache

    def _find_signing_key(self, kid: str) -> dict[str, Any]:
        """Find the JWK signing key matching the given key ID.

        If the ``kid`` is not found in the cache, the cache is refreshed
        once to handle key rotation.

        Args:
            kid: The ``kid`` value from the JWT header.

        Returns:
            The JWK key dictionary matching the ``kid``.

        Raises:
            SigningKeyNotFoundError: If no matching key is found even after
                a cache refresh.
            JWKFetchError: If keys cannot be fetched.
        """
        # Try with current cache first.
        jwks = self._get_signing_keys()
        key = self._match_key(jwks, kid)
        if key is not None:
            return key

        # Key not found – force refresh (key rotation may have occurred).
        jwks = self._get_signing_keys(force_refresh=True)
        key = self._match_key(jwks, kid)
        if key is not None:
            return key

        raise SigningKeyNotFoundError(kid)

    @staticmethod
    def _match_key(jwks: dict[str, Any], kid: str) -> Optional[dict[str, Any]]:
        """Return the key from the JWKS document matching the given kid."""
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                return key
        return None

    # -- private helpers: claim extraction ------------------------------------

    @staticmethod
    def _extract_user(claims: dict[str, Any]) -> AuthenticatedUser:
        """Build an :class:`AuthenticatedUser` from validated JWT claims.

        Extracts standard OIDC claims and the custom Keycloak protocol
        mapper claims (``participant_id``, ``participant_type``, ``roles``).

        Args:
            claims: The decoded and validated JWT claims dictionary.

        Returns:
            An :class:`AuthenticatedUser` populated from the claims.

        Raises:
            TokenValidationError: If required claims are missing.
        """
        subject = claims.get("sub")
        if not subject:
            raise TokenValidationError("token missing required 'sub' claim")

        participant_id = claims.get("participant_id", "")
        participant_type = claims.get("participant_type", "")
        username = claims.get("preferred_username", subject)
        email = claims.get("email")

        # Roles: check top-level 'roles' claim (custom mapper), then fall
        # back to realm_access.roles (standard Keycloak structure).
        roles = claims.get("roles", [])
        if not roles and isinstance(claims.get("realm_access"), dict):
            roles = claims["realm_access"].get("roles", [])
        if isinstance(roles, str):
            roles = [roles]

        # Organization: check 'organization' claim, fall back to participant_type.
        organization = claims.get("organization") or participant_type or None

        return AuthenticatedUser(
            subject=subject,
            participant_id=participant_id,
            participant_type=participant_type,
            username=username,
            email=email,
            roles=list(roles),
            organization=organization,
            token_claims=dict(claims),
        )


# ---------------------------------------------------------------------------
# Private utility: parse Envoy X-Forwarded-Client-Cert header
# ---------------------------------------------------------------------------


def _parse_xfcc_subject(xfcc_value: str) -> Optional[str]:
    """Extract the Subject DN from an Envoy ``X-Forwarded-Client-Cert`` header.

    The header value is a semicolon-separated list of key=value or
    key="value" pairs.  We look for the ``Subject`` key.

    Args:
        xfcc_value: Raw header value.

    Returns:
        The subject DN string, or ``None`` if not found.
    """
    for part in xfcc_value.split(";"):
        part = part.strip()
        if part.lower().startswith("subject="):
            value = part[len("subject="):]
            # Remove surrounding quotes if present.
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            return value.strip()
    return None
