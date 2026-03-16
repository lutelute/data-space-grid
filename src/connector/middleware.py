"""FastAPI middleware for the Federated Data Space Connector.

The ``ConnectorMiddleware`` wraps every request on participant nodes with
three non-negotiable layers:

  1. **Authentication** -- validates the JWT Bearer token via
     :class:`~src.connector.auth.KeycloakAuthBackend` and rejects
     unauthenticated requests with HTTP 401.
  2. **Policy check** -- when a :class:`~src.connector.policy.PolicyEngine`
     is provided, verifies that the authenticated user's participant is
     registered with the engine.  Unregistered participants are rejected
     with HTTP 403.  Detailed per-request policy evaluation (contracts,
     purposes, assets) should be performed by route handlers using
     ``request.state.policy_engine``.
  3. **Audit logging** -- hashes request and response bodies via
     :func:`~src.connector.audit.compute_hash` and records the exchange
     via :class:`~src.connector.audit.AuditLogger`.  Audit logging is
     non-optional: if the audit write fails, the request itself fails
     with HTTP 500.

Key design decisions:
  - Health endpoints (``/health``, ``/healthz``, ``/ready``) are exempt
    from authentication to allow infrastructure probes.  Additional
    paths can be configured via the ``skip_auth_paths`` parameter.
  - The ``X-Emergency-Override`` request header is extracted and stored
    in ``request.state.emergency_override`` (boolean) so downstream route
    handlers and the policy engine can apply the DSO emergency override
    path.
  - The authenticated user identity is stored in ``request.state.user``
    as an :class:`~src.connector.auth.AuthenticatedUser` instance.
  - The ``X-Purpose-Tag`` and ``X-Contract-ID`` request headers supply
    metadata for audit entries.  When absent they default to
    ``"unknown"``.
  - mTLS client certificate DN is extracted from request headers (when
    present) and attached to the authenticated user identity.
  - All error responses use JSON with a ``detail`` key, consistent with
    FastAPI conventions.
  - The middleware uses Starlette's ``BaseHTTPMiddleware`` for
    compatibility with both FastAPI and raw Starlette applications.
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from src.connector.audit import AuditLogger
from src.connector.auth import AuthError, AuthenticatedUser, KeycloakAuthBackend
from src.connector.models import AuditAction, AuditOutcome
from src.connector.policy import ParticipantNotRegisteredError, PolicyEngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default paths that skip authentication (infrastructure health probes).
_DEFAULT_SKIP_AUTH_PATHS: frozenset[str] = frozenset({
    "/health",
    "/healthz",
    "/ready",
})

# Request header carrying the emergency override flag.
_EMERGENCY_OVERRIDE_HEADER: str = "X-Emergency-Override"

# Request header carrying the stated purpose for audit entries.
_PURPOSE_TAG_HEADER: str = "X-Purpose-Tag"

# Request header carrying the contract ID for audit entries.
_CONTRACT_ID_HEADER: str = "X-Contract-ID"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MiddlewareError(Exception):
    """Base exception for middleware errors."""


class AuditFailureError(MiddlewareError):
    """Raised when audit logging fails, causing the entire request to fail.

    This is a terminal error: the request cannot proceed because the
    data space requires every exchange to be audited (spec Pattern 4 --
    Audit Trail on Every Exchange).
    """

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"Audit logging failed, request rejected: {detail}")


class _MissingCredentialsError(AuthError):
    """Internal: raised when the Authorization header is missing or malformed.

    This exception is caught by :meth:`ConnectorMiddleware.dispatch` and
    converted to an HTTP 401 response.  It is not part of the public API.
    """

    def __init__(self) -> None:
        super().__init__(
            "Missing or invalid Authorization header: expected 'Bearer <token>'"
        )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _map_method_to_action(method: str) -> AuditAction:
    """Map an HTTP method to an :class:`~src.connector.models.AuditAction`.

    Args:
        method: The HTTP request method (e.g. ``"GET"``, ``"POST"``).

    Returns:
        The corresponding :class:`AuditAction`.
    """
    method_upper = method.upper()
    if method_upper == "GET":
        return AuditAction.READ
    if method_upper in ("POST", "PUT", "PATCH", "DELETE"):
        return AuditAction.WRITE
    # Default to READ for other methods (HEAD, OPTIONS, etc.).
    return AuditAction.READ


def _map_status_to_outcome(status_code: int) -> AuditOutcome:
    """Map an HTTP status code to an :class:`~src.connector.models.AuditOutcome`.

    Args:
        status_code: The HTTP response status code.

    Returns:
        The corresponding :class:`AuditOutcome`.
    """
    if 200 <= status_code < 300:
        return AuditOutcome.SUCCESS
    if 400 <= status_code < 500:
        return AuditOutcome.DENIED
    return AuditOutcome.ERROR


def _json_error(
    status_code: int,
    detail: str,
    *,
    headers: Optional[dict[str, str]] = None,
) -> JSONResponse:
    """Create a JSON error response consistent with FastAPI conventions.

    Args:
        status_code: HTTP status code.
        detail: Human-readable error description.
        headers: Optional additional response headers.

    Returns:
        A :class:`JSONResponse` with a ``{"detail": ...}`` body.
    """
    return JSONResponse(
        status_code=status_code,
        content={"detail": detail},
        headers=headers,
    )


# ---------------------------------------------------------------------------
# ConnectorMiddleware
# ---------------------------------------------------------------------------


class ConnectorMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware wrapping every request with auth, policy, and audit.

    The middleware is the enforcement backbone of the Data Space Connector.
    It guarantees that:

    - No unauthenticated request reaches a route handler (except health
      endpoints).
    - No unregistered participant can access protected endpoints (when a
      policy engine is configured).
    - Every authenticated exchange (whether successful, denied, or
      erroneous) is recorded in the audit trail.
    - Failing to record an audit entry causes the request to fail.

    Usage::

        from fastapi import FastAPI
        from src.connector.middleware import ConnectorMiddleware
        from src.connector.auth import KeycloakAuthBackend
        from src.connector.audit import AuditLogger

        app = FastAPI(title="DSO Node - Federated Data Space")
        app.add_middleware(
            ConnectorMiddleware,
            auth_backend=KeycloakAuthBackend(),
            audit_logger=AuditLogger(),
            participant_id="dso-001",
        )

    Route handlers can access the authenticated user and emergency flag::

        from starlette.requests import Request

        @router.get("/api/v1/constraints")
        async def get_constraints(request: Request):
            user = request.state.user              # AuthenticatedUser
            emergency = request.state.emergency_override  # bool

    Args:
        app: The ASGI application to wrap.
        auth_backend: Backend for OIDC token validation and mTLS
            certificate extraction.
        audit_logger: Logger for recording data exchange audit entries.
        participant_id: The unique identifier of this participant node
            (used as ``provider_id`` in audit entries).
        policy_engine: Optional policy engine for middleware-level access
            checks.  When provided, the engine verifies that the
            authenticated user's participant is registered.  Detailed
            per-request policy evaluation (contracts, purposes, assets)
            is left to individual route handlers via
            ``request.state.policy_engine``.
        skip_auth_paths: Paths that bypass authentication entirely.
            Defaults to ``{"/health", "/healthz", "/ready"}``.
    """

    def __init__(
        self,
        app: object,
        *,
        auth_backend: KeycloakAuthBackend,
        audit_logger: AuditLogger,
        participant_id: str,
        policy_engine: Optional[PolicyEngine] = None,
        skip_auth_paths: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(app)
        self._auth_backend = auth_backend
        self._audit_logger = audit_logger
        self._participant_id = participant_id
        self._policy_engine = policy_engine
        self._skip_auth_paths: frozenset[str] = (
            frozenset(skip_auth_paths)
            if skip_auth_paths is not None
            else _DEFAULT_SKIP_AUTH_PATHS
        )

    # -- public properties ---------------------------------------------------

    @property
    def participant_id(self) -> str:
        """Return the participant ID of this node."""
        return self._participant_id

    @property
    def auth_backend(self) -> KeycloakAuthBackend:
        """Return the configured authentication backend."""
        return self._auth_backend

    @property
    def audit_logger(self) -> AuditLogger:
        """Return the configured audit logger."""
        return self._audit_logger

    @property
    def policy_engine(self) -> Optional[PolicyEngine]:
        """Return the configured policy engine, or ``None``."""
        return self._policy_engine

    # -- dispatch (main entry point) -----------------------------------------

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Process the request through authentication, policy, and audit layers.

        The processing order is:

        1. **Skip-auth check** -- health endpoints bypass all layers.
        2. **Authentication** -- validate the Bearer token and extract the
           user identity.  Failures produce HTTP 401.
        3. **Policy check** -- verify the participant is registered (when a
           policy engine is configured).  Failures produce HTTP 403 and
           are audited.
        4. **Route handler** -- forward the request to the actual handler.
        5. **Audit logging** -- record the exchange with request/response
           hashes.  Failures produce HTTP 500.

        Args:
            request: The incoming HTTP request.
            call_next: Callback to invoke the next middleware or route
                handler.

        Returns:
            The HTTP response (possibly an error response from one of the
            enforcement layers).
        """
        # -- Skip auth for health endpoints ----------------------------------
        if self._should_skip_auth(request):
            return await call_next(request)

        # -- Step 1: Authentication ------------------------------------------
        try:
            user = self._authenticate_request(request)
        except AuthError as exc:
            return _json_error(
                401,
                str(exc),
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Attach identity and context to request.state for route handlers.
        request.state.user = user
        request.state.emergency_override = self._extract_emergency_override(
            request
        )
        if self._policy_engine is not None:
            request.state.policy_engine = self._policy_engine

        # -- Step 2: Policy check --------------------------------------------
        if self._policy_engine is not None:
            denial_reason = self._check_policy(user)
            if denial_reason is not None:
                return await self._handle_policy_denial(
                    request, user, denial_reason
                )

        # -- Step 3: Forward to route handler and audit ----------------------
        request_body = await request.body()

        response = await call_next(request)

        # Consume the response body for hashing (we must reconstruct the
        # response afterward since the body_iterator is single-use).
        response_body = await self._consume_response_body(response)

        action = _map_method_to_action(request.method)
        outcome = _map_status_to_outcome(response.status_code)

        # Audit logging is non-optional: failure means the request fails.
        try:
            self._record_audit(
                user=user,
                request=request,
                request_body=request_body,
                response_body=response_body,
                action=action,
                outcome=outcome,
            )
        except AuditFailureError:
            return _json_error(
                500, "Internal error: audit logging failed"
            )

        # Reconstruct the response with the consumed body.
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

    # -- private helpers: auth -----------------------------------------------

    def _should_skip_auth(self, request: Request) -> bool:
        """Return ``True`` if the request path is exempt from authentication.

        Args:
            request: The incoming HTTP request.

        Returns:
            Whether the path is in the skip-auth set.
        """
        return request.url.path in self._skip_auth_paths

    def _authenticate_request(self, request: Request) -> AuthenticatedUser:
        """Extract and validate the Bearer token from the request.

        Also attempts to extract the mTLS client certificate distinguished
        name from reverse-proxy headers and attaches it to the returned
        user identity.

        Args:
            request: The incoming HTTP request.

        Returns:
            The authenticated user identity.

        Raises:
            _MissingCredentialsError: If the Authorization header is absent
                or does not start with ``Bearer``.
            AuthError: If token validation fails (expired, invalid
                signature, missing claims, etc.).
        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.lower().startswith("bearer "):
            raise _MissingCredentialsError()

        # Extract the raw token (preserve case after the scheme prefix).
        token = auth_header[len("Bearer ") :]

        # Validate the token and extract the user identity.
        user = self._auth_backend.authenticate_token(token)

        # Attempt to enrich the user with mTLS certificate DN.
        headers_dict = dict(request.headers)
        cert_dn = KeycloakAuthBackend.extract_client_certificate_dn(
            headers_dict
        )
        if cert_dn is not None:
            user = user.model_copy(update={"certificate_dn": cert_dn})

        return user

    @staticmethod
    def _extract_emergency_override(request: Request) -> bool:
        """Extract the emergency override flag from request headers.

        The ``X-Emergency-Override`` header is checked for truthy values
        (``"true"``, ``"1"``, ``"yes"``).

        Args:
            request: The incoming HTTP request.

        Returns:
            ``True`` if the emergency override flag is set.
        """
        value = (
            request.headers.get(_EMERGENCY_OVERRIDE_HEADER, "")
            .strip()
            .lower()
        )
        return value in ("true", "1", "yes")

    # -- private helpers: policy ---------------------------------------------

    def _check_policy(self, user: AuthenticatedUser) -> Optional[str]:
        """Perform a middleware-level policy check on the authenticated user.

        Verifies that the user's participant is registered with the policy
        engine.  This is a coarse-grained check; fine-grained contract and
        purpose-based checks should be performed by individual route
        handlers.

        Args:
            user: The authenticated user identity.

        Returns:
            A denial reason string if the check fails, or ``None`` if the
            check passes.
        """
        if self._policy_engine is None:
            return None

        try:
            self._policy_engine.get_participant(user.participant_id)
        except ParticipantNotRegisteredError:
            return (
                f"Participant '{user.participant_id}' is not registered "
                f"with the policy engine"
            )
        return None

    async def _handle_policy_denial(
        self,
        request: Request,
        user: AuthenticatedUser,
        denial_reason: str,
    ) -> Response:
        """Create a 403 response for a policy denial and audit it.

        Args:
            request: The incoming HTTP request.
            user: The authenticated user whose access was denied.
            denial_reason: Human-readable reason for the denial.

        Returns:
            An HTTP 403 JSON response, or HTTP 500 if audit logging fails.
        """
        denial_response = _json_error(403, denial_reason)

        request_body = await request.body()
        response_body = denial_response.body

        try:
            self._record_audit(
                user=user,
                request=request,
                request_body=request_body,
                response_body=response_body,
                action=_map_method_to_action(request.method),
                outcome=AuditOutcome.DENIED,
            )
        except AuditFailureError:
            return _json_error(
                500, "Internal error: audit logging failed"
            )

        return denial_response

    # -- private helpers: response body consumption --------------------------

    @staticmethod
    async def _consume_response_body(response: Response) -> bytes:
        """Read the full response body from the streaming body iterator.

        The ``body_iterator`` on a ``StreamingResponse`` is single-use.
        After calling this method, the caller must reconstruct the response
        with the returned bytes.

        Args:
            response: The response whose body to consume.

        Returns:
            The complete response body as bytes.
        """
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            if isinstance(chunk, bytes):
                chunks.append(chunk)
            else:
                chunks.append(chunk.encode("utf-8"))
        return b"".join(chunks)

    # -- private helpers: audit ----------------------------------------------

    def _record_audit(
        self,
        *,
        user: AuthenticatedUser,
        request: Request,
        request_body: bytes,
        response_body: bytes,
        action: AuditAction,
        outcome: AuditOutcome,
    ) -> None:
        """Record an audit entry for the exchange.

        The request and response bodies are hashed by the audit logger.
        Purpose tag and contract ID are extracted from request headers.

        Args:
            user: The authenticated user identity.
            request: The incoming HTTP request.
            request_body: Raw bytes of the request body.
            response_body: Raw bytes of the response body.
            action: The audit action classification.
            outcome: The exchange outcome.

        Raises:
            AuditFailureError: If the audit entry cannot be recorded.
        """
        purpose_tag = request.headers.get(_PURPOSE_TAG_HEADER, "unknown")
        contract_id = request.headers.get(_CONTRACT_ID_HEADER, "unknown")

        try:
            self._audit_logger.log_exchange(
                requester_id=user.participant_id,
                provider_id=self._participant_id,
                asset_id=request.url.path,
                purpose_tag=purpose_tag,
                request_body=request_body,
                response_body=response_body,
                contract_id=contract_id,
                action=action,
                outcome=outcome,
            )
        except Exception as exc:
            logger.error(
                "Audit logging failed for %s %s (requester=%s): %s",
                request.method,
                request.url.path,
                user.participant_id,
                exc,
            )
            raise AuditFailureError(str(exc)) from exc
