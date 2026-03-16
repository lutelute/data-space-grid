"""Integration tests for OIDC + mTLS authentication flows.

Verifies the authentication enforcement in the connector middleware:
  1. Valid Bearer token grants access to protected endpoints.
  2. Missing or absent Bearer token is rejected with HTTP 401.
  3. Expired JWT token is rejected with HTTP 401.
  4. Unregistered participant (wrong role) is rejected with HTTP 403
     when a policy engine is configured.
  5. mTLS client certificate DN is extracted from request headers
     and attached to the user identity.
  6. Request with untrusted (or missing) client certificate is rejected
     by a route-level trust check simulating real mTLS enforcement.

These tests create FastAPI applications with customised mock authentication
backends to simulate various token and certificate scenarios without
requiring a real Keycloak instance or TLS infrastructure.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.routing import APIRouter

from src.catalog.routes import create_router as create_catalog_router
from src.catalog.store import CatalogStore
from src.connector.audit import AuditLogger
from src.connector.auth import (
    AuthenticatedUser,
    KeycloakAuthBackend,
    TokenExpiredError,
    TokenValidationError,
)
from src.connector.middleware import ConnectorMiddleware
from src.connector.policy import PolicyEngine
from src.participants.dso.routes import create_router as create_dso_router
from src.participants.dso.store import DSOStore
from tests.conftest import (
    MockEventBus,
    MockKeycloakAuthBackend,
    make_aggregator_user,
    make_dso_user,
    make_participant,
)


# ---------------------------------------------------------------------------
# Custom auth backend variants for testing failure scenarios
# ---------------------------------------------------------------------------


class ExpiredTokenAuthBackend(KeycloakAuthBackend):
    """Auth backend that always simulates an expired JWT token.

    Every call to :meth:`authenticate_token` raises a
    :class:`TokenExpiredError`, simulating the case where the client
    presents a token whose ``exp`` claim is in the past.
    """

    def __init__(self) -> None:
        super().__init__(
            server_url="http://localhost:8080",
            realm="dataspace-test",
        )

    def authenticate_token(self, token: str) -> AuthenticatedUser:
        """Always raise :class:`TokenExpiredError`."""
        raise TokenExpiredError()


class InvalidTokenAuthBackend(KeycloakAuthBackend):
    """Auth backend that always simulates an invalid JWT token.

    Every call to :meth:`authenticate_token` raises a
    :class:`TokenValidationError` with a message indicating the
    token signature could not be verified.
    """

    def __init__(self) -> None:
        super().__init__(
            server_url="http://localhost:8080",
            realm="dataspace-test",
        )

    def authenticate_token(self, token: str) -> AuthenticatedUser:
        """Always raise :class:`TokenValidationError`."""
        raise TokenValidationError("invalid token signature")


# ---------------------------------------------------------------------------
# Helpers: create a trusted-cert-checking route for mTLS simulation
# ---------------------------------------------------------------------------

_TRUSTED_CERTIFICATE_DNS: list[str] = [
    "CN=dso-node,O=GridCo,C=NL",
    "CN=aggregator-node,O=FlexEnergy,C=NL",
    "CN=prosumer-node,O=TechCampus,C=NL",
]


def _create_mtls_checking_router() -> APIRouter:
    """Create a router with a protected endpoint that checks certificate DN.

    The endpoint simulates a real mTLS trust check by verifying that the
    client certificate DN (extracted by the middleware from reverse-proxy
    headers) is in the list of trusted DNs.
    """
    router = APIRouter()

    @router.get("/health")
    async def health():
        return {"status": "healthy", "service": "mtls-test"}

    @router.get("/api/v1/protected")
    async def protected(request: Request):
        user: AuthenticatedUser = request.state.user
        if user.certificate_dn not in _TRUSTED_CERTIFICATE_DNS:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Untrusted client certificate: "
                    f"DN '{user.certificate_dn}' is not in the trust store"
                ),
            )
        return {
            "message": "Access granted",
            "certificate_dn": user.certificate_dn,
            "participant_id": user.participant_id,
        }

    return router


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def valid_auth_catalog_client(tmp_path: Path):
    """Catalog client with a valid mock auth backend (DSO user)."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-valid-catalog-audit.jsonl")
    )
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog (Valid Auth)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-catalog-auth",
    ) as client:
        yield client


@pytest.fixture()
async def expired_token_catalog_client(tmp_path: Path):
    """Catalog client with an auth backend that always returns expired tokens."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-expired-catalog-audit.jsonl")
    )
    backend = ExpiredTokenAuthBackend()
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog (Expired Token)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-catalog-expired",
    ) as client:
        yield client


@pytest.fixture()
async def invalid_token_catalog_client(tmp_path: Path):
    """Catalog client with an auth backend that always rejects tokens."""
    store = CatalogStore(database_url="sqlite:///:memory:")
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-invalid-catalog-audit.jsonl")
    )
    backend = InvalidTokenAuthBackend()
    router = create_catalog_router(store=store)
    app = FastAPI(title="Test Catalog (Invalid Token)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="catalog-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-catalog-invalid",
    ) as client:
        yield client


@pytest.fixture()
async def policy_enforced_dso_client(tmp_path: Path):
    """DSO client with a policy engine that only registers the DSO participant.

    Requests from participants not registered with the policy engine
    (e.g. an unknown aggregator) will be rejected with HTTP 403.
    """
    store = DSOStore(database_url="sqlite:///:memory:")
    store.seed()
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-policy-dso-audit.jsonl")
    )
    bus = MockEventBus()

    # Auth backend returns an aggregator user (not registered with engine).
    backend = MockKeycloakAuthBackend(mock_user=make_aggregator_user())

    # Policy engine only has the DSO participant registered.
    engine = PolicyEngine()
    engine.register_participant(
        make_participant(
            pid="dso-001",
            roles=["dso_operator"],
            organization="GridCo",
        )
    )

    router = create_dso_router(
        store=store, audit_logger=audit_logger, event_bus=bus
    )
    app = FastAPI(title="Test DSO (Policy Enforced)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="dso-001",
        policy_engine=engine,
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-dso-policy",
    ) as client:
        yield client


@pytest.fixture()
async def policy_enforced_dso_client_valid(tmp_path: Path):
    """DSO client with a policy engine where the DSO user IS registered.

    Requests from the DSO participant are allowed because the participant
    is registered with the policy engine.
    """
    store = DSOStore(database_url="sqlite:///:memory:")
    store.seed()
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-policy-valid-dso-audit.jsonl")
    )
    bus = MockEventBus()

    # Auth backend returns a DSO user (registered with engine).
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())

    # Policy engine has the DSO participant registered.
    engine = PolicyEngine()
    engine.register_participant(
        make_participant(
            pid="dso-001",
            roles=["dso_operator"],
            organization="GridCo",
        )
    )

    router = create_dso_router(
        store=store, audit_logger=audit_logger, event_bus=bus
    )
    app = FastAPI(title="Test DSO (Policy Valid)", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="dso-001",
        policy_engine=engine,
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-dso-policy-valid",
    ) as client:
        yield client


@pytest.fixture()
async def mtls_checking_client(tmp_path: Path):
    """Client for a service with a route that checks client certificate DN.

    Simulates mTLS trust enforcement at the application level. Requests
    must include a trusted certificate DN in the ``X-SSL-Client-DN`` header
    to access the protected endpoint.
    """
    audit_logger = AuditLogger(
        log_path=str(tmp_path / "auth-mtls-audit.jsonl")
    )
    # Backend returns a DSO user with certificate_dn set.
    backend = MockKeycloakAuthBackend(mock_user=make_dso_user())
    router = _create_mtls_checking_router()
    app = FastAPI(title="Test mTLS Enforcement", version="0.1.0-test")
    app.add_middleware(
        ConnectorMiddleware,
        auth_backend=backend,
        audit_logger=audit_logger,
        participant_id="mtls-test-001",
    )
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test-mtls",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

DSO_HEADERS = {"Authorization": "Bearer test-dso-token"}
AGG_HEADERS = {"Authorization": "Bearer test-agg-token"}


# ---------------------------------------------------------------------------
# Test: Valid token grants access
# ---------------------------------------------------------------------------


class TestValidTokenAccess:
    """Integration tests verifying that valid tokens grant access."""

    @pytest.mark.asyncio
    async def test_valid_token_grants_access_to_catalog_health(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Health endpoint is accessible without authentication."""
        client = valid_auth_catalog_client
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_valid_token_grants_access_to_catalog_assets(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Valid token allows listing catalog assets."""
        client = valid_auth_catalog_client
        response = await client.get(
            "/api/v1/assets", headers=DSO_HEADERS
        )
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    @pytest.mark.asyncio
    async def test_valid_token_grants_access_to_register_asset(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Valid token allows registering an asset in the catalog."""
        client = valid_auth_catalog_client
        payload = {
            "provider_id": "dso-001",
            "name": "Auth Test Asset",
            "description": "Asset for auth flow testing",
            "data_type": "feeder_constraint",
            "sensitivity": "medium",
            "endpoint": "https://dso.local/api/v1/constraints",
            "update_frequency": "5m",
            "resolution": "per_feeder",
            "anonymized": False,
            "personal_data": False,
            "policy_metadata": {
                "allowed_purposes": "congestion_management",
            },
        }
        response = await client.post(
            "/api/v1/assets", json=payload, headers=DSO_HEADERS
        )
        assert response.status_code == 201
        assert "id" in response.json()

    @pytest.mark.asyncio
    async def test_valid_token_with_policy_engine_grants_registered_participant(
        self, policy_enforced_dso_client_valid: httpx.AsyncClient
    ) -> None:
        """A registered participant passes the policy engine check."""
        client = policy_enforced_dso_client_valid
        response = await client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Test: Missing or malformed token rejected
# ---------------------------------------------------------------------------


class TestMissingTokenRejection:
    """Integration tests verifying that missing or malformed tokens are rejected."""

    @pytest.mark.asyncio
    async def test_missing_authorization_header_returns_401(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Request without Authorization header should return 401."""
        client = valid_auth_catalog_client
        response = await client.get("/api/v1/assets")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data

    @pytest.mark.asyncio
    async def test_empty_authorization_header_returns_401(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Request with empty Authorization header should return 401."""
        client = valid_auth_catalog_client
        response = await client.get(
            "/api/v1/assets", headers={"Authorization": ""}
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_malformed_authorization_header_returns_401(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Request with non-Bearer Authorization should return 401."""
        client = valid_auth_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_401_response_includes_www_authenticate_header(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """401 response should include the WWW-Authenticate: Bearer header."""
        client = valid_auth_catalog_client
        response = await client.get("/api/v1/assets")
        assert response.status_code == 401
        assert "www-authenticate" in response.headers
        assert response.headers["www-authenticate"] == "Bearer"

    @pytest.mark.asyncio
    async def test_401_response_body_is_json_with_detail(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """401 response body should be JSON with a 'detail' key."""
        client = valid_auth_catalog_client
        response = await client.get("/api/v1/assets")
        assert response.status_code == 401
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], str)
        assert len(data["detail"]) > 0


# ---------------------------------------------------------------------------
# Test: Expired token rejected
# ---------------------------------------------------------------------------


class TestExpiredTokenRejection:
    """Integration tests verifying that expired JWT tokens are rejected."""

    @pytest.mark.asyncio
    async def test_expired_token_returns_401(
        self, expired_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Request with an expired token should return 401."""
        client = expired_token_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer expired-jwt-token-xyz"},
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_expired_token_error_detail_mentions_expiry(
        self, expired_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Expired token 401 should include an error detail about expiry."""
        client = expired_token_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer expired-jwt-token-xyz"},
        )
        assert response.status_code == 401
        detail = response.json()["detail"]
        assert "expired" in detail.lower()

    @pytest.mark.asyncio
    async def test_expired_token_includes_www_authenticate_header(
        self, expired_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Expired token 401 should include WWW-Authenticate header."""
        client = expired_token_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer expired-jwt-token-xyz"},
        )
        assert response.status_code == 401
        assert "www-authenticate" in response.headers
        assert response.headers["www-authenticate"] == "Bearer"

    @pytest.mark.asyncio
    async def test_expired_token_health_endpoint_still_accessible(
        self, expired_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Health endpoint should be accessible even with expired token backend."""
        client = expired_token_catalog_client
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


# ---------------------------------------------------------------------------
# Test: Invalid token rejected
# ---------------------------------------------------------------------------


class TestInvalidTokenRejection:
    """Integration tests verifying that invalid JWT tokens are rejected."""

    @pytest.mark.asyncio
    async def test_invalid_token_returns_401(
        self, invalid_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Request with an invalid (bad signature) token should return 401."""
        client = invalid_token_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer bad-signature-token"},
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_invalid_token_error_detail_describes_failure(
        self, invalid_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Invalid token 401 should include a descriptive error detail."""
        client = invalid_token_catalog_client
        response = await client.get(
            "/api/v1/assets",
            headers={"Authorization": "Bearer bad-signature-token"},
        )
        assert response.status_code == 401
        detail = response.json()["detail"]
        assert "invalid" in detail.lower() or "validation" in detail.lower()


# ---------------------------------------------------------------------------
# Test: Wrong role / unregistered participant rejected
# ---------------------------------------------------------------------------


class TestWrongRoleRejection:
    """Integration tests verifying that participants with wrong roles are rejected.

    When a policy engine is configured, the middleware verifies that the
    authenticated participant is registered.  An unregistered participant
    (representing someone with the wrong role for this service) receives
    HTTP 403.
    """

    @pytest.mark.asyncio
    async def test_unregistered_participant_returns_403(
        self, policy_enforced_dso_client: httpx.AsyncClient
    ) -> None:
        """Request from an unregistered participant should return 403.

        The policy engine only has DSO registered. The auth backend returns
        an aggregator user, so the middleware rejects with 403.
        """
        client = policy_enforced_dso_client
        response = await client.get(
            "/api/v1/constraints", headers=AGG_HEADERS
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_unregistered_participant_error_detail(
        self, policy_enforced_dso_client: httpx.AsyncClient
    ) -> None:
        """403 response should explain which participant was not registered."""
        client = policy_enforced_dso_client
        response = await client.get(
            "/api/v1/constraints", headers=AGG_HEADERS
        )
        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "aggregator-001" in detail
        assert "not registered" in detail.lower()

    @pytest.mark.asyncio
    async def test_registered_participant_is_allowed(
        self, policy_enforced_dso_client_valid: httpx.AsyncClient
    ) -> None:
        """A registered participant should pass the policy engine check."""
        client = policy_enforced_dso_client_valid
        response = await client.get(
            "/api/v1/constraints", headers=DSO_HEADERS
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_unregistered_participant_health_still_accessible(
        self, policy_enforced_dso_client: httpx.AsyncClient
    ) -> None:
        """Health endpoint should bypass policy engine checks."""
        client = policy_enforced_dso_client
        response = await client.get("/health")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Test: mTLS client certificate verification
# ---------------------------------------------------------------------------


class TestMTLSCertificateValidation:
    """Integration tests for mTLS client certificate DN extraction and trust checks.

    The connector middleware extracts client certificate DNs from
    reverse-proxy headers (e.g. ``X-SSL-Client-DN``).  A route that
    checks the extracted DN simulates the trust enforcement that a real
    mTLS handshake provides.
    """

    @pytest.mark.asyncio
    async def test_trusted_cert_dn_grants_access(
        self, mtls_checking_client: httpx.AsyncClient
    ) -> None:
        """Request with a trusted certificate DN should be accepted."""
        client = mtls_checking_client
        headers = {
            "Authorization": "Bearer test-dso-token",
            "X-SSL-Client-DN": "CN=dso-node,O=GridCo,C=NL",
        }
        response = await client.get("/api/v1/protected", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Access granted"
        assert data["certificate_dn"] == "CN=dso-node,O=GridCo,C=NL"

    @pytest.mark.asyncio
    async def test_untrusted_cert_dn_returns_403(
        self, mtls_checking_client: httpx.AsyncClient
    ) -> None:
        """Request with an untrusted certificate DN should be rejected."""
        client = mtls_checking_client
        headers = {
            "Authorization": "Bearer test-dso-token",
            "X-SSL-Client-DN": "CN=evil-node,O=MaliciousOrg,C=XX",
        }
        response = await client.get("/api/v1/protected", headers=headers)
        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "untrusted" in detail.lower()
        assert "CN=evil-node" in detail

    @pytest.mark.asyncio
    async def test_missing_cert_header_uses_mock_default(
        self, mtls_checking_client: httpx.AsyncClient
    ) -> None:
        """Without a cert DN header, the mock backend's default DN is used.

        The mock DSO user has ``certificate_dn="CN=dso-node,O=GridCo,C=NL"``
        which is in the trust store, so access is granted.
        """
        client = mtls_checking_client
        headers = {"Authorization": "Bearer test-dso-token"}
        response = await client.get("/api/v1/protected", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["certificate_dn"] == "CN=dso-node,O=GridCo,C=NL"

    @pytest.mark.asyncio
    async def test_envoy_style_cert_header_extracted(
        self, mtls_checking_client: httpx.AsyncClient
    ) -> None:
        """Envoy-style X-Forwarded-Client-Cert header is parsed correctly."""
        client = mtls_checking_client
        headers = {
            "Authorization": "Bearer test-dso-token",
            "X-Forwarded-Client-Cert": (
                'Hash=abc123;Subject="CN=aggregator-node,O=FlexEnergy,C=NL"'
            ),
        }
        response = await client.get("/api/v1/protected", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["certificate_dn"] == "CN=aggregator-node,O=FlexEnergy,C=NL"

    @pytest.mark.asyncio
    async def test_x_client_dn_header_extracted(
        self, mtls_checking_client: httpx.AsyncClient
    ) -> None:
        """X-Client-DN header is supported for certificate DN extraction."""
        client = mtls_checking_client
        headers = {
            "Authorization": "Bearer test-dso-token",
            "X-Client-DN": "CN=prosumer-node,O=TechCampus,C=NL",
        }
        response = await client.get("/api/v1/protected", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["certificate_dn"] == "CN=prosumer-node,O=TechCampus,C=NL"


# ---------------------------------------------------------------------------
# Test: Combined auth flow scenarios
# ---------------------------------------------------------------------------


class TestCombinedAuthScenarios:
    """Integration tests for combined authentication scenarios."""

    @pytest.mark.asyncio
    async def test_auth_failure_before_policy_check(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """Auth failure (missing token) should return 401, not 403.

        The authentication layer is checked before the policy layer,
        so a missing token results in 401 regardless of policy config.
        """
        client = valid_auth_catalog_client
        response = await client.get("/api/v1/assets")
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_health_endpoint_bypasses_all_auth(
        self, expired_token_catalog_client: httpx.AsyncClient
    ) -> None:
        """Health endpoints should bypass all authentication layers.

        Even with a backend that always rejects tokens, the /health
        endpoint should be accessible.
        """
        client = expired_token_catalog_client
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_different_protected_endpoints_all_require_auth(
        self, valid_auth_catalog_client: httpx.AsyncClient
    ) -> None:
        """All non-health endpoints should require authentication."""
        client = valid_auth_catalog_client

        # All of these should require auth.
        endpoints = [
            ("GET", "/api/v1/assets"),
            ("POST", "/api/v1/assets"),
            ("GET", "/api/v1/assets/some-id"),
        ]
        for method, path in endpoints:
            if method == "GET":
                response = await client.get(path)
            else:
                response = await client.post(path, json={})
            assert response.status_code == 401, (
                f"{method} {path} should require auth but returned "
                f"{response.status_code}"
            )
