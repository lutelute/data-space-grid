"""Consent manager for the Prosumer participant node.

Manages purpose-based consent records that control how prosumer data is
shared within the Federated Data Space.  No consumer data leaves the
prosumer node without an active, non-expired consent record matching the
request's purpose.

Key design principles (from spec):
  - Default is maximum restriction; only explicit consent widens access.
  - Consent can be revoked at any time, and revocation takes effect
    immediately for subsequent requests (in-flight data is not affected).
  - Consent records are HIGH_PRIVACY and only visible to the consent holder.
  - Each consent is scoped to a specific purpose and requester.

The ``ConsentManager`` keeps an in-memory registry of consent records
keyed by ``consent_id``.  For production use this store would be backed
by a persistent database, but the interface stays the same.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from src.semantic.consumer import (
    ConsentRecord,
    ConsentStatus,
    DisclosureLevel,
    PURPOSE_DISCLOSURE_MAP,
)


def _utc_now() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConsentError(Exception):
    """Base exception for consent management errors."""


class ConsentNotFoundError(ConsentError):
    """Raised when a referenced consent record does not exist."""

    def __init__(self, consent_id: str) -> None:
        self.consent_id = consent_id
        super().__init__(f"Consent record not found: '{consent_id}'")


class InvalidConsentPurposeError(ConsentError):
    """Raised when a consent purpose is not in the PURPOSE_DISCLOSURE_MAP."""

    def __init__(self, purpose: str) -> None:
        self.purpose = purpose
        allowed = ", ".join(sorted(PURPOSE_DISCLOSURE_MAP.keys()))
        super().__init__(
            f"Unknown consent purpose: '{purpose}'. "
            f"Allowed purposes: {allowed}"
        )


class ConsentAlreadyRevokedError(ConsentError):
    """Raised when attempting to revoke an already-revoked consent."""

    def __init__(self, consent_id: str) -> None:
        self.consent_id = consent_id
        super().__init__(f"Consent already revoked: '{consent_id}'")


# ---------------------------------------------------------------------------
# ConsentManager
# ---------------------------------------------------------------------------


class ConsentManager:
    """Manages purpose-based consent records for prosumer data sharing.

    The manager maintains an in-memory registry of consent records and
    provides methods to grant, revoke, check, and list consents.  Every
    data sharing request must pass a consent check before the anonymizer
    transforms and releases data.

    Usage::

        cm = ConsentManager(prosumer_id="prosumer-001")
        consent = cm.grant_consent(
            purpose="research",
            requester_id="researcher-001",
            expiry=datetime(2026, 12, 31, tzinfo=timezone.utc),
        )
        assert cm.check_consent("researcher-001", "research")
        cm.revoke_consent(consent.consent_id)
        assert not cm.check_consent("researcher-001", "research")

    Args:
        prosumer_id: Identifier of the prosumer who owns these consents.
    """

    def __init__(self, prosumer_id: str) -> None:
        self._prosumer_id = prosumer_id
        self._consents: dict[str, ConsentRecord] = {}

    @property
    def prosumer_id(self) -> str:
        """The prosumer identifier this manager belongs to."""
        return self._prosumer_id

    # -- helpers -------------------------------------------------------------

    def _get_consent(self, consent_id: str) -> ConsentRecord:
        """Retrieve a consent record or raise :class:`ConsentNotFoundError`."""
        try:
            return self._consents[consent_id]
        except KeyError:
            raise ConsentNotFoundError(consent_id) from None

    def _check_expired(self, consent: ConsentRecord) -> ConsentRecord:
        """Transition an ACTIVE consent to EXPIRED if its validity window has passed.

        Args:
            consent: The consent record to check.

        Returns:
            The consent record, potentially with updated status.
        """
        if consent.status != ConsentStatus.ACTIVE:
            return consent

        now = _utc_now()
        if now > consent.valid_until:
            consent.status = ConsentStatus.EXPIRED
            consent.updated_at = now
        return consent

    # -- public API ----------------------------------------------------------

    def grant_consent(
        self,
        purpose: str,
        requester_id: str,
        expiry: datetime,
        *,
        allowed_data_types: Optional[list[str]] = None,
        valid_from: Optional[datetime] = None,
    ) -> ConsentRecord:
        """Grant consent for a specific purpose and requester.

        The purpose must exist in :data:`PURPOSE_DISCLOSURE_MAP`; unknown
        purposes are rejected (fail-closed).  The disclosure level is
        automatically determined by the purpose.

        Args:
            purpose: Data usage purpose (must be a key in PURPOSE_DISCLOSURE_MAP).
            requester_id: Identifier of the party being granted access.
            expiry: When this consent expires (UTC).
            allowed_data_types: Data types covered by this consent.
                Defaults to ``["demand_profile"]``.
            valid_from: When the consent becomes effective.
                Defaults to now (UTC).

        Returns:
            The newly created :class:`ConsentRecord`.

        Raises:
            InvalidConsentPurposeError: If the purpose is not recognized.
        """
        if purpose not in PURPOSE_DISCLOSURE_MAP:
            raise InvalidConsentPurposeError(purpose)

        now = _utc_now()
        consent_id = f"CONSENT-{uuid.uuid4().hex[:8]}"
        effective_from = valid_from if valid_from is not None else now
        effective_data_types = (
            allowed_data_types if allowed_data_types is not None
            else ["demand_profile"]
        )

        consent = ConsentRecord(
            consent_id=consent_id,
            prosumer_id=self._prosumer_id,
            requester_id=requester_id,
            purpose=purpose,
            allowed_data_types=effective_data_types,
            disclosure_level=PURPOSE_DISCLOSURE_MAP[purpose],
            status=ConsentStatus.ACTIVE,
            granted_at=now,
            revoked_at=None,
            valid_from=effective_from,
            valid_until=expiry,
        )

        self._consents[consent_id] = consent
        return consent

    def revoke_consent(self, consent_id: str) -> ConsentRecord:
        """Revoke an active consent immediately.

        Revocation takes effect for all subsequent data requests.
        In-flight exchanges that started before revocation are not
        affected (they complete with the data already released).

        Args:
            consent_id: The identifier of the consent to revoke.

        Returns:
            The revoked :class:`ConsentRecord`.

        Raises:
            ConsentNotFoundError: If the consent does not exist.
            ConsentAlreadyRevokedError: If the consent was already revoked.
        """
        consent = self._get_consent(consent_id)

        if consent.status == ConsentStatus.REVOKED:
            raise ConsentAlreadyRevokedError(consent_id)

        now = _utc_now()
        consent.status = ConsentStatus.REVOKED
        consent.revoked_at = now
        consent.updated_at = now
        return consent

    def check_consent(self, requester_id: str, purpose: str) -> bool:
        """Check whether an active consent exists for the given requester and purpose.

        Automatically expires consents whose validity window has passed.
        A consent is considered valid when:
          1. Its status is ``ACTIVE``.
          2. Its ``requester_id`` matches.
          3. Its ``purpose`` matches.
          4. The current time falls within ``[valid_from, valid_until]``.

        Args:
            requester_id: Identifier of the party requesting data.
            purpose: The data usage purpose being requested.

        Returns:
            ``True`` if at least one matching active consent exists,
            ``False`` otherwise.
        """
        now = _utc_now()
        for consent in self._consents.values():
            # Auto-expire if past validity window
            self._check_expired(consent)

            if consent.status != ConsentStatus.ACTIVE:
                continue
            if consent.requester_id != requester_id:
                continue
            if consent.purpose != purpose:
                continue
            if now < consent.valid_from:
                continue
            return True
        return False

    def get_consent(self, consent_id: str) -> ConsentRecord:
        """Retrieve a specific consent record by ID.

        Automatically checks and updates expiration status.

        Args:
            consent_id: The unique consent identifier.

        Returns:
            The :class:`ConsentRecord`.

        Raises:
            ConsentNotFoundError: If the consent does not exist.
        """
        consent = self._get_consent(consent_id)
        self._check_expired(consent)
        return consent

    def list_active_consents(self) -> list[ConsentRecord]:
        """Return all currently active (non-revoked, non-expired) consents.

        Automatically expires consents whose validity window has passed
        before filtering.

        Returns:
            List of :class:`ConsentRecord` instances with ``ACTIVE`` status.
        """
        active: list[ConsentRecord] = []
        for consent in self._consents.values():
            self._check_expired(consent)
            if consent.status == ConsentStatus.ACTIVE:
                active.append(consent)
        return active

    def list_all_consents(self) -> list[ConsentRecord]:
        """Return all consent records regardless of status.

        Returns:
            List of all :class:`ConsentRecord` instances.
        """
        for consent in self._consents.values():
            self._check_expired(consent)
        return list(self._consents.values())

    def get_disclosure_level(
        self, requester_id: str, purpose: str
    ) -> Optional[DisclosureLevel]:
        """Determine the disclosure level for a requester and purpose.

        Returns the disclosure level from :data:`PURPOSE_DISCLOSURE_MAP`
        only if an active consent exists for the given requester and purpose.
        Otherwise returns ``None`` (access denied).

        Args:
            requester_id: Identifier of the requesting party.
            purpose: The data usage purpose.

        Returns:
            The :class:`DisclosureLevel` if consent is active, or ``None``
            if access should be denied.
        """
        if not self.check_consent(requester_id, purpose):
            return None

        return PURPOSE_DISCLOSURE_MAP.get(purpose)
