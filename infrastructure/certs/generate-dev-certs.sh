#!/usr/bin/env bash
# generate-dev-certs.sh
#
# Generate development certificates for mTLS communication between federated
# data space participants. Uses Python's cryptography package to create:
#   - CA certificate and key (self-signed root)
#   - DSO participant certificate and key
#   - Aggregator participant certificate and key
#   - Prosumer participant certificate and key
#   - Catalog service certificate and key
#
# Usage:
#   bash infrastructure/certs/generate-dev-certs.sh
#
# Output directory: infrastructure/certs/
#
# WARNING: These certificates are for DEVELOPMENT ONLY. Do not use in production.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}"

python3 - "${CERTS_DIR}" <<'PYTHON_SCRIPT'
"""Generate development CA and participant certificates for mTLS."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

CERTS_DIR = Path(sys.argv[1])
CERTS_DIR.mkdir(parents=True, exist_ok=True)

# Certificate validity: 365 days from now (dev only)
NOT_BEFORE = datetime.now(timezone.utc)
NOT_AFTER = NOT_BEFORE + timedelta(days=365)

# RSA key size for dev certificates
KEY_SIZE = 2048


def generate_key() -> rsa.RSAPrivateKey:
    """Generate an RSA private key."""
    return rsa.generate_private_key(public_exponent=65537, key_size=KEY_SIZE)


def write_key(key: rsa.RSAPrivateKey, path: Path) -> None:
    """Write a private key to PEM file."""
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def write_cert(cert: x509.Certificate, path: Path) -> None:
    """Write a certificate to PEM file."""
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def build_subject(cn: str, org: str = "Federated Data Space") -> x509.Name:
    """Build an X.509 subject name."""
    return x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "JP"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Tokyo"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, org),
            x509.NameAttribute(NameOID.COMMON_NAME, cn),
        ]
    )


# ── CA Certificate (self-signed root) ──────────────────────────────────────

ca_key = generate_key()
ca_subject = build_subject("Dataspace Dev CA")

ca_cert = (
    x509.CertificateBuilder()
    .subject_name(ca_subject)
    .issuer_name(ca_subject)
    .public_key(ca_key.public_key())
    .serial_number(x509.random_serial_number())
    .not_valid_before(NOT_BEFORE)
    .not_valid_after(NOT_AFTER)
    .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
    .add_extension(
        x509.KeyUsage(
            digital_signature=True,
            key_cert_sign=True,
            crl_sign=True,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            encipher_only=False,
            decipher_only=False,
        ),
        critical=True,
    )
    .add_extension(
        x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
        critical=False,
    )
    .sign(ca_key, hashes.SHA256())
)

write_key(ca_key, CERTS_DIR / "ca.key")
write_cert(ca_cert, CERTS_DIR / "ca.crt")

# ── Participant certificates ───────────────────────────────────────────────

PARTICIPANTS = [
    {
        "name": "dso",
        "cn": "DSO Participant Node",
        "san_dns": ["localhost", "dso-node"],
        "san_ip": ["127.0.0.1"],
    },
    {
        "name": "aggregator",
        "cn": "Aggregator Participant Node",
        "san_dns": ["localhost", "aggregator-node"],
        "san_ip": ["127.0.0.1"],
    },
    {
        "name": "prosumer",
        "cn": "Prosumer Participant Node",
        "san_dns": ["localhost", "prosumer-node"],
        "san_ip": ["127.0.0.1"],
    },
    {
        "name": "catalog",
        "cn": "Federated Catalog Service",
        "san_dns": ["localhost", "catalog-service"],
        "san_ip": ["127.0.0.1"],
    },
]

for participant in PARTICIPANTS:
    key = generate_key()
    subject = build_subject(participant["cn"])

    san_names: list[x509.GeneralName] = [
        x509.DNSName(dns) for dns in participant["san_dns"]
    ]
    for ip_str in participant["san_ip"]:
        import ipaddress
        san_names.append(x509.IPAddress(ipaddress.IPv4Address(ip_str)))

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(NOT_BEFORE)
        .not_valid_after(NOT_AFTER)
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.SubjectAlternativeName(san_names),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    write_key(key, CERTS_DIR / f"{participant['name']}.key")
    write_cert(cert, CERTS_DIR / f"{participant['name']}.crt")

    print(f"  Generated: {participant['name']}.key, {participant['name']}.crt")

print(f"\nAll certificates written to: {CERTS_DIR}")
print("CA certificate: ca.crt / ca.key")
print("\nWARNING: These certificates are for DEVELOPMENT ONLY.")
PYTHON_SCRIPT

echo ""
echo "Dev certificate generation complete."
echo "Files created in: ${CERTS_DIR}"
echo ""
echo "Certificates generated:"
echo "  ca.crt / ca.key           - Self-signed CA"
echo "  dso.crt / dso.key         - DSO participant"
echo "  aggregator.crt / aggregator.key - Aggregator participant"
echo "  prosumer.crt / prosumer.key     - Prosumer participant"
echo "  catalog.crt / catalog.key       - Catalog service"
