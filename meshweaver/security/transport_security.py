"""
MeshWeaver transport security.

Provides:

- HMAC-SHA256 authentication
- Message integrity
- Peer authentication helpers

Does NOT provide encryption.
"""

import hashlib
import hmac
import secrets


class TransportSecurityError(Exception):
    """Base exception for transport security errors."""


class InvalidMessageError(TransportSecurityError):
    """Raised when a received message fails authentication."""


class PeerAuthenticationError(TransportSecurityError):
    """Raised when peer authentication fails."""


class TransportSecurity:

    VERSION = 1

    TAG_SIZE = 32
    NONCE_SIZE = 16

    def __init__(self, key: bytes):

        if not isinstance(key, bytes):
            raise TypeError(
                "Security key must be bytes."
            )

        if len(key) < 32:
            raise ValueError(
                "Security key must contain at least 32 bytes."
            )

        self.key = key

    @classmethod
    def generate_key(cls, size: int = 32) -> bytes:

        if size < 32:
            raise ValueError(
                "Security key must be at least 32 bytes."
            )

        return secrets.token_bytes(size)

    def _calculate_tag(
        self,
        nonce: bytes,
        payload: bytes,
    ) -> bytes:

        return hmac.new(
            self.key,
            nonce + payload,
            hashlib.sha256,
        ).digest()

    def protect(
        self,
        payload: bytes,
    ) -> bytes:

        if not isinstance(payload, bytes):
            raise TypeError(
                "Payload must be bytes."
            )

        nonce = secrets.token_bytes(
            self.NONCE_SIZE
        )

        tag = self._calculate_tag(
            nonce,
            payload,
        )

        return (
            bytes([self.VERSION])
            + nonce
            + payload
            + tag
        )

    def unprotect(
        self,
        packet: bytes,
    ) -> bytes:

        if not isinstance(packet, bytes):
            raise TypeError(
                "Packet must be bytes."
            )

        minimum_size = (
            1
            + self.NONCE_SIZE
            + self.TAG_SIZE
        )

        if len(packet) < minimum_size:

            raise InvalidMessageError(
                "Secure packet is too short."
            )

        version = packet[0]

        if version != self.VERSION:

            raise InvalidMessageError(
                f"Unsupported security version: {version}"
            )

        nonce_start = 1

        nonce_end = (
            nonce_start
            + self.NONCE_SIZE
        )

        nonce = packet[
            nonce_start:nonce_end
        ]

        tag_start = (
            len(packet)
            - self.TAG_SIZE
        )

        payload = packet[
            nonce_end:tag_start
        ]

        received_tag = packet[
            tag_start:
        ]

        expected_tag = self._calculate_tag(
            nonce,
            payload,
        )

        if not hmac.compare_digest(
            received_tag,
            expected_tag,
        ):

            raise InvalidMessageError(
                "Message authentication failed."
            )

        return payload

    # ---------------------------------------------------------
    # PEER AUTHENTICATION
    # ---------------------------------------------------------

    def create_peer_challenge(self) -> bytes:
        """
        Create a random challenge for peer authentication.
        """

        return secrets.token_bytes(
            self.NONCE_SIZE
        )

    def create_peer_response(
        self,
        challenge: bytes,
        peer_id: str,
    ) -> bytes:
        """
        Create an HMAC response to a peer challenge.

        The peer ID is included so the authentication response
        is bound to the expected peer identity.
        """

        if not isinstance(
            challenge,
            bytes,
        ):
            raise TypeError(
                "Challenge must be bytes."
            )

        if not isinstance(
            peer_id,
            str,
        ):
            raise TypeError(
                "Peer ID must be a string."
            )

        message = (
            b"PEER_AUTH:"
            + peer_id.encode("utf-8")
            + b":"
            + challenge
        )

        return hmac.new(
            self.key,
            message,
            hashlib.sha256,
        ).digest()

    def verify_peer_response(
        self,
        challenge: bytes,
        peer_id: str,
        response: bytes,
    ) -> bool:
        """
        Verify a peer authentication response.
        """

        expected = self.create_peer_response(
            challenge,
            peer_id,
        )

        if not hmac.compare_digest(
            response,
            expected,
        ):

            raise PeerAuthenticationError(
                f"Peer authentication failed: {peer_id}"
            )

        return True