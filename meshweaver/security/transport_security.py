import hashlib
import hmac
import secrets


class TransportSecurityError(Exception):
    """Base exception for transport security errors."""

class InvalidMessageError(TransportSecurityError):
    """Raised when a received message fails authentication."""

class TransportSecurity:
    """
    Provides authentication and integrity protection for UDP messages.

    Day 1 uses HMAC-SHA256.

    HMAC does NOT encrypt the message.
    It guarantees that:
        1. The message was created by someone possessing the shared key.
        2. The message was not modified in transit.

    Encryption will be added in a later security milestone using
    a vetted cryptographic implementation.
    """

    VERSION = 1
    TAG_SIZE = 32
    NONCE_SIZE = 16

    def __init__(self, key: bytes):
        if not isinstance(key, bytes):
            raise TypeError("Security key must be bytes.")

        if len(key) < 32:
            raise ValueError(
                "Security key must contain at least 32 bytes."
            )

        self.key = key

    @classmethod
    def generate_key(cls, size: int = 32) -> bytes:
        """
        Generate a cryptographically secure random key.
        """

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
        """
        Calculate HMAC-SHA256 over the nonce and payload.
        """

        return hmac.new(
            self.key,
            nonce + payload,
            hashlib.sha256,
        ).digest()

    def protect(self, payload: bytes) -> bytes:
        """
        Add authentication/integrity information to a payload.

        Packet format:

            VERSION | NONCE | PAYLOAD | HMAC

        Note:
            PAYLOAD remains readable.
            This is intentional for Day 1 because this layer
            provides authentication/integrity, not encryption.
        """

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

    def unprotect(self, packet: bytes) -> bytes:
        """
        Verify and extract the original payload.
        """

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

        tag_start = len(packet) - self.TAG_SIZE

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