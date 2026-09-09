"""
MeshWeaver transport security.

Provides:

- HMAC-SHA256 authentication
- Message integrity
- Replay attack protection
- Packet validation
- Peer authentication helpers

Does NOT provide encryption.
"""

import hashlib
import hmac
import secrets


class TransportSecurityError(Exception):
    """Base exception for transport security."""


class InvalidMessageError(TransportSecurityError):
    """Raised when a received message is invalid."""


class ReplayAttackError(InvalidMessageError):
    """Raised when a packet has already been received."""


class PacketTooLargeError(InvalidMessageError):
    """Raised when a packet exceeds the allowed size."""


class PeerAuthenticationError(
    TransportSecurityError
):
    """Raised when peer authentication fails."""


class TransportSecurity:

    VERSION = 1

    TAG_SIZE = 32

    NONCE_SIZE = 16

    # Maximum application payload.
    MAX_PAYLOAD_SIZE = 64 * 1024

    # Maximum number of remembered nonces.
    MAX_SEEN_NONCES = 10_000

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

        self.seen_nonces = set()

    # =========================================================
    # Key generation
    # =========================================================

    @classmethod
    def generate_key(
        cls,
        size: int = 32,
    ) -> bytes:

        if size < 32:
            raise ValueError(
                "Security key must be at least 32 bytes."
            )

        return secrets.token_bytes(size)

    # =========================================================
    # HMAC
    # =========================================================

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

    # =========================================================
    # Protect outgoing packet
    # =========================================================

    def protect(
        self,
        payload: bytes,
    ) -> bytes:

        if not isinstance(
            payload,
            bytes,
        ):
            raise TypeError(
                "Payload must be bytes."
            )

        if len(payload) > self.MAX_PAYLOAD_SIZE:
            raise PacketTooLargeError(
                "Payload exceeds maximum allowed size."
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

    # =========================================================
    # Validate incoming packet
    # =========================================================

    def unprotect(
        self,
        packet: bytes,
    ) -> bytes:

        if not isinstance(
            packet,
            bytes,
        ):
            raise TypeError(
                "Packet must be bytes."
            )

        # -----------------------------------------------------
        # Basic packet size
        # -----------------------------------------------------

        minimum_size = (
            1
            + self.NONCE_SIZE
            + self.TAG_SIZE
        )

        if len(packet) < minimum_size:
            raise InvalidMessageError(
                "Secure packet is too short."
            )

        # -----------------------------------------------------
        # Maximum packet size
        # -----------------------------------------------------

        maximum_size = (
            1
            + self.NONCE_SIZE
            + self.MAX_PAYLOAD_SIZE
            + self.TAG_SIZE
        )

        if len(packet) > maximum_size:

            raise PacketTooLargeError(
                "Secure packet exceeds "
                "maximum allowed size."
            )

        # -----------------------------------------------------
        # Version
        # -----------------------------------------------------

        version = packet[0]

        if version != self.VERSION:

            raise InvalidMessageError(
                f"Unsupported security version: "
                f"{version}"
            )

        # -----------------------------------------------------
        # Nonce
        # -----------------------------------------------------

        nonce_start = 1

        nonce_end = (
            nonce_start
            + self.NONCE_SIZE
        )

        nonce = packet[
            nonce_start:nonce_end
        ]

        if len(nonce) != self.NONCE_SIZE:

            raise InvalidMessageError(
                "Invalid nonce length."
            )

        # -----------------------------------------------------
        # Payload
        # -----------------------------------------------------

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

        if len(received_tag) != self.TAG_SIZE:

            raise InvalidMessageError(
                "Invalid authentication tag length."
            )

        # -----------------------------------------------------
        # Payload size
        # -----------------------------------------------------

        if len(payload) > self.MAX_PAYLOAD_SIZE:

            raise PacketTooLargeError(
                "Payload exceeds maximum allowed size."
            )

        # -----------------------------------------------------
        # HMAC verification
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # Replay protection
        # -----------------------------------------------------

        if nonce in self.seen_nonces:

            raise ReplayAttackError(
                "Replay attack detected: "
                "nonce has already been used."
            )

        self.seen_nonces.add(nonce)

        # -----------------------------------------------------
        # Prevent unlimited memory growth
        # -----------------------------------------------------

        if (
            len(self.seen_nonces)
            > self.MAX_SEEN_NONCES
        ):

            self.seen_nonces.clear()

            self.seen_nonces.add(nonce)

        return payload

    # =========================================================
    # Peer authentication
    # =========================================================

    def create_peer_challenge(
        self,
    ) -> bytes:

        return secrets.token_bytes(
            self.NONCE_SIZE
        )

    def create_peer_response(
        self,
        challenge: bytes,
        peer_id: str,
    ) -> bytes:

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

        expected = self.create_peer_response(
            challenge,
            peer_id,
        )

        if not hmac.compare_digest(
            response,
            expected,
        ):

            raise PeerAuthenticationError(
                f"Peer authentication failed: "
                f"{peer_id}"
            )

        return True