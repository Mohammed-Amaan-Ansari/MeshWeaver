import hashlib
import hmac
import secrets


class TransportSecurityError(Exception):
    pass


class InvalidMessageError(TransportSecurityError):
    pass


class ReplayAttackError(InvalidMessageError):
    pass


class PacketTooLargeError(InvalidMessageError):
    pass


class PeerAuthenticationError(TransportSecurityError):
    pass


class TransportSecurity:
    VERSION = 1
    TAG_SIZE = 32
    NONCE_SIZE = 16
    MAX_PAYLOAD_SIZE = 64 * 1024
    MAX_SEEN_NONCES = 10_000

    def __init__(self, key: bytes):
        if not isinstance(key, bytes):
            raise TypeError("Security key must be bytes")
        if len(key) < 32:
            raise ValueError("Security key must be at least 32 bytes")
        self.key = key
        self.seen_nonces = set()

    @classmethod
    def generate_key(cls, size=32):
        if size < 32:
            raise ValueError("Security key must be at least 32 bytes")
        return secrets.token_bytes(size)

    def _calculate_tag(self, nonce, payload):
        return hmac.new(
            self.key,
            nonce + payload,
            hashlib.sha256,
        ).digest()

    def protect(self, payload):
        if not isinstance(payload, bytes):
            raise TypeError("Payload must be bytes")
        if len(payload) > self.MAX_PAYLOAD_SIZE:
            raise PacketTooLargeError("Payload exceeds maximum size")

        nonce = secrets.token_bytes(self.NONCE_SIZE)
        tag = self._calculate_tag(nonce, payload)

        return (
            bytes([self.VERSION])
            + nonce
            + payload
            + tag
        )

    def unprotect(self, packet):
        if not isinstance(packet, bytes):
            raise InvalidMessageError("Packet must be bytes")

        minimum_size = 1 + self.NONCE_SIZE + self.TAG_SIZE
        if len(packet) < minimum_size:
            raise InvalidMessageError("Secure packet is too short.")

        maximum_size = (
            1
            + self.NONCE_SIZE
            + self.MAX_PAYLOAD_SIZE
            + self.TAG_SIZE
        )
        if len(packet) > maximum_size:
            raise PacketTooLargeError("Secure packet is too large.")

        version = packet[0]
        if version != self.VERSION:
            raise InvalidMessageError(
                f"Unsupported packet version: {version}"
            )

        nonce_start = 1
        nonce_end = nonce_start + self.NONCE_SIZE
        nonce = packet[nonce_start:nonce_end]

        tag_start = len(packet) - self.TAG_SIZE
        payload = packet[nonce_end:tag_start]
        received_tag = packet[tag_start:]

        expected_tag = self._calculate_tag(
            nonce,
            payload,
        )

        if not hmac.compare_digest(
            received_tag,
            expected_tag,
        ):
            raise InvalidMessageError("Invalid HMAC")

        if nonce in self.seen_nonces:
            raise ReplayAttackError(
                "Replay attack detected"
            )

        self.seen_nonces.add(nonce)

        if len(self.seen_nonces) > self.MAX_SEEN_NONCES:
            self.seen_nonces.pop()

        return payload

    def create_peer_challenge(self):
        return secrets.token_bytes(self.NONCE_SIZE)

    def create_peer_response(self, challenge, peer_id):
        if not isinstance(challenge, bytes):
            raise TypeError("Challenge must be bytes")

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
        challenge,
        peer_id,
        response,
    ):
        expected = self.create_peer_response(
            challenge,
            peer_id,
        )

        if not hmac.compare_digest(
            response,
            expected,
        ):
            raise PeerAuthenticationError(
                "Peer authentication failed"
            )

        return True
