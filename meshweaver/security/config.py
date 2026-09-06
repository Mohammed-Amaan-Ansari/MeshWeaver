import os


class SecurityConfigError(Exception):
    """Raised when security configuration is invalid."""


class SecurityConfig:
    """
    MeshWeaver security configuration.

    Loads the shared transport-security key from
    the MESHWEAVER_SECURITY_KEY environment variable.
    """

    ENV_KEY = "MESHWEAVER_SECURITY_KEY"
    MIN_KEY_LENGTH = 32

    def __init__(self, key: bytes):
        if not isinstance(key, bytes):
            raise TypeError("Security key must be bytes.")

        if len(key) < self.MIN_KEY_LENGTH:
            raise SecurityConfigError(
                "Security key must contain at least 32 bytes."
            )

        self.key = key

    @classmethod
    def from_environment(cls):
        """
        Load the security key from the environment.
        """

        value = os.getenv(cls.ENV_KEY)

        if not value:
            raise SecurityConfigError(
                f"Missing environment variable: {cls.ENV_KEY}"
            )

        try:
            key = bytes.fromhex(value)
        except ValueError as exc:
            raise SecurityConfigError(
                "Security key must be a valid hexadecimal string."
            ) from exc

        return cls(key)

    @classmethod
    def development(cls):
        """
        Development-only configuration.

        This keeps local development convenient while making
        production configuration environment-based.
        """

        return cls(
            b"12345678901234567890123456789012"
        )