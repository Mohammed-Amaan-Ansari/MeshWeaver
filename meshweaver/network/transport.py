import asyncio

from meshweaver.security.transport_security import (
    TransportSecurity,
    TransportSecurityError,
)


class UDPProtocol(asyncio.DatagramProtocol):

    def __init__(
        self,
        node,
        security=None,
    ):
        self.node = node
        self.security = security

    def connection_made(
        self,
        transport,
    ):
        self.node.transport = transport

        print(
            f"[{self.node.node_id}] "
            f"UDP listening on "
            f"{self.node.host}:{self.node.port}"
        )

        if self.security is not None:

            print(
                f"[{self.node.node_id}] "
                f"Transport security: "
                f"HMAC-SHA256 enabled"
            )

    def datagram_received(
        self,
        data,
        addr,
    ):

        # ---------------------------------------------
        # SECURITY
        # ---------------------------------------------

        if self.security is not None:

            try:

                data = self.security.unprotect(
                    data
                )

            except TransportSecurityError as exc:

                print(
                    f"[{self.node.node_id}] "
                    f"SECURITY ERROR from {addr}: "
                    f"{exc}"
                )

                return

            except Exception as exc:

                print(
                    f"[{self.node.node_id}] "
                    f"Security processing error: "
                    f"{exc}"
                )

                return

        # ---------------------------------------------
        # MESSAGE HANDLING
        # ---------------------------------------------

        asyncio.create_task(
            self.node.handle_message(
                data,
                addr,
            )
        )

    def error_received(
        self,
        exc,
    ):

        print(
            f"[{self.node.node_id}] "
            f"UDP error: {exc}"
        )

    def connection_lost(
        self,
        exc,
    ):

        self.node.transport = None

        if exc:

            print(
                f"[{self.node.node_id}] "
                f"UDP connection lost: {exc}"
            )


class SecureUDPTransport:
    """
    Wrapper around asyncio's UDP transport.

    Keeps the existing API used by node.py:

        transport.sendto(data, addr)

    while applying transport security before
    the datagram reaches the UDP socket.
    """

    def __init__(
        self,
        transport,
        security,
    ):

        self._transport = transport
        self._security = security

    def sendto(
        self,
        data,
        addr,
    ):

        secure_data = (
            self._security.protect(
                data
            )
        )

        self._transport.sendto(
            secure_data,
            addr,
        )

    def close(self):

        self._transport.close()

    def is_closing(self):

        return self._transport.is_closing()


async def start_udp_server(
    node,
    security_key=None,
):

    loop = asyncio.get_running_loop()

    security = None

    if security_key is not None:

        security = TransportSecurity(
            security_key
        )

    raw_transport, _ = (
        await loop.create_datagram_endpoint(
            lambda: UDPProtocol(
                node,
                security=security,
            ),
            local_addr=(
                node.host,
                node.port,
            ),
        )
    )

    # ---------------------------------------------
    # SECURITY ENABLED
    # ---------------------------------------------

    if security is not None:

        secure_transport = (
            SecureUDPTransport(
                raw_transport,
                security,
            )
        )

        # node.py expects:
        #
        # self.transport.sendto(...)
        #
        # so replace the raw transport with
        # our secure wrapper.

        node.transport = secure_transport

        return secure_transport

    # ---------------------------------------------
    # LEGACY / INSECURE MODE
    # ---------------------------------------------

    node.transport = raw_transport

    return raw_transport