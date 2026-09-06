import asyncio


class UDPProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):
        self.node = node

    def connection_made(self, transport):
        self.node.transport = transport

        print(
            f"[{self.node.node_id}] "
            f"UDP listening on "
            f"{self.node.host}:{self.node.port}"
        )

    def datagram_received(self, data, addr):
        asyncio.create_task(
            self.node.handle_message(data, addr)
        )

    def error_received(self, exc):
        print(
            f"[{self.node.node_id}] "
            f"UDP error: {exc}"
        )

    def connection_lost(self, exc):
        self.node.transport = None

        if exc:
            print(
                f"[{self.node.node_id}] "
                f"UDP connection lost: {exc}"
            )


async def start_udp_server(node):

    loop = asyncio.get_running_loop()

    transport, _ = (
        await loop.create_datagram_endpoint(
            lambda: UDPProtocol(node),
            local_addr=(
                node.host,
                node.port,
            ),
        )
    )

    return transport