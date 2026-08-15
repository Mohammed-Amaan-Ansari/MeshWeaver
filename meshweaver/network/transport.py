import asyncio


class UDPProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):
        self.node = node
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.node.transport = transport

        print(
            f"[{self.node.node_id[:8]}] "
            f"UDP listening on "
            f"{self.node.host}:{self.node.port}"
        )

    def datagram_received(self, data, addr):
        asyncio.create_task(
            self.node.handle_message(data, addr)
        )

    def error_received(self, exc):
        print(f"UDP error: {exc}")


async def start_udp_server(node):

    loop = asyncio.get_running_loop()

    transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(node),
        local_addr=(node.host, node.port),
    )

    return transport