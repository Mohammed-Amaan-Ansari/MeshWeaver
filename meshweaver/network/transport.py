import asyncio
import json


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_message):
        self.on_message = on_message

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = json.loads(data.decode())
            asyncio.create_task(self.on_message(message, addr))
        except Exception as e:
            print(f"❌ Failed to process packet from {addr}: {e}")
