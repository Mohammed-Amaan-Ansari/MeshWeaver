from dataclasses import dataclass, field
import asyncio
import uuid
import json

from meshweaver.network.transport import start_udp_server