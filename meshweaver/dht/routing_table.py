from dataclasses import dataclass

from meshweaver.dht.node_id import (
    ID_BITS,
    xor_distance,
)


@dataclass
class PeerInfo:

    node_id: bytes
    host: str
    port: int


class RoutingTable:

    def __init__(
        self,
        local_node_id: bytes,
    ):

        if not isinstance(
            local_node_id,
            bytes,
        ):
            raise TypeError(
                "local_node_id must be bytes"
            )

        self.local_node_id = local_node_id

        self.buckets = [
            []
            for _ in range(ID_BITS)
        ]

    # =====================================================
    # BUCKET
    # =====================================================

    def bucket_index(
        self,
        peer_node_id,
    ):

        distance = xor_distance(
            self.local_node_id,
            peer_node_id,
        )

        if distance == 0:
            return None

        return distance.bit_length() - 1

    # =====================================================
    # ADD
    # =====================================================

    def add_peer(
        self,
        peer: PeerInfo,
    ):

        index = self.bucket_index(
            peer.node_id
        )

        if index is None:
            return False

        bucket = self.buckets[index]

        for existing in bucket:

            if existing.node_id == peer.node_id:

                existing.host = peer.host
                existing.port = peer.port

                return False

        bucket.append(peer)

        return True

    # =====================================================
    # REMOVE
    # =====================================================

    def remove_peer(
        self,
        node_id,
    ):

        index = self.bucket_index(
            node_id
        )

        if index is None:
            return False

        bucket = self.buckets[index]

        for peer in list(bucket):

            if peer.node_id == node_id:

                bucket.remove(peer)

                return True

        return False

    # =====================================================
    # ALL PEERS
    # =====================================================

    def get_all_peers(self):

        peers = []

        for bucket in self.buckets:
            peers.extend(bucket)

        return peers

    # =====================================================
    # CLOSEST
    # =====================================================

    def find_closest_peers(
        self,
        target_node_id,
        count=3,
    ):

        if count <= 0:
            return []

        peers = self.get_all_peers()

        peers.sort(
            key=lambda peer:
                xor_distance(
                    peer.node_id,
                    target_node_id,
                )
        )

        return peers[:count]

    # =====================================================
    # BUCKET PEERS
    # =====================================================

    def find_bucket_peers(
        self,
        peer_node_id,
    ):

        index = self.bucket_index(
            peer_node_id
        )

        if index is None:
            return []

        return list(
            self.buckets[index]
        )

    # =====================================================
    # LENGTH
    # =====================================================

    def __len__(self):

        return sum(
            len(bucket)
            for bucket in self.buckets
        )