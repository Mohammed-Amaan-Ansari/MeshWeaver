from dataclasses import dataclass

from meshweaver.dht.node_id import (
    ID_BITS,
    xor_distance,
)


@dataclass
class PeerInfo:
    """
    Information about a peer in the DHT.
    """

    node_id: bytes
    host: str
    port: int


class RoutingTable:

    def __init__(self, local_node_id):

        if not isinstance(
            local_node_id,
            bytes,
        ):
            raise TypeError(
                "local_node_id must be bytes"
            )

        self.local_node_id = local_node_id

        # Kademlia has one bucket
        # for each bit of the node ID.
        self.buckets = [
            []
            for _ in range(ID_BITS)
        ]

    # =====================================================
    # BUCKET INDEX
    # =====================================================

    def bucket_index(self, peer_node_id):

        distance = xor_distance(
            self.local_node_id,
            peer_node_id,
        )

        # Same node
        if distance == 0:
            return None

        return distance.bit_length() - 1

    # =====================================================
    # ADD PEER
    # =====================================================

    def add_peer(self, peer):

        index = self.bucket_index(
            peer.node_id
        )

        if index is None:
            return False

        bucket = self.buckets[index]

        # Prevent duplicate peers
        for existing_peer in bucket:

            if (
                existing_peer.node_id
                == peer.node_id
            ):
                return False

        bucket.append(peer)

        return True

    # =====================================================
    # REMOVE PEER
    # =====================================================

    def remove_peer(self, node_id):

        index = self.bucket_index(
            node_id
        )

        if index is None:
            return False

        bucket = self.buckets[index]

        for peer in bucket:

            if peer.node_id == node_id:

                bucket.remove(peer)

                return True

        return False

    # =====================================================
    # FIND BUCKET PEERS
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
    # GET ALL PEERS
    # =====================================================

    def get_all_peers(self):

        peers = []

        for bucket in self.buckets:

            peers.extend(bucket)

        return peers

    # =====================================================
    # XOR DISTANCE
    # =====================================================

    def distance_to(self, node_id):

        return xor_distance(
            self.local_node_id,
            node_id,
        )

    # =====================================================
    # FIND CLOSEST PEERS
    # =====================================================

    def find_closest_peers(
        self,
        target_node_id,
        count=3,
    ):
        """
        Return the closest peers to a target
        node ID using Kademlia XOR distance.
        """

        if not isinstance(
            target_node_id,
            bytes,
        ):
            raise TypeError(
                "target_node_id must be bytes"
            )

        if count <= 0:
            return []

        peers = self.get_all_peers()

        # Sort peers according to
        # XOR distance from target.
        peers.sort(
            key=lambda peer: xor_distance(
                peer.node_id,
                target_node_id,
            )
        )

        return peers[:count]

    # =====================================================
    # TABLE LENGTH
    # =====================================================

    def __len__(self):

        return sum(
            len(bucket)
            for bucket in self.buckets
        )