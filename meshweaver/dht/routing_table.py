from dataclasses import dataclass

from meshweaver.dht.node_id import (
    ID_BITS,
    node_id_to_int,
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

        self.local_node_id = (
            local_node_id
        )

        # Kademlia uses one bucket
        # for each bit of the node ID.
        self.buckets = [
            []
            for _ in range(ID_BITS)
        ]

    def bucket_index(self, peer_node_id):
        """
        Determine which Kademlia bucket
        should contain the peer.
        """

        distance = xor_distance(
            self.local_node_id,
            peer_node_id,
        )

        if distance == 0:

            return None

        index = distance.bit_length() - 1

        return index

    def add_peer(self, peer):

        index = self.bucket_index(
            peer.node_id
        )

        # Same node ID as local node.
        if index is None:

            return False

        bucket = self.buckets[index]

        # Avoid duplicates.
        for existing_peer in bucket:

            if (
                existing_peer.node_id
                == peer.node_id
            ):

                return False

        bucket.append(peer)

        return True

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

    def get_all_peers(self):

        peers = []

        for bucket in self.buckets:

            peers.extend(bucket)

        return peers

    def __len__(self):

        return sum(
            len(bucket)
            for bucket in self.buckets
        )