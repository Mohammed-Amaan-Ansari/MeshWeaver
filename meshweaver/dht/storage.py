class DHTStorage:

    def __init__(self):

        self.data = {}

    def store(
        self,
        key,
        value,
    ):

        if not isinstance(
            key,
            str,
        ):
            raise TypeError(
                "key must be a string"
            )

        self.data[key] = value

        return True

    def get(
        self,
        key,
    ):

        return self.data.get(key)

    def exists(
        self,
        key,
    ):

        return key in self.data

    def delete(
        self,
        key,
    ):

        if key not in self.data:
            return False

        del self.data[key]

        return True

    def keys(self):

        return list(
            self.data.keys()
        )

    def all_items(self):

        return dict(
            self.data
        )

    def __len__(self):

        return len(self.data)