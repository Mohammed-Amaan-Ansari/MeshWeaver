class DHTStorage:

    def __init__(self):

        # In-memory key-value storage
        self.data = {}

    # =====================================================
    # STORE
    # =====================================================

    def store(self, key, value):

        if not isinstance(key, str):

            raise TypeError(
                "key must be a string"
            )

        self.data[key] = value

        return True

    # =====================================================
    # GET
    # =====================================================

    def get(self, key):

        return self.data.get(key)

    # =====================================================
    # EXISTS
    # =====================================================

    def exists(self, key):

        return key in self.data

    # =====================================================
    # DELETE
    # =====================================================

    def delete(self, key):

        if key not in self.data:

            return False

        del self.data[key]

        return True

    # =====================================================
    # ALL KEYS
    # =====================================================

    def keys(self):

        return list(
            self.data.keys()
        )

    # =====================================================
    # SIZE
    # =====================================================

    def __len__(self):

        return len(self.data)