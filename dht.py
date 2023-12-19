from threading import Lock

class DistributedHashTable:
    def __init__(self) -> None:
        self.map = {}
        self.lock = Lock()

        def set(self, key, value, req_term) -> bool:
            with self.lock:
                if(key not in self.map or self.map[key][1] < req_term):
                    self.map[key] = (value, req_term)
                    return True
                return False

        def get(self, key):
            with self.lock:
                if key in self.map:
                    return self.map[key][0]
                return None