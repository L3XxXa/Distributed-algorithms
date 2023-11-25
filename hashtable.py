from threading import Lock

class HashTable:
    def __init__(self) -> None:
        self.map = {}
        self.lock = Lock()

        def add_value(self, key, value, req_id) -> bool:
            with self.lock:
                if(key not in self.map or self.map[key][1] < req_id):
                    self.map[key] = (value, req_id)
                    return True
                return False
            
        def get_value(self, key):
            with self.lock:
                if key in self.map:
                    return self.map[key][0]
                return None