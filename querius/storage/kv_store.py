from typing import Any, Dict, Optional

class KeyValueStore:
    def __init__(self):
        self.store: Dict[str, Dict[str, Any]] = {}

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        return self.store.get(key)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.store[key] = value

    def delete(self, key: str) -> None:
        if key in self.store:
            del self.store[key]

    def all(self) -> Dict[str, Dict[str, Any]]:
        return self.store
