from memphis.exceptions import MemphisHeaderError


class Headers:
    def __init__(self):
        self.headers = {}

    def add(self, key, value):
        """Add a header.
        Args:
            key (string): header key.
            value (string): header value.
        Raises:
            Exception: _description_
        """
        if not key.startswith("$memphis"):
            self.headers[key] = value
        else:
            raise MemphisHeaderError("Keys in headers should not start with $memphis")
