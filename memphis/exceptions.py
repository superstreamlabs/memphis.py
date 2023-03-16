class MemphisError(Exception):
    def __init__(self, message):
        message = message.replace("nats", "memphis")
        message = message.replace("NATS", "memphis")
        message = message.replace("Nats", "memphis")
        message = message.replace("NatsError", "MemphisError")
        self.message = message
        if message.startswith("memphis:"):
            super().__init__(self.message)
        else:
            super().__init__("memphis: " + self.message)


class MemphisConnectError(MemphisError):
    pass


class MemphisSchemaError(MemphisError):
    pass


class MemphisHeaderError(MemphisError):
    pass
