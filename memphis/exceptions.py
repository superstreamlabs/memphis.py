from dataclasses import dataclass

MAX_BATCH_SIZE = 5000


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


@dataclass
class MemphisErrors:
    TimeoutError: MemphisError = MemphisError("Memphis: TimeoutError")
    PartitionNumberKeyError: MemphisError = MemphisError(
        "Can not use both partition number and partition key"
    )
    PartitionOutOfRange: MemphisError = MemphisError("Partition number is out of range")
    InvalidBatchSize: MemphisError = MemphisError(
        f"Batch size can not be greater than {MAX_BATCH_SIZE} or less than 1"
    )
    DeadConnection: MemphisError = MemphisError("Connection is dead")
    MissingNameOrStationName: MemphisError = MemphisError(
        "name and station name can not be empty"
    )
    MissingStationName: MemphisError = MemphisError("station name is missing")
    InvalidStationNameTpye: MemphisError = MemphisError(
        "station_name should be either string or list of strings"
    )
    NonPositiveStartConsumeFromSeq: MemphisError = MemphisError(
        "start_consume_from_sequence has to be a positive number"
    )
    InvalidMinLasMessagesVal: MemphisError = MemphisError(
        "min value for last_messages is -1"
    )
    ContainsStartConsumeAndLastMessages: MemphisError = MemphisError(
        "Consumer creation options can't contain both start_consume_from_sequence and last_messages"
    )
    MissingSchemaName: MemphisError = MemphisError("Schema name cannot be empty")
    SchemaNameTooLong: MemphisError = MemphisError(
        "Schema name should be under 128 characters"
    )
    InvalidSchemaChars: MemphisError = MemphisError(
        "Only alphanumeric and the '_', '-', '.' characters are allowed in the schema name"
    )
    InvalidSchemaStartChar: MemphisError = MemphisError(
        "Schema name cannot start or end with a non-alphanumeric character"
    )
    DLSCannotBeDelayed: MemoryError = MemphisError("cannot delay DLS message")

    InvalidKeyInHeader: MemphisHeaderError = MemphisHeaderError(
        "Keys in headers should not start with $memphis"
    )

    InvalidConnectionType: MemphisConnectError = MemphisConnectError(
        "You have to connect with one of the following methods: connection token / password"
    )
    MissingTLSCert: MemphisConnectError = MemphisConnectError(
        "Must provide a TLS cert file"
    )
    MissingTLSKey: MemphisConnectError = MemphisConnectError(
        "Must provide a TLS cert file"
    )
    MissingTLSCa: MemphisConnectError = MemphisConnectError(
        "Must provide a TLS cert file"
    )

    UnsupportedMsgType: MemphisSchemaError = MemphisSchemaError(
        "Unsupported message type"
    )

    @staticmethod
    def expecting_format(error: Exception, format: str):
        return MemphisError(f"Expecting {format} format: " + str(error))

    @staticmethod
    def schema_validation_failed(error):
        return MemphisSchemaError("Schema validation has failed: " + str(error))

    @staticmethod
    def schema_msg_mismatch(error: Exception):
        return MemphisSchemaError(
            f"Deserialization has been failed since the message format does not align with the currently attached schema: {str(error)}"
        )

    @staticmethod
    def partition_not_in_station(partition_number, station_name):
        return MemphisError(
            f"Partition {str(partition_number)} does not exist in station {station_name}"
        )

    @staticmethod
    def invalid_schema_type(schema_type):
        return MemphisError(
            "schema type not supported"
            + schema_type
            + " is not json, graphql, protobuf or avro"
        )
