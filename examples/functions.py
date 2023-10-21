from memphis import memphis, MemphisConnectError, MemphisError, MemphisHeaderError
from memphis.message import Message

# JSON validated or no schema
def default_function(headers: dict, payload: dict):
    pass

# def protobuf_function(headers: dict, payload: )