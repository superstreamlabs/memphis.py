import json
import base64

def create_function(
    event,
    user_func: callable
) -> None:
    """
    This function handles a batch of messages and processes them with the passed-in user function.

    Args:
        event (dict):
            The Lambda event that is provided to the function Lambda is calling, usually named `lambda_handler`. 
            For more information, refer to: https://docs.aws.amazon.com/lambda/latest/dg/python-handler.html
        user_func (callable):
            `create_function` assumes the function signature is in the format: <user_func>(payload, headers) -> processed_payload, processed_headers. 
            This function will modify the payload and headers and return them in the modified format.

            Args:
                payload (bytes): The payload of the message. It will be encoded as bytes, and the user can assume UTF-8 encoding.
                headers (dict): The headers associated with the Memphis message.

            Returns:
                modified_message (bytes): The modified message must be encoded into bytes before being returned from the `user_func`.
                modified_headers (dict): The headers will be passed in and returned as a Python dictionary.

            Raises:
                Error:
                    Raises an exception of any kind when something goes wrong with processing a message. 
                    The unprocessed message and the exception will be appended to the list of failed messages that is returned to the broker.

    Returns:
        lambda_handler (callable):
            A function that handles the batching of data and manages the returned messages. 
            Return the result of this function in the Lambda handler.
    """
    class EncodeBase64(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, bytes):
                return str(base64.b64encode(o), encoding='utf-8')
            return json.JSONEncoder.default(self, o)

    def lambda_handler(event):
        processed_events = {}
        processed_events["successfullMessages"] = []
        processed_events["errorMessages"] = []
        for message in event["messages"]:
            try:
                payload = base64.b64decode(bytes(message['payload'], encoding='utf-8'))
                processed_message, processed_headers = user_func(payload, message['headers'])

                if isinstance(processed_message, bytes) and isinstance(processed_headers, dict):
                    processed_events["successfullMessages"].append({
                        "headers": processed_headers,
                        "payload": processed_message
                    })
                else:
                    err_msg = "The returned processed_message or processed_headers were not in the right format. processed_message must be bytes and processed_headers, dict"
                    raise Exception(err_msg)

            except Exception as e:
                processed_events["errorMessages"].append({
                    "headers": message["headers"],
                    "payload": message["payload"],
                    "error": str(e)  
                })

        try:
            return json.dumps(processed_events, cls=EncodeBase64).encode('utf-8')
        except Exception as e:
            return f"Returned message types from user function are not able to be converted into JSON: {e}"

    return lambda_handler(event)