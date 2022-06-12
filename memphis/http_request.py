import requests
import json


def http_request(method, url, headers={}, body_params={}, query_params={}, file=None, timeout=None):
    if method != 'GET' and method != 'POST' and method != 'PUT' and method != 'DELETE':
        raise Exception(
            {"status": 400, "message": "Invalid HTTP method", "data": {method, url, body_params}})
    if file:
        body_params['file'] = file

    headers['content-type'] = 'application/json'
    try:
        match method:
            case 'GET':
                response = requests.get(url, headers=headers, timeout=timeout,
                                        data=json.dumps(body_params), params=json.dumps(query_params))
            case 'POST':
                response = requests.post(url, headers=headers, timeout=timeout,
                                         data=json.dumps(body_params), params=json.dumps(query_params))
            case 'PUT':
                response = requests.put(url, headers=headers, timeout=timeout,
                                        data=json.dumps(body_params), params=json.dumps(query_params))
            case 'DELETE':
                response = requests.delete(url, headers=headers, timeout=timeout,
                                           data=json.dumps(body_params), params=json.dumps(query_params))
        if response.status_code != 200:
            raise Exception(response.text)
        else:
            return response.text
    except Exception as e:
        raise Exception(e)
