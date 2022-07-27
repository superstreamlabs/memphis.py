#  Copyright 2021-2022 The Memphis Authors
#  Licensed under the Apache License, Version 2.0 (the “License”);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an “AS IS” BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License

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
        typeOfMethod = {
            "GET": requests.get(url, headers=headers, timeout=timeout,
                                data=json.dumps(body_params), params=json.dumps(query_params)),
            "POST": requests.post(url, headers=headers, timeout=timeout,
                                  data=json.dumps(body_params), params=json.dumps(query_params)),
            'PUT': requests.put(url, headers=headers, timeout=timeout,
                                data=json.dumps(body_params), params=json.dumps(query_params)),
            'DELETE': requests.delete(url, headers=headers, timeout=timeout,
                                      data=json.dumps(body_params), params=json.dumps(query_params))
        }
        response = typeOfMethod.get(method)
        if response.status_code != 200:
            raise Exception(response.text)
        else:
            return response.text
    except Exception as e:
        raise Exception(e)
