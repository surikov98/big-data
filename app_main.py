import os
from app import create_app
from app.util import get_version
from flask import request

app = create_app(os.getenv('SERVER_ENV') or 'dev')


@app.route('/version', methods=['GET'])
def get():
    return {'version': get_version()}


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Access-Control-Allow-Headers, Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    if app.config['LOG_ERRORS'] and response.status_code >= 400:
        app.logger.error("Request url: {request_url}."
                         "Request headers: {request_headers}."
                         "Request data: {request_data}."
                         "Response status: {response_status}."
                         "Response data: {response_data}".format(request_url=request.url,
                                                                 request_data=request.get_data(),
                                                                 request_headers=request.headers,
                                                                 response_status=response.status,
                                                                 response_data=response.get_data()))

    return response
