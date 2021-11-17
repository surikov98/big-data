class CaptchaError(Exception):
    def __str__(self):
        return 'Captcha was caught. Please change cookies.'


class ConnectionError(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return f'Failed to connect to {self.url}'


class DBConnectionError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f'Failed to connect to MongoDB: {self.message}'
