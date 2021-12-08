from os import environ as env


class Config:
    HOST = env['HOST']
    DEBUG = False
    LOG_ERRORS = False
    BOOKS_PER_PAGE = 50

    BASE_URL = 'https://www.litres.ru'

    DB_HOST = env.get('DB_HOST', '127.0.0.1')
    DB_PORT = env.get('DB_PORT', '27017')
    DB_USER = env.get('MONGO_INITDB_ROOT_USERNAME')
    DB_PASSWORD = env.get('MONGO_INITDB_ROOT_PASSWORD')
    DB_NAME = env.get('MONGO_INITDB_DATABASE', 'bigdata')

    ANTICAPTCHA_TOKEN = env['ANTICAPTCHA_TOKEN']
    ANTICAPTCHA_TIMEOUT = 300

    HREF_CHECKPOINT = './checkpoints/checkpoint_href.txt'
    DB_CHECKPOINT = './checkpoints/checkpoint_db.txt'

    LOCK_NAME = 'collect.lock'


class DevConfig(Config):
    DEV = True
    DOC_URL = '/swagger'
    URL_PREFIX = '/api/v1'


class ProdConfig(Config):
    URL_PREFIX = '/api/v1'
    DOC_URL = False
    DEV = False


config_by_name = dict(
    dev=DevConfig,
    prod=ProdConfig
)
