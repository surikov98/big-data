from os import environ as env


class Config:
    HOST = env['HOST']
    DEBUG = False
    # WORKERS = env['WORKERS']
    LOG_ERRORS = False
    BOOKS_PER_PAGE = 50


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
