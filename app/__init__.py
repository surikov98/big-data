from flask import Flask
from flask_cors import CORS
from . import controller
from .config import config_by_name


def create_flask_app(config_name):
    config_file = config_by_name[config_name]
    app = Flask(__name__)
    app.config.from_object(config_file)
    return app


def create_app(config_name):
    app = create_flask_app(config_name)
    CORS(app, resources={r'/*': {'origins': '*'}}, supports_credentials=True)
    controller.init_app(app)
    return app
