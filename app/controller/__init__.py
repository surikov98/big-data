from flask import Blueprint, url_for
from flask_restx import Api
from flask_restx.apidoc import apidoc
from os import environ

from . import book_controller

from ..util import get_version


class ApiScheme(Api):
    @property
    def specs_url(self):
        scheme = environ['SCHEME']
        return url_for(self.endpoint('specs'), _external=True, _scheme=scheme)


AUTHORIZATIONS = {
    'api-token': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'authorization'
    }
}


def init_app(app):
    apidoc.url_prefix = app.config['URL_PREFIX']
    blueprint = Blueprint('api', __name__, url_prefix=app.config['URL_PREFIX'])
    api = ApiScheme(blueprint, version=get_version(), title='LITRES BIGDATA API', description='API for BIGDATA',
                    doc=app.config['DOC_URL'], authorizations=AUTHORIZATIONS)

    api.add_namespace(book_controller.api)
    app.register_blueprint(blueprint)
