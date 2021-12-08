from pymongo import MongoClient
from app.utils.utils import get_uri_mongodb


def init_db(app):
    uri = get_uri_mongodb(app.config['DB_NAME'], app.config['DB_USER'], app.config['DB_PASSWORD'],
                          app.config['DB_HOST'], app.config['DB_PORT'], None)
    client = MongoClient(uri)
    app.mongo_client = client
    app.db_uri = uri
    app.db = client.get_database()
