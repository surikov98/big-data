import json
import os
import threading
import zipfile

from flask import send_file
from flask_restx import Namespace, Resource
from http import HTTPStatus
from pymongo import MongoClient
from tqdm import tqdm
from werkzeug.datastructures import FileStorage

from analytics_tasks import RatingsCorrelationTask, DatesCorrelationTask, CountingByLitresDateTask

from dump_db_to_json import delete_from_dict
from connector import Connector
from utils import get_uri_mongodb, get_data_frame_from_mongodb


class BookDto:
    api = Namespace('book', description='Book operations')


THREAD_NAME = 'book_collect'

task_map = {
    'calculate_ratings_rank_correlation': RatingsCorrelationTask(xaxis_title='Ранг рейтинга литреса',
                                                                 yaxis_title='Ранг рейтинга лайвлиба',
                                                                 name='Корреляция рейтингов литрес и лайвлиб',
                                                                 description='', file_name='ratings_correlation_task'),
    'correlation_date_litres_and_date_writing':
        DatesCorrelationTask(xaxis_title='Ранг года выхода на Литресс', yaxis_title='Ранг года написания',
                             name='Корреляция даты написания и выхода на литрес', description='',
                             file_name='dates_correlation_task'),
    'count_books_by_year_on_litres': CountingByLitresDateTask(top_count=5,
                                                              name='Распределение количества книг по годам',
                                                              description='', file_name='counting_by_litres_date_task')
}


def update_argument_parser_mongodb(parser):
    parser.add_argument('username', help='username for authentication')
    parser.add_argument('password', help='password for authentication')
    parser.add_argument('database', help='database to connect to', required=True)
    parser.add_argument('host', help='server to connect to', default='localhost')
    parser.add_argument('port', help='port to connect to', default=27017)
    parser.add_argument('authenticationDatabase', help='user source')


api = BookDto.api

_collect_parser = api.parser()
update_argument_parser_mongodb(_collect_parser)

_collect_parser.add_argument('start_page', help='start book page (from 1)', type=int, default=1)
_collect_parser.add_argument('end_page', help='end book page', type=int)
_collect_parser.add_argument('start_book', help='start book index', type=int, default=1)
_collect_parser.add_argument('end_book', help='end book index', type=int, default=50)
_collect_parser.add_argument('clear_database', help='clear database', action='store_true')
_collect_parser.add_argument('links_file', type=FileStorage, help='File txt with book links', location='files')

_db_parser = api.parser()
update_argument_parser_mongodb(_db_parser)

_dump_parser = api.parser()
update_argument_parser_mongodb(_dump_parser)
_dump_parser.add_argument('clear_database', help='clear database', action='store_true')
_dump_parser.add_argument('dump_file', type=FileStorage, help='Database dump file', location='files',
                          required=True)

_analytic_parser = api.parser()
update_argument_parser_mongodb(_analytic_parser)
_analytic_parser.add_argument('task_name', help='Analytic task name', type=str,
                              choices=list(task_map.keys()),
                              required=True)


def collect(args, source_file=None):
    connector = Connector(args['database'], args['username'], args['password'], args['host'], args['port'],
                          args['authenticationDatabase'])
    connector.collect(args['start_page'], args['end_page'], args['start_book'], args['end_book'],
                      args['clear_database'], source_file=source_file, server_side=True)
    try:
        os.remove(source_file)
    except Exception:
        pass


@api.route('/collect')
class BooksCollectApi(Resource):
    @api.doc('collect_books')
    @api.expect(_collect_parser)
    def post(self):
        """Start collecting books from litres"""
        args = _collect_parser.parse_args()
        source_file = None

        if args.get('links_file'):
            file: FileStorage = args['links_file']
            if file.mimetype != 'text/plain':
                api.abort(HTTPStatus.BAD_REQUEST, 'Wrong file type')
            file.save('./assets/all_links_tmp.txt')
            source_file = './assets/all_links_tmp.txt'

        th = threading.Thread(target=collect, name=THREAD_NAME, args=(args, source_file))
        th.start()

        return 'Started collecting'

    @api.doc('get_books_count')
    @api.expect(_db_parser)
    def get(self):
        """Get current books count in database"""
        args = _db_parser.parse_args()
        uri = get_uri_mongodb(args['database'], args['username'], args['password'], args['host'], args['port'],
                              args['authenticationDatabase'])
        client = MongoClient(uri)
        db = client.get_database()
        books = db.books.find()
        count = books.count()
        return {'count': count}


@api.route('/dump')
class BooksDumpApi(Resource):
    @api.doc('load_dump')
    @api.expect(_dump_parser)
    def post(self):
        """Load json dump into database"""
        args = _dump_parser.parse_args()
        connector = Connector(args['database'], args['username'], args['password'], args['host'], args['port'],
                              args['authenticationDatabase'])
        dump_path = './assets/dump_tmp.json'
        args['dump_file'].save(dump_path)
        connector.connect_from_file(dump_path, args['clear_database'])
        os.remove(dump_path)
        return 'Success'

    @api.doc('get_dump')
    @api.expect(_db_parser)
    def get(self):
        """Get database json dump"""
        args = _db_parser.parse_args()
        uri = get_uri_mongodb(args['database'], args['username'], args['password'], args['host'], args['port'],
                              args['authenticationDatabase'])
        client = MongoClient(uri)
        db = client.get_database()
        books = db.books.find()
        count = books.count()

        dump_path = 'assets/dump.json'

        with open(f'./{dump_path}', 'w', encoding='utf8') as file_object:
            file_object.write(f'[\n{json.dumps({"count": count})},\n')
            for i, book in enumerate(tqdm(books, ascii=True, total=count)):
                s = json.dumps(delete_from_dict(book, '_id'), ensure_ascii=False)
                if i < count - 1:
                    s += ',\n'
                file_object.write(s)
            file_object.write('\n]')
        result = send_file(f'{os.getcwd()}/{dump_path}', mimetype='application/json', as_attachment=True)
        try:
            os.remove(dump_path)
        except Exception as e:
            pass
        return result


@api.route('/analytic')
class BooksAnalyticApi(Resource):
    @api.doc('get_analytic')
    @api.expect(_analytic_parser)
    def get(self):
        """Execute analytic task"""
        args = _analytic_parser.parse_args()
        if args['task_name'] not in task_map:
            api.abort(HTTPStatus.NOT_FOUND, 'Specified task not found')

        df = get_data_frame_from_mongodb(args['database'], args['username'], args['password'], args['host'],
                                         args['port'], args['authenticationDatabase'])
        filenames = task_map[args['task_name']].run_process(df)
        zip_name = f"{os.getcwd()}/analytics_results/zip/{args['task_name']}.zip"
        zipf = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
        for filename in filenames:
            zipf.write(f"{filename}")
        zipf.close()

        return send_file(zip_name, mimetype='zip', as_attachment=True)
