import json
import os
import threading
import zipfile
from flask import send_file, current_app
from flask_restx import Namespace, Resource
from flask_restx.inputs import boolean as flask_boolean
from http import HTTPStatus
from tqdm import tqdm
from werkzeug.datastructures import FileStorage

from ..connector import Connector
from app.analytics_tasks import RatingsCorrelationTask, DatesCorrelationTask, CountingByLitresDateTask, \
    MarksAndCommentsCountsCorrelation, CountingByAuthorTask, CountingByGenreTask
from app.utils.dump_db_to_json import delete_from_dict
from app.utils.utils import get_data_frame_from_mongodb


class BookDto:
    api = Namespace('book', description='Book operations')


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
                                                              description='', file_name='counting_by_litres_date_task',
                                                              label='Общее количество книг по выбранным годам'),
    'correlation_count_of_marks_and_count_of_comments': MarksAndCommentsCountsCorrelation(
                                                                 xaxis_title='Ранг количества оценок',
                                                                 yaxis_title='Ранг количества отзывов',
                                                                 name='Корреляция колиства оценок и отзывов',
                                                                 description='',
                                                                 file_name='marks_comments_correlation_task'),
    'get_authors_with_most_books': CountingByAuthorTask(top_count=20,
                                                        name='Распределение количества книг по авторам',
                                                        description='', file_name='counting_by_author_task',
                                                        label='Общее количество книг по выбранным авторам'),
    'get_more_popular_genres': CountingByGenreTask(top_count=10, name='Самые популярные жанры', description='',
                                                   file_name='counting_by_genre_task',
                                                   label='Общее количество книг по выбранным жанрам')
}


api = BookDto.api

_collect_parser = api.parser()

_collect_parser.add_argument('start_page', help='start book page (from 1). Only for selenium', type=int, default=1)
_collect_parser.add_argument('end_page', help='end book page. Only for selenium', type=int)
_collect_parser.add_argument('start_index', help='start book index. Only for reading from file', type=int, default=-1)
_collect_parser.add_argument('clear_database', type=flask_boolean, help='clear database', default=False)
_collect_parser.add_argument('links_file', type=FileStorage, help='File txt with book links', location='files')

_dump_parser = api.parser()
_dump_parser.add_argument('clear_database', type=flask_boolean, help='clear database', default=False)
_dump_parser.add_argument('dump_file', type=FileStorage, help='Database dump file', location='files',
                          required=True)

_analytic_parser = api.parser()
_analytic_parser.add_argument('task_name', help='Analytic task name', type=str,
                              choices=list(task_map.keys()),
                              required=True)


def collect(args, source_file=None):
    from app_main import app

    with app.app_context():
        open(app.config['LOCK_NAME'], 'w')
        connector = Connector()
        connector.collect(args['start_page'], args['end_page'], args['start_index'], args['clear_database'],
                          source_file=source_file, server_side=True)
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

        th = threading.Thread(target=collect, args=(args, source_file))
        th.start()

        return 'Started collecting'

    @api.doc('get_books_count')
    def get(self):
        """Get current books count in database"""
        books = current_app.db.books.find()
        count = books.count()
        return {'count': count}

    @api.doc('stop_collect')
    def delete(self):
        """Stop current collecting process"""
        if os.path.exists(current_app.config['LOCK_NAME']):
            os.remove(current_app.config['LOCK_NAME'])
        return "Stopped collecting"


@api.route('/dump')
class BooksDumpApi(Resource):
    @api.doc('load_dump')
    @api.expect(_dump_parser)
    def post(self):
        """Load json dump into database"""
        args = _dump_parser.parse_args()
        connector = Connector()
        dump_path = './assets/tmp_dump.json'
        args['dump_file'].save(dump_path)
        connector.connect_from_file(dump_path, args['clear_database'])
        os.remove(dump_path)
        return 'Success'

    @api.doc('get_dump')
    def get(self):
        """Get database json dump"""
        books = current_app.db.books.find()
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

        df = get_data_frame_from_mongodb(current_app.db_uri)
        filenames = task_map[args['task_name']].run_process(df)
        zip_name = f"{os.getcwd()}/analytics_results/zip/{args['task_name']}.zip"
        zipf = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
        for filename in filenames:
            zipf.write(f"{filename}")
        zipf.close()

        return send_file(zip_name, mimetype='zip', as_attachment=True)
