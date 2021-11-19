import json
import traceback

from http import HTTPStatus

from bs4 import BeautifulSoup
import requests
from lxml.html import fromstring
from pymongo import MongoClient
from time import sleep
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from selenium.common.exceptions import NoSuchElementException
from typing import Union

from .errors import CaptchaError, ConnectionError, DBConnectionError
from .insert_buffer import InsertBuffer
from .request import Request
from utils import get_uri_mongodb

import re

BASE_URL = 'https://www.litres.ru'
BOOKS_PER_PAGE = 50

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/85.0.4183.121 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru,en-us;q=0.7,en;q=0.3',
    'Accept-Encoding': 'deflate',
    'Accept-Charset': 'windows-1251,utf-8;q=0.7,*;q=0.7',
    'Keep-Alive': '300',
    'Connection': 'keep-alive',
    'Referer': 'https://www.litres.ru/',
    'Cookie': 'user-geo-region-id=2; user-geo-country-id=2; desktop_session_key=a879d130e3adf0339260b581e66d773df11'
              'd8e9d3c7ea1053a6a7b473c166afff28b4d6c3e80e91249baaa7f3c3e90ef898a714ba131694d595c6a4f7e8f6df19d46c31'
              'ce10d2837ff5ad61d138aefd65c01aa7acc1327ce6d0918deae0a3c71; '
              'desktop_session_key.sig=drC4D-uw685k9LLTsxPhIFVyLFY; '
              'i=Hn0YWarMxO/96XpUg9b7btBjrSjo+ItWSfeOXC4oUOtwp6TEcbOkk/ajoJbz1xD/0dPkdWRcJTTk3x1/kZ09uNlji8g=; '
              'mda_exp_enabled=1; sso_status=sso.passport.yandex.ru:blocked; yandex_plus_metrika_cookie=true; '
              '_ym_wasSynced=%7B%22time%22%3A1604668139580%2C%22params%22%3A%7B%22eu%22%3A0%7D%2C%22bkParams%22%3A%'
              '7B%7D%7D; gdpr=0; _ym_uid=1604668140171070080; _ym_d=1604668140; mda=0; _ym_isad=1; '
              '_ym_visorc_56177992=b; _ym_visorc_52332406=b; _ym_visorc_22663942=b; location=1',
}

_BUFFER_SIZE = BOOKS_PER_PAGE
_FILE_WITH_HREF_NAME = './connector/configs/all_links.txt'
_CHECKPOINT_FILE_NAME = './connector/configs/checkpoint.txt'


class Connector:
    def __init__(self, database: str, username: Union[str, None] = None,
                 password: Union[str, None] = None, host: str = 'localhost', port: Union[int, str] = 27017,
                 authentication_database: Union[str, None] = None, sorting: Union[str, None] = None):
        self._username = username
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._authentication_database = authentication_database
        self._sorting = sorting
        self._log_file = None
        self._request = Request(_HEADERS)
        self._db = None
        self._book_buffer = None
        self._start_book_page = self._end_book_page = self._start_book = self._end_book = None
        self._current_book_page = self._current_book = None
        self._pages_count = None
        self._books_ids = None
        self._current_page_link = None
        self._current_page_number = None
        self._file_with_href = None

        self._check_fields()

    @property
    def current_book_page(self):
        return self._current_book_page

    @property
    def current_book(self):
        return self._current_book

    def _check_fields(self):
        field_types = {
            '_username': [str, type(None)],
            '_password': [str, type(None)],
            '_database': [str],
            '_host': [str],
            '_port': [int, str],
            '_authentication_database': [str, type(None)],
            '_sorting': [str, type(None)]
        }
        for field, types in field_types.items():
            value = getattr(self, field)
            if not any(isinstance(value, type_) for type_ in types):
                raise TypeError(f"Field '{field[1:]}' must have type{'s' if len(types) > 1 else ''} "
                                f"{', '.join(map(lambda type_: type_.__name__, types[:-1]))}"
                                f"{' or ' if len(types) > 1 else ''}{types[-1].__name__}, not {type(value).__name__}")
        if self._username is not None and self._password is None:
            raise TypeError(f"Field 'password' must have type str, not NoneType")

    def _init_database(self, is_clear_database):
        uri = get_uri_mongodb(self._database, self._username, self._password, self._host, self._port,
                              self._authentication_database)
        client = MongoClient(uri)
        self._db = client.get_database()

        try:
            collections = self._db.collection_names()
            if 'books' in collections and is_clear_database:
                self._db.books.delete_many({})
            elif 'books' not in collections:
                self._db.create_collection('books')
        except Exception:
            raise DBConnectionError('collection initialization failed') from None
        try:
            self._book_links = set()
            if not is_clear_database and 'books' in collections:
                self._book_links = set(book['key'] for book in self._db.books.find())
        except Exception:
            raise DBConnectionError('data from database initialization failed') from None
        self._book_buffer = InsertBuffer(self._db.books, _BUFFER_SIZE, self._update_log)

    def _make_request(self, url):
        response = self._request.get(url)
        if response.status_code == HTTPStatus.OK:
            content = response.content.decode(response.encoding)
            if 'captcha' in content:
                raise CaptchaError
            page = fromstring(content)
            return page
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self._update_log(f'Page {url} not found')
            return None
        else:
            raise Exception(f'Unknown error: {response.status_code}; {response.text}')

    def _get_book_links(self):
        def check_exists_captcha(driver):
            try:
                driver.find_element(By.CLASS_NAME, "litres_captcha_page")
                return True
            except NoSuchElementException:
                return False

        with open(_CHECKPOINT_FILE_NAME, 'r') as checkpoint_file:
            self._current_page_link = checkpoint_file.readline().strip()
            self._current_page_number = int(checkpoint_file.readline())

        driver = webdriver.Firefox()

        while True:
            try:
                driver.get(self._current_page_link)
                if check_exists_captcha(driver):
                    print("Captcha! Waiting for manual handling")
                    time.sleep(120)
                    print('Captcha managed')
                    driver.get(self._current_page_link)

                content_books_names = driver.find_elements(By.CLASS_NAME, "art__name")  # названия книги
                time.sleep(1)

                for book in content_books_names:
                    element = book.find_element(By.TAG_NAME, "a")
                    yield element.get_attribute("href")
                time.sleep(1)
            except NoSuchElementException:
                print('Go to the next page...')

            self._current_page_number += 1

            self._current_page_link = f"https://www.litres.ru/novie/elektronnie-knigi/page-" \
                                      f"{self._current_page_number}/?lang=52"

    def _get_book_links_from_file(self, input_file):
        if isinstance(input_file, str):
            try:
                self._file_with_href = open(input_file, 'r')
                return (line.strip() for line in self._file_with_href)
            except FileNotFoundError:
                print(f'file with name {input_file} not found')
        return (line for line in input_file)

    def _get_book(self, book_key):
        #book_data = self._make_request(f'{BASE_URL}{book_key}')
        book_data = requests.get(f'{BASE_URL}{book_key}')
        if book_data is None:
            self._update_log(f"Can't find information about book {book_key}")
            return None

        soup = BeautifulSoup(book_data.text.encode('utf-8'), features="html.parser")

        book_dict = {"key": book_key}

        author = soup.find('a', class_='biblio_book_author__link')
        book_dict.update({} if author is None else {'author': author.text})

        book_name = soup.find('h1', itemprop='name')
        book_dict.update({} if book_name is None else {'book_name': book_name.text})

        content_mark = soup.find('div', class_='art-rating-unit rating-source-litres rating-popup-launcher')
        average_rating = content_mark.find('div', class_='rating-number bottomline-rating') \
            if content_mark is not None else None
        book_dict.update({} if average_rating is None else {'average_rating_litres': float(average_rating.text
                                                                                           .replace(',', '.'))})
        votes_count = content_mark.find('div', class_='votes-count bottomline-rating-count') \
            if content_mark is not None else None
        book_dict.update({} if votes_count is None else {'votes_count_litres': int(votes_count.text)})

        content_mark = soup.find('div', class_='art-rating-unit rating-source-livelib rating-popup-launcher')
        average_rating = content_mark.find('div', class_='rating-number bottomline-rating') \
            if content_mark is not None else None
        book_dict.update(
            {} if average_rating is None else {'average_rating_livelib': float(average_rating.text.replace(',', '.'))})

        votes_count = content_mark.find('div', class_='votes-count bottomline-rating-count') \
            if content_mark is not None else None
        book_dict.update({} if votes_count is None else {'votes_count_livelib': int(votes_count.text)})

        recenses = soup.find('div', class_='recenses-count')
        recenses_count = recenses.find('div', class_='rating-text-wrapper') \
            if recenses is not None else None
        book_dict.update({} if recenses_count is None else {'recenses_count': int(recenses_count.text)})

        subscr = soup.find('div', class_='get_book_by_subscr')
        book_dict.update({} if subscr is None else {'subscr_price': 'Не указана'} if (
                    len(subscr.text) < len('Взять по абонементу за')) else {
            'subscr_price': int(re.sub(r'Взять по абонементу за', '', subscr.text).strip()[:-2])})

        buy = soup.find('div', class_ = 'biblio_book_buy_block')
        buy_price = buy.find('span', class_='simple-price') \
            if buy is not None else None
        book_dict.update({} if buy_price is None else {
            'buy_price': int(re.sub('Купить и скачать за', '', buy_price.text).strip()[:-2])})

        audio = soup.find('span', class_ = 'type type_audio')
        audio_price = audio.find('span', class_='simple-price') \
            if audio is not None else None
        book_dict.update({} if audio_price is None else {
            'audio_price': int(re.sub('Цена аудиокниги', '', audio_price.text).strip()[:-2])})

        paper = soup.find('span', class_ = 'type type_hardcopy')
        paper_price = paper.find('span', class_='simple-price') \
            if paper is not None else None
        book_dict.update({} if paper_price is None else {
            'paper_price': int(re.sub('Цена бумажной версии', '', paper_price.text).strip()[:-2])})

        volume_info = soup.find('li', class_ = 'volume')
        volume = re.search(r'Объем:(.+?)стр', volume_info.text).group(1) \
            if volume_info is not None else None
        book_dict.update({} if volume is None else {'volume': int(volume.strip())})

        genre_info = soup.find('div', class_ = 'ab-container breadcrumbs-container')
        book_dict.update({} if genre_info is None else {
            'genre': str(genre_info.find_all('li', {'class': 'breadcrumbs__item'})[1].text)})

        award = soup.find('a', class_ = 'badge flag_best')
        award_bestseller = award.find('span', class_='flag_text') \
            if award is not None else None
        book_dict.update({} if award_bestseller is None else {'award_bestseller': award_bestseller.text})

        award = soup.find('a', class_ = 'badge flag_hit')
        award_hit = award.find('span', class_='flag_text') \
            if award is not None else None
        book_dict.update({} if award_hit is None else {'award_hit': award_hit.text})


        self._update_log('book was got')
        return book_dict

    def _update_log(self, log_message):
        if self._log_file is not None:
            print(log_message, file=self._log_file, flush=True)

    def _process_db_connection_error(self, buffer_size=_BUFFER_SIZE):
        # does not take into account unexpected repetitions and skips of books
        successful_books = BOOKS_PER_PAGE * (self._current_book_page - self._start_book_page) - self._start_book + 1 \
                           + self._current_book - buffer_size
        previous_books = BOOKS_PER_PAGE * (self._start_book_page - 1) + self._start_book - 1
        all_books = successful_books + previous_books
        last_successful_page = all_books // BOOKS_PER_PAGE
        last_successful_book = all_books % BOOKS_PER_PAGE
        if successful_books == 0:
            self._update_log('No successful inserts in database')
        elif last_successful_book > 0:
            self._update_log(f'Last successful insert in database: page {last_successful_page + 1}, '
                             f'book {last_successful_book} ({successful_books} books)')
        else:
            self._update_log(f'Last successful insert in database: page {last_successful_page}, book {BOOKS_PER_PAGE} '
                             f'({successful_books} books)')

    def _flush_buffer(self):
        try:
            self._book_buffer.flush()
        except DBConnectionError as exc:
            self._process_db_connection_error(len(self._book_buffer))
            raise exc from None

    def _close_log_file(self):
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    def _collect(self, start_book_page, end_book_page, start_book, end_book, is_from_file):
        self._start_book_page = start_book_page
        self._end_book_page = end_book_page
        self._start_book = start_book
        self._end_book = end_book

        generator = self._get_book_links_from_file(_FILE_WITH_HREF_NAME) if is_from_file else self._get_book_links()

        try:
            for book_link in generator:
                book_key = book_link.replace(BASE_URL, '')
                if book_key in self._book_links:
                    self._update_log(f'book {book_key} has been already gotten')
                    continue

                book_data = self._get_book(book_key)
                if book_data is None:
                    continue

                self._book_buffer.add(book_data)
                self._book_links.add(book_link)
            if self._file_with_href is not None:
                self._file_with_href.close()
        except DBConnectionError as exc:
            self._process_db_connection_error()
            if self._file_with_href is not None:
                self._file_with_href.close()
            raise exc from None
        except (ConnectionError, ValueError, CaptchaError, KeyboardInterrupt, Exception) as exc:
            self._flush_buffer()
            if self._file_with_href is not None:
                self._file_with_href.close()
            raise exc from None

        self._flush_buffer()

    def collect(self, start_book_page: int = 1, end_book_page: Union[int, None] = None, start_book: int = 1,
                end_book: int = BOOKS_PER_PAGE, is_clear_database: bool = True, log_file_path: Union[str, None] = None,
                is_from_file: bool = True):
        if log_file_path is not None:
            self._log_file = open(log_file_path, 'w')
        else:
            self._log_file = None

        self._init_database(is_clear_database)

        start_book_page = max(start_book_page, 1)
        end_book_page = end_book_page
        start_book = max(start_book, 1)
        end_book = min(max(end_book, 1), BOOKS_PER_PAGE)
        if end_book_page is not None and start_book_page > end_book_page:
            self._close_log_file()
            return

        try:
            self._collect(start_book_page, end_book_page, start_book, end_book, is_from_file)
            self._close_log_file()
        except DBConnectionError:
            self._close_log_file()
            raise
        except (ConnectionError, ValueError, CaptchaError, KeyboardInterrupt, Exception):
            self._close_log_file()
            raise
