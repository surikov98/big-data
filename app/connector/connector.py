import json
import os
import re
import requests
import time
from flask import current_app
from bs4 import BeautifulSoup
from itertools import islice
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from typing import Union
from tqdm import tqdm

from .anticaptcha import process_captcha
from .errors import CaptchaError, ConnectionError, DBConnectionError
from .insert_buffer import InsertBuffer


class Connector:
    def __init__(self, sorting: Union[str, None] = None):
        self._sorting = sorting
        self._log_file = None
        self._db = current_app.db
        self._book_buffer = None
        self._start_book_page = self._end_book_page = None
        self._start_book_index = None
        self._book_links = None
        self._current_page_link = None
        self._current_page_number = None
        self._file_with_href = None
        self._current_book_index = 0
        self.buffer_size = current_app.config['BOOKS_PER_PAGE']

    @property
    def current_book(self):
        return self._current_book

    def _init_database(self, is_clear_database):
        try:
            collections = self._db.collection_names()
            if 'books' in collections and is_clear_database:
                self._db.books.drop()
            elif 'books' not in collections:
                self._db.create_collection('books')
        except Exception as e:
            raise DBConnectionError('collection initialization failed') from None
        try:
            self._book_links = set()
            if not is_clear_database and 'books' in collections:
                self._book_links = set(book['key'] for book in self._db.books.find())
        except Exception as e:
            raise DBConnectionError('data from database initialization failed') from None
        self._book_buffer = InsertBuffer(self._db.books, self.buffer_size, self._update_log)

    def __check_exists_captcha(self, driver):
        try:
            driver.find_element(By.CLASS_NAME, "litres_captcha_page")
            return True
        except NoSuchElementException:
            return False

    def _get_book_links(self, get_new_electronic_russian_book=True, server_side=False):
        try:
            with open(current_app.config['HREF_CHECKPOINT'], 'r') as checkpoint_file:
                self._current_page_link = checkpoint_file.readline().strip()
                self._current_page_number = int(checkpoint_file.readline())
        except Exception:
            pass
        if self._start_book_page:
            self._current_page_number = self._start_book_page

        options = webdriver.FirefoxOptions()
        if server_side:
            options.add_argument('--headless')

        driver = webdriver.Firefox(executable_path='./assets/geckodriver', service_log_path='./logs/geckodriver.log',
                                   options=options)

        while True:
            try:
                driver.get(self._current_page_link)
                if self.__check_exists_captcha(driver):
                    if process_captcha(driver):
                        print("Captcha is done")
                    else:
                        print("Failed to resolve captcha. Waiting for manual handling")
                        while self.__check_exists_captcha(driver):
                            time.sleep(1)
                    print('Captcha managed')

                time.sleep(1)
                content_books_names = driver.find_elements(By.CLASS_NAME, "art__name")  # названия книги

                for book in content_books_names:
                    try:
                        element = book.find_element(By.TAG_NAME, "a")
                    except Exception as e:
                        continue
                    yield element.get_attribute("href")
                time.sleep(1)
            except NoSuchElementException:
                print('Go to the next page...')

            self._current_page_number += 1
            if self._end_book_page and self._current_page_number >= self._end_book_page:
                break

            if get_new_electronic_russian_book:
                self._current_page_link = f"https://www.litres.ru/novie/elektronnie-knigi" \
                                          f"/page-{self._current_page_number}/?lang=52"
            else:
                self._current_page_link = f"https://www.litres.ru/kollekcii-knig/besplatnie-knigi/elektronnie-knigi" \
                                          f"/page-{self._current_page_number}/?lang=52"

    def _get_book_links_from_file(self, input_file):
        if isinstance(input_file, str):
            input_file = open(input_file, 'r')
        return islice(input_file, self._current_book_index, None)

    @staticmethod
    def _get_price_from_string(s, prefix=None):
        if not prefix:
            return float(s.strip().replace(',', '.'))
        return float(re.sub(prefix, '', s).strip().replace(',', '.')[:-2].replace(' ', ''))

    def _get_book(self, book_key):
        book_data = requests.get(f'{current_app.config["BASE_URL"]}{book_key}')
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
        book_dict.update({} if average_rating is None else {'average_rating_litres':
                                                            self._get_price_from_string(average_rating.text)})

        votes_count = content_mark.find('div', class_='votes-count bottomline-rating-count') \
            if content_mark is not None else None
        book_dict.update({} if votes_count is None else {'votes_count_litres': int(re.sub(' ', '', votes_count.text))})

        content_mark = soup.find('div', class_='art-rating-unit rating-source-livelib rating-popup-launcher')
        average_rating = content_mark.find('div', class_='rating-number bottomline-rating') \
            if content_mark is not None else None
        book_dict.update(
            {} if average_rating is None else {'average_rating_livelib':
                                               self._get_price_from_string(average_rating.text)})

        votes_count = content_mark.find('div', class_='votes-count bottomline-rating-count') \
            if content_mark is not None else None
        book_dict.update({} if votes_count is None else {'votes_count_livelib': int(re.sub(' ', '', votes_count.text))})

        reviews = soup.find('div', class_='recenses-count')
        reviews_count = reviews.find('div', class_='rating-text-wrapper') if reviews is not None else None
        book_dict.update({} if reviews_count is None else {'reviews_count': int(reviews_count.text)})

        subscr = soup.find('div', class_='get_book_by_subscr')
        book_dict.update({} if subscr is None
                         else {'subscr_price': self._get_price_from_string(subscr.text, 'Взять по абонементу за')}
                         if len(subscr.text) >= len('Взять по абонементу за') else {'subscr_price': 0})

        buy = soup.find('div', class_='biblio_book_buy_block')
        buy_price = buy.find('span', class_='simple-price') \
            if buy is not None else None
        book_dict.update({} if buy_price is None
                         else {'buy_price': self._get_price_from_string(buy_price.text, 'Купить и скачать за')})

        audio = soup.find('span', class_='type type_audio')
        audio_price = audio.find('span', class_='simple-price') \
            if audio is not None else None
        book_dict.update({} if audio_price is None
                         else {'audio_price': self._get_price_from_string(audio_price.text, 'Цена аудиокниги')})

        paper = soup.find('span', class_='type type_hardcopy')
        paper_price = paper.find('span', class_='simple-price') \
            if paper is not None else None
        book_dict.update({} if paper_price is None
                         else {'paper_price': self._get_price_from_string(paper_price.text, 'Цена бумажной версии')})

        volume_info = soup.find('li', class_='volume')
        volume_ask = re.search(r'Объем:(.+?)стр', volume_info.text) \
            if volume_info is not None else None
        volume = volume_ask.group(1) \
            if volume_ask is not None else None
        book_dict.update({} if volume is None else {'volume': int(volume.strip())})

        genre_info = soup.find('div', class_='ab-container breadcrumbs-container')
        book_dict.update({} if genre_info is None
                         else {'genre': str(genre_info.find_all('li', {'class': 'breadcrumbs__item'})[1].text)})

        award = soup.find('a', class_='badge flag_best')
        award_bestseller = award.find('span', class_='flag_text') \
            if award is not None else None
        book_dict.update({} if award_bestseller is None else {'award_bestseller': award_bestseller.text})

        award = soup.find('a', class_='badge flag_hit')
        award_hit = award.find('span', class_='flag_text') \
            if award is not None else None
        book_dict.update({} if award_hit is None else {'award_hit': award_hit.text})

        mark = soup.find('a', class_='badge flag_free')
        mark_free = mark.find('span', class_='flag_text') \
            if mark is not None else None
        book_dict.update({} if mark_free is None else {'mark_free': mark_free.text})

        mark = soup.find('div', class_='biblio_book_text_preorder_info')
        book_dict.update({} if mark is None else {'mark_not_available': mark.text})

        cit = soup.find('span', class_='quotes__count')
        book_dict.update({} if cit is None else {'citations': int(cit.text.strip())})

        annotation = soup.find('div', class_='biblio_book_annotation')
        book_dict.update({} if annotation is None
                         else {'annotation': annotation.text.replace('\xa0', '\x20')
                                                            .replace('Аннотация от ЛитРес', '')})

        description = soup.find('div', class_='biblio_book_descr_publishers')
        book_dict.update({} if description is None else {'description': description.text.replace('\xa0', '\x20')})

        blocks = soup.find('div', class_='biblio_book_info_detailed')
        part1 = blocks.find('ul', class_='biblio_book_info_detailed_left') if blocks is not None else None
        elements_left = part1.find_all('li') if part1 is not None else []
        part2 = blocks.find('ul', class_='biblio_book_info_detailed_right') if blocks is not None else None
        elements_right = part2.find_all('li') if part2 is not None else []
        elements = elements_left + elements_right

        for elem in elements:
            if elem.text.startswith('Возрастное ограничение'):
                age_name = elem.text
                book_dict.update({} if age_name is None
                                 else {'age': int(re.sub('\+', '',
                                                         re.sub('Возрастное ограничение:', '', age_name)).strip())})
            elif elem.text.startswith('Дата выхода на ЛитРес'):
                date = elem.text
                book_dict.update({} if date is None
                                 else {'date_litres': int(re.sub('Дата выхода на ЛитРес:', '', date).strip()[-4:])})
            elif elem.text.startswith('Дата написания'):
                try:
                    date = elem.text
                    date = re.sub('Дата написания:', '', date).strip()
                    date = re.sub('г.', '', date).strip()
                    book_dict.update({} if date is None
                                     else {'date_writing': int(date[:2] + date[-2:])}
                                     if 5 < len(date) < 8 else {'date_writing': int(date[-4:])})
                except Exception:
                    pass
            elif elem.text.startswith('Дата перевода'):
                try:
                    date = elem.text
                    date = re.sub('Дата перевода:', '', date).strip()
                    date = re.sub('г.', '', date).strip()
                    book_dict.update({} if date is None
                                     else {'translate': int(date[:2] + date[-2:])}
                                     if 5 < len(date) < 8 else {'translate': int(date[-4:])})
                except Exception:
                    pass
            elif elem.text.startswith('Переводчик'):
                translator = elem.text
                book_dict.update({} if translator is None
                                 else {'translator': re.sub('Переводчик:', '', translator).strip()})
            elif elem.text.startswith('ISBN'):
                isbn = elem.text
                book_dict.update({} if isbn is None else {'isbn': re.sub('ISBN:', '', isbn).strip()})
            elif elem.text.startswith('Правообладатель'):
                rights = elem.text
                book_dict.update({} if rights is None else {'rights': re.sub('Правообладатель:', '', rights).strip()})
            elif elem.text.startswith('Общий размер'):
                weight = elem.text
                book_dict.update({} if weight is None
                                 else {'weight': int(re.sub('Общий размер:', '', weight).strip()[:-3])})
            elif elem.text.startswith('Размер страницы'):
                page_size = elem.text
                book_dict.update({} if page_size is None
                                 else {'page_size': re.sub('Размер страницы:', '', page_size).strip()})

        genres_info = soup.find('div', class_='biblio_book_info')
        genres_list = genres_info.find_all('li') if genres_info is not None else None

        if genres_list is not None:
            for elem in genres_list:
                if elem.text.startswith('Жанр'):
                    genres_all = elem.text
                    genres_all = list(map(lambda s: s.strip(), re.sub('Жанр:', '', genres_all).split(',')))
                    genres_all[len(genres_all) - 1] = re.sub('Редактировать', '', genres_all[len(genres_all) - 1])
                    book_dict.update({} if genres_all is None else {'genres_all': genres_all})
                elif elem.text.startswith('Теги'):
                    tags_all = elem.text
                    tags_all = list(map(lambda s: s.strip(), re.sub('Теги:', '', tags_all).split(',')))
                    tags_all[len(tags_all) - 1] = re.sub('Редактировать', '', tags_all[len(tags_all) - 1])
                    book_dict.update({} if tags_all is None else {'tags_all': tags_all})

        self._update_log('book was got')
        return book_dict

    def _update_log(self, log_message):
        if self._log_file is not None:
            print(log_message, file=self._log_file, flush=True)

    def _process_db_connection_error(self, buffer_size=None):
        if buffer_size is None:
            buffer_size = self.buffer_size
        # does not take into account unexpected repetitions and skips of books
        successful_books = buffer_size * (self._current_page_number - self._start_book_page - 1)
        if successful_books == 0:
            self._update_log('No successful inserts in database')
        elif self._current_page_number > 0:
            self._update_log(f'Last successful insert in database: page {self._current_page_number + 1}')

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

    def _collect(self, start_book_page, end_book_page, source_file, server_side):
        self._start_book_page = start_book_page
        self._end_book_page = end_book_page

        generator = self._get_book_links_from_file(source_file) if source_file else \
            self._get_book_links(server_side=server_side)

        try:
            for book_link in generator:
                if server_side and not os.path.exists(current_app.config['LOCK_NAME']):
                    raise KeyboardInterrupt("Aborted")

                book_link = book_link.strip()
                book_key = book_link.replace(current_app.config['BASE_URL'], '')
                print(f"{self._current_book_index}: {book_link}", end='\r')
                self._current_book_index += 1
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
        except (ConnectionError, ValueError, KeyboardInterrupt, Exception) as exc:
            self._flush_buffer()
            if self._file_with_href is not None:
                self._file_with_href.close()
            raise exc from None

        self._flush_buffer()

    def collect(self, start_book_page: int = 1, end_book_page: Union[int, None] = None, start_book_index: int = -1,
                is_clear_database: bool = True, log_file_path: Union[str, None] = None,
                source_file: Union[str, None] = None, server_side: bool = False):
        if log_file_path is not None:
            self._log_file = open(log_file_path, 'w')
        else:
            self._log_file = None

        self._init_database(is_clear_database)

        start_book_page = max(start_book_page, 1)
        end_book_page = end_book_page
        if end_book_page is not None and start_book_page > end_book_page:
            self._close_log_file()
            return

        if source_file and os.path.exists(current_app.config['DB_CHECKPOINT']):
            with open(current_app.config['DB_CHECKPOINT'], 'r') as checkpoint_file:
                try:
                    self._current_book_index = int(checkpoint_file.readline())
                except Exception:
                    pass

        if start_book_index >= 0:
            self._current_book_index = start_book_index

        try:
            self._collect(start_book_page, end_book_page, source_file, server_side)
            self._close_log_file()
        except DBConnectionError:
            self._close_log_file()
            raise
        except (ConnectionError, ValueError, CaptchaError, KeyboardInterrupt, Exception):
            with open(current_app.config['DB_CHECKPOINT'], 'w') as checkpoint_file:
                checkpoint_file.write(str(self._current_book_index))
            self._close_log_file()
            raise

    def __authorize_by_email(self, session: webdriver.Firefox, email, password):
        time.sleep(3)
        login_tab = session.find_element(By.CLASS_NAME, 'Login-module__loginLink')
        session.execute_script('arguments[0].scrollIntoView(true);', login_tab)
        session.execute_script('arguments[0].click();', login_tab)
        time.sleep(3)
        email_and_phone_tabs = session.find_element(By.CLASS_NAME, 'AuthorizationPopup-module__step__block')
        email_tab = email_and_phone_tabs.find_element(By.XPATH,
                                                      '/html/body/div[1]/div[1]/header/div[2]/div[2]/div[2]/div/div/div'
                                                      '/div/div[1]/div[3]/button[1]')
        session.execute_script('arguments[0].scrollIntoView(true);', email_tab)
        session.execute_script('arguments[0].click();', email_tab)
        time.sleep(3)
        email_input = session.find_element(By.CLASS_NAME, 'AuthorizationPopup-module__input')
        email_input.clear()
        email_input.send_keys(email)
        continue_tab = session.find_element(By.XPATH,
                                            '/html/body/div[1]/div[1]/header/div[2]/div[2]/div[2]/div/div/div/div[1]'
                                            '/div/form/div[2]/button')
        session.execute_script('arguments[0].scrollIntoView(true);', continue_tab)
        session.execute_script('arguments[0].click();', continue_tab)
        time.sleep(3)
        pwd_input = session.find_element(By.CLASS_NAME, 'AuthorizationPopup-module__input')
        pwd_input.clear()
        pwd_input.send_keys(password)
        authorize_tab = session.find_element(By.XPATH, '/html/body/div[1]/div[1]/header/div[2]/div[2]/div[2]/div/div'
                                                       '/div/div[1]/div/form/div[3]/button')
        session.execute_script('arguments[0].scrollIntoView(true);', authorize_tab)
        session.execute_script('arguments[0].click();', authorize_tab)
        time.sleep(7)
        try:
            close_reserve_tab = session.find_element(By.XPATH, '/html/body/div[1]/div[1]/header/div[2]/div[2]/div[2]'
                                                               '/div/div/div/a')
            session.execute_script('arguments[0].scrollIntoView(true);', close_reserve_tab)
            session.execute_script('arguments[0].click();', close_reserve_tab)
        except NoSuchElementException:
            print('Not found reserve authorization methods')

    def get_books_text(self, source_file: str = None):
        generator = self._get_book_links_from_file(source_file) if source_file \
            else self._get_book_links(False)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 'text/plain')  # need than don't open dialog window for download TXT
        browser = webdriver.Firefox(profile)

        browser.get(current_app.config['BASE_URL'])
        self.__authorize_by_email(browser, 'some_email@mail.ru', 'password')

        for book_link in generator:
            try:
                browser.get(book_link)
                if self.__check_exists_captcha(browser):
                    print("Captcha! Waiting for manual handling")
                    time.sleep(120)
                    print('Captcha managed')
                    browser.get(book_link)
                download_button = browser.find_element(By.CLASS_NAME, 'bb_newbutton_download')
                open_format_list = download_button.find_element(By.CLASS_NAME, 'bb_newbutton_caption')
                browser.execute_script('arguments[0].scrollIntoView(true);', open_format_list)
                browser.execute_script('arguments[0].click();', open_format_list)
                download_txt_section = browser.find_element(By.CLASS_NAME, 'format_txt')
                download_href = download_txt_section.find_element(By.CLASS_NAME, 'biblio_book_download_file__link')
                browser.execute_script('arguments[0].scrollIntoView(true);', download_href)
                browser.execute_script('arguments[0].click();', download_href)
            except NoSuchElementException:
                print('Go to the next href...')

    def _get_book_data_from_file(self, file_object):
        file_object.seek(0)
        if file_object.readline() != '[\n':
            raise Exception('Incorrect file')
        count_str = file_object.readline()
        if count_str[-1] != '\n':
            raise Exception('Incorrect file')
        if count_str[-2] != ',':
            return
        books_count = json.loads(count_str[:-2])['count']
        self._current_book = 0
        with tqdm(ascii=True, total=books_count) as bar:
            while True:
                s = file_object.readline()
                if s[-1] == ']':
                    if len(s) != 1:
                        raise Exception('Incorrect file')
                    break
                else:
                    if s[-1] != '\n':
                        raise Exception('Incorrect file')
                    s = s[:-1]
                    if s[-1] == ',':
                        s = s[:-1]

                book_data = json.loads(s)
                self._current_book += 1
                self._update_log(f"book: {self._current_book}/{books_count}; bookLink: {book_data['key']}")
                yield book_data
                bar.update()

    def _connect_from_file(self, file_object):
        try:
            for book_data in self._get_book_data_from_file(file_object):
                book_link = book_data['key']
                if book_link in self._book_links:
                    self._update_log(f'book {book_link} has been already gotten')
                    continue

                self._book_buffer.add(book_data)
                self._book_links.add(book_link)
        except DBConnectionError as exc:
            # does not take into account unexpected repetitions of books
            if self._current_book == 0:
                self._update_log('No successful inserts in database')
            else:
                successful_books = (self._current_book // self.buffer_size) * self.buffer_size
                self._update_log(f'{successful_books} successful books in database')
            raise exc from None
        except (KeyboardInterrupt, Exception) as exc:
            self._flush_buffer()
            raise exc from None

        self._flush_buffer()

    def connect_from_file(self, filename: str, is_clear_database: bool = True):
        self._log_file = open('./logs/connect_from_file.log', 'w')
        self._init_database(is_clear_database)

        if isinstance(filename, str):
            file_object = open(filename, 'r', encoding='utf-8')
        else:
            file_object = filename

        try:
            self._connect_from_file(file_object)
            self._close_log_file()
            file_object.close()
        except (DBConnectionError, KeyboardInterrupt, Exception):
            self._close_log_file()
            file_object.close()
            raise
