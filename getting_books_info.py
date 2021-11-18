import json
import time
import requests
from bs4 import BeautifulSoup
import re

count_links = 1

#ref = "https://www.litres.ru/mitio-kaku/uravnenie-boga-v-poiskah-teorii-vsego/"
ref = "https://www.litres.ru/sergey-lukyanenko/sem-dney-do-megiddo/"
#ref = "https://www.litres.ru/mark-levi/sumerki-hischnikov/"
#ref = "https://www.litres.ru/raznoe/zhurnal-ministerstva-narodnogo-prosvescheniya-tom-318/"
#ref = "https://www.litres.ru/adrian-lorey/carstvo-bezzakoniya/"

json_count = 2
books_list = []

#with open(f'books_info_{json_count}.json', "r") as file:  # if want write in once json
#    try:
#        content = json.load(file)
#        print(content)
#    except:  # if json is empty
#        content = []

while count_links < 2:
    # Эта часть - загрузка информации с персональной страницы книги
    try:
        # перешли по персональной ссылке
        html_code = requests.get(ref)
        soup = BeautifulSoup(html_code.text.encode('utf-8'), features="html.parser")

        book_dict = {}

        print("Ключ:", ref.replace('https://www.litres.ru', ''))
        # content_name = driver.find_element(By.CLASS_NAME, "biblio_book_name biblio-book__title-block")
        # print(content_name.text)
        book_dict['key'] = ref

        try:
            author = soup.find('a', {'class': 'biblio_book_author__link'})
            print("Автор:", author.text)
            book_dict['author'] = author.text
        except:
            print("Автор:", None)
            book_dict['author'] = None
    
        try:
            book_name = soup.find('h1', {'itemprop': 'name'})
            print("Название:", book_name.text)
            book_dict['book_name'] = book_name.text
        except:
            print("Название:", None)
            book_dict['book_name'] = None

        # считываем информацию об оценках
        try:
            content_mark = soup.find('div', {'class': 'art-rating-unit rating-source-litres rating-popup-launcher'})
            average_rating = content_mark.find('div', {'class': 'rating-number bottomline-rating'}).text
            print("Средняя оценка ЛитРес:", average_rating)
            book_dict['average_rating_litres'] = float(average_rating.replace(',', '.'))   
        except:
            print("Средняя оценка ЛитРес:", None)
            book_dict['average_rating_litres'] = None
            
        try:
            votes_count = content_mark.find('div', {'class': 'votes-count bottomline-rating-count'}).text
            print("Количество оценок ЛитРес:", votes_count)  
            book_dict['votes_count_litres'] = int(votes_count)
        except:
            print("Количество оценок ЛитРес:", None)
            book_dict['votes_count_litres'] = None

        try:
            subscr = soup.find('div', {'class': 'get_book_by_subscr'})
            #print(subscr.text)
            subscr_price = re.sub('Взять по абонементу за', '', subscr.text)
            subscr_price = subscr_price.strip()[:-2]
            print('Взять по абонементу за', subscr_price)
            book_dict['subscr_price'] = int(subscr_price)
        except:
            print('Взять по абонементу за',None)
            book_dict['subscr_price'] = None
        
        try:
            buy = soup.find('div', {'class': 'biblio_book_buy_block'})
            buy_price = buy.find('span', {'class': 'simple-price'})
            buy_price_rub = buy_price.text.strip()[:-2]
            print("Купить и скачать за", buy_price_rub)
            book_dict['buy_price'] = buy_price_rub
        except:
            print("Купить и скачать за", None)
            book_dict['buy_price'] = None
            
        try:
            audio = soup.find('span', {'class': 'type type_audio'})           
            price = audio.find('span', {'class': 'simple-price'})
            audio_price = price.text.strip()[:-2]
            print("Цена аудиокниги", audio_price)
            book_dict['audio_price'] = audio_price
        except:
            print("Цена аудиокниги", None)
            book_dict['audio_price'] = None
            
        try:
            paper = soup.find('span', {'class': 'type type_hardcopy'})           
            price = paper.find('span', {'class': 'simple-price'})
            paper_price = price.text.strip()[:-2]
            print("Цена бумажной версии", paper_price)
            book_dict['paper_price'] = paper_price
        except:
            print("Цена бумажной версии", None)
            book_dict['paper_price'] = None   
            
        try:
            recenses = soup.find('div', {'class': 'recenses-count'})
            recenses_count = recenses.find('div', {'class': 'rating-text-wrapper'}).text
            print("Количество отзывов:", recenses_count)
            book_dict['recenses_count'] = int(recenses_count)
        except:
            print("Количество отзывов:", None)
            book_dict['recenses_count'] = None
        
        try:
            volume_info = soup.find('li', {'class': 'volume'})
            volume = re.search(r'Объем:(.+?)стр',volume_info.text).group(1)
            print("Количество страниц:", volume)
            book_dict['volume'] = int(volume.strip())
        except:
            print("Количество страниц:", None)
            book_dict['volume'] = None
        
        try:
            genre_info = soup.find_all('a', {'class': 'biblio_info__link'})
            genre = ""
            for g in genre_info:
                genre = genre + ', ' + g.text
            genre = genre[2:]
            print("Жанр:", genre)
            book_dict['genre'] = genre
        except:
            print("Жанр:", None)
            book_dict['genre'] = None
            
        try:
            award = soup.find('a', {'class': 'badge flag_best'})
            award_sale = award.find('span', {'class': 'flag_text'})
            print(award_sale.text)
            book_dict['award_sale'] = award_sale.text.split()
        except:
            print("Не бестселлер", None)
            book_dict['award_sale'] = None
            
        try:
            award2 = soup.find('a', {'class': 'badge flag_hit'})
            award_hit = award2.find('span', {'class': 'flag_text'})
            print(award_hit.text)
            book_dict['award_hit'] = award_hit.text
        except:
            print("Не хит", None)
            book_dict['award_hit'] = None
            
        #try:
            #cit = soup.find('nav', {'class': 'book-tabs'})
            #print(cit.text)
            #citation = cit.find('li',{'data-name': 'citaty'})
            #cit_count = citation.find('span', {'class': 'count'})
            #print(cit_count.text)
        #except:
            #print("Error!")
            
        try:    
            blocks = soup.find('div', {'class': 'biblio_book_info_detailed'})
            #bloks_left = blocks.find('ul', 'biblio_book_info_detailed_left')
            #for b in blocks:
                #print(b.text)
            part = blocks.find('ul', {'class': 'biblio_book_info_detailed_left'})
            elements = part.find_all('li')
            for el in elements:
                print(el.text)
        except:
            print('Error!')
            
           
    except:
        print("Error reading personal book info. Go to the next book...")
    finally:
        #         здесь должен быть переход к следующей книге
        books_list.append(book_dict)
        count_links += 1

with open(f'books_info_{json_count}.json', "w") as file:
    json.dump(books_list, file, indent=2)

print()
with open(f'books_info_{json_count}.json', "r") as file:
    content = json.load(file)
    print(content)

#         mark = content_mark.text.split('\n')
#         print("Средняя оценка:", mark[0])
#         print("Количество оценок:", mark[1])
#
#         content_price = driver.find_element(By.CLASS_NAME, "simple-price")
#         print("Цена:", content_price.text)
#
#         # хотим получить информацию о жанре
#         content_info = content_mark_number = driver.find_element(By.CLASS_NAME, "biblio_book_info")
#         content_info_details = content_info.find_elements(By.TAG_NAME, "li")
#         for i in range(len(content_info_details)):
#             print(content_info_details[i].text)
#
#         # считываем сначала информацию из левого стобца (так проще) - класс biblio_book_info_detailed_left;
#         # как правило, это: возрастное ограничение, дата выхода на литрес, (иногда, но не всегда) дата перевода, объем
#         content_details_left = driver.find_element(By.CLASS_NAME, "biblio_book_info_detailed_left")
#         # вся считанную из столбца информацию можно разбить на элементы с помощью тега li, так мы получаем список
#         content_parts_left = content_details_left.find_elements(By.TAG_NAME, "li")
#         for i in range(len(content_parts_left)):
#             print(content_parts_left[i].text)
#
#             # теперь считываем информацию из правого стобца - класс biblio_book_info_detailed_right;
#         # как правило, это: ISBN (здесь сидит), иногда переводчик, художник, правоообладатель
#         content_details_right = driver.find_element(By.CLASS_NAME, "biblio_book_info_detailed_right")
#         # теперь считанную из столбца информацию можно разбиваем на элементы с помощью тега li, получаем список
#         content_parts_right = content_details_right.find_elements(By.TAG_NAME, "li")
#         for i in range(len(content_parts_right)):
#             print(content_parts_right[i].text)
#
#         content_descript = driver.find_element(By.CLASS_NAME, "biblio_book_descr_publishers")
#         print("Описание:", content_descript.text)
#         time.sleep(2)
#
#     except:
#         print("Error reading personal book info. Go to the next book...")
#     finally:
#         # здесь должен быть переход к следующей книге
#         ref = "https://www.litres.ru/mark-levi/sumerki-hischnikov/"
#         count_links += 1
#
# driver.close()  # эта строчка временная
