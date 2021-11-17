from selenium import webdriver
from selenium.webdriver.common.by import By
import time
# from anticaptcha import process_captcha 
from selenium.common.exceptions import NoSuchElementException

# открываем сайт литрес, точнее его страницу с новинками

f_input = open('first_page.txt', 'r')
page_link = f_input.readline().strip()
count_pages_init = int(f_input.readline())
count_pages_interval = int(f_input.readline())
f_input.close()


def check_exists_captcha(driver):
    try:
        driver.find_element(By.CLASS_NAME, "litres_captcha_page")
        return True
    except NoSuchElementException:
        return False


count_pages = count_pages_init

driver = webdriver.Firefox()

f_links = open('links.txt', 'a')
f_err = open('errors.txt', 'a')

# цикл по страницам сайта
while count_pages < count_pages_init + count_pages_interval:

    try:
        driver.get(page_link)
        if check_exists_captcha(driver):
            print("Captcha! Waiting for manual handling")
            time.sleep(120)
            print('Captcha managed')
            driver.get(page_link)

        print("Page number:", count_pages)

        # time.sleep(1)
        content_books_names = driver.find_elements(By.CLASS_NAME, "art__name")  # названия книги
        time.sleep(1)

        content_books_hrefs = set()  # персональные ссылки книг
        for book in content_books_names:
            elements = book.find_elements(By.TAG_NAME, "a")
            for el in elements:
                content_books_hrefs.add(el.get_attribute("href"))
        time.sleep(1)

        for i in content_books_hrefs:
            f_links.write(i + '\n')
        time.sleep(1)

        content_books_hrefs.clear()
    except :
        print('Something wrong with page', count_pages)
        f_err.write(page_link + '\n')
        print('Go to the next page...')

    count_pages += 1

    page_link = "https://www.litres.ru/novie/elektronnie-knigi/page-" + str(count_pages) + "/?lang=52"

f_links.close()
f_err.close()

f_output = open('last_page.txt', 'w')
f_output.write(page_link)
f_output.close()

driver.close()
