import os
import re
import time
import atexit
import warnings
import sys
from threading import Thread
warnings.filterwarnings("ignore")

import psycopg2 as pg
import pandas.io.sql as psql

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options

elem = []


def fill_by_name(name, fill):
    global elem
    if len(elem) > 0:
        elem.pop()
    elem.append(driver.find_element_by_name(name))
    elem[0].clear()
    elem[0].send_keys(fill)


def fill_by_class(name, fill):
    classes = select_by_class(name)
    for c in classes:
        if c.get_attribute('class') == 'form__col _12-12 _medium_6-12':
            break
    elem[0].clear()
    elem[0].send_keys(fill)


def press_enter():
    elem[0].send_keys(Keys.RETURN)


def click_mouse():
    elem[0].click()


def select_by_class(name):
    global elem
    elems = driver.find_elements_by_class_name(name)
    for el in elems:
        if len(elem) > 0:
            elem.pop()
        elem.append(el)
        yield el


def select_by_tag(name):
    global elem
    elems = driver.find_elements_by_tag_name(name)
    for el in elems:
        if len(elem) > 0:
            elem.pop()
        elem.append(el)
        yield el


def select_by_attr(tag_name, attr_name, attr):
    global elem
    elems = driver.find_elements(By.XPATH, f"//{tag_name}[@{attr_name}='{attr}']")
    for el in elems:
        if len(elem) > 0:
            elem.pop()
        elem.append(el)
        yield el


def wait_till_load_by_text(text):
    # driver.find_elements_by_xpath(f"//*[contains(text(), '{text}')]")
    trials = 5
    for i in range(trials):
        try:
            myElem = WebDriverWait(driver, 5).until(EC.text_to_be_present_in_element((By.CLASS_NAME, 'layout'), text))
            return True
        except TimeoutException:
            driver.back()
            # time.sleep(1)
            driver.forward()
            # driver.refresh()
    exit_handler()
    print('exit_handler called')
    exit()
    return False


def index_rows():
    driver.get("https://kaspi.kz/merchantcabinet/#/offers")
    success1 = wait_till_load_by_text('Управление товарами')
    success2 = wait_till_load_by_text(' из ')
    success = success1 and success2
    while not success:
        driver.back()
        time.sleep(2)
        driver.get("https://kaspi.kz/merchantcabinet/#/offers")
        success1 = wait_till_load_by_text('Управление товарами')
        success2 = wait_till_load_by_text(' из ')
        success = success1 and success2
    links = set()
    finished = False
    while not finished:
        rows = []
        while len(rows) == 0:
            rows = list(select_by_class('offer-managment__product-cell-link'))

        links.update([el.get_attribute('href') for el in rows])
        # for el in rows:
            # print(el)
            # el.get_attribute('href')
            # press_enter()

        page_info = list(select_by_attr('div', 'class', 'gwt-HTML'))[-1].text
        if page_info != '':
            page_info = page_info.split()
            finished = page_info[0].split('-')[1] == page_info[-1]
            if finished:
                # print('after_finished', rows)
                try:
                    # print('after_finished', [r.get_attribute('href') for r in rows])
                    [r.get_attribute('href') for r in rows]
                except:
                    finished = False
                    # print('after_failure')
                # print('---')
        if not finished:
            while not list(select_by_attr('img', 'aria-label', 'Next page')):
                time.sleep(0.1)
            # wait_till_load_by_text(' из ')
            click_mouse()
            wait_till_load_by_text(' из ')

            # finished = True  # TODO: COMMENT

    return list(links)


def login():
    global driver
    while True:
        driver.get("https://kaspi.kz/merchantcabinet/login#/offers")
        success1 = wait_till_load_by_text('Вход для магазинов')
        while not success1:
            driver.close()
            # driver = webdriver.Firefox(firefox_profile=fp)
            create_driver()
            driver.get("https://kaspi.kz/merchantcabinet/login#/offers")
            success1 = wait_till_load_by_text('Вход для магазинов')

        # fill_by_name('username', 'bitcom-90@mail.ru')
        fill_by_name('username', 'Kuanishbekkyzy@mail.ru')
        fill_by_name('password', 'Nurislam177@')
        press_enter()
        success2 = wait_till_load_by_text('Заказы')
        if success2:
            break
        else:
            driver.close()
            # driver = webdriver.Firefox(firefox_profile=fp)
            create_driver()
        # while not success:
        #     driver.close()
        #     driver = webdriver.Firefox(firefox_profile=fp)
        #     driver.get("https://kaspi.kz/merchantcabinet/login#/offers")
        #     success = wait_till_load_by_text('Заказы')


def get_price_rows(link):
    driver.get(link)
    if list(select_by_attr('a', 'data-city-id', "750000000")):
        click_mouse()
    prices = {}
    while True:
        # print('next loop')
        rows = list(select_by_tag('tr'))
        while len(rows) == 0:
            time.sleep(0.1)
            rows = list(select_by_tag('tr'))

            # print(rows)

        for row in rows:
            a_tag = row.find_elements_by_tag_name('a')
            prc = row.find_elements_by_class_name('sellers-table__price-cell-text')
            if a_tag:
                prices[a_tag[0].get_attribute('href')] = prc[0].text
        pages_button = list(select_by_class('pagination__el'))
        # print('bbb', pages_button.__len__())
        exit_by_break = False
        for pg in pages_button:
            if pg.text == 'Следующая' and pg.get_attribute('class') == 'pagination__el':
                while True:
                    try:
                        # print('click trial')
                        click_mouse()
                        break
                    except:
                        time.sleep(0.1)
                        continue
                exit_by_break = True
                break
        if exit_by_break:
            continue
        break

    # print([row.text for row in rows])
    return prices


def update_price(link, new_price):
    link = f"https://kaspi.kz/merchantcabinet/#/offers/edit/{link.split('-')[-1]}_11056023"
    driver.get(link)
    got_page = False
    while not got_page:
        success1 = wait_till_load_by_text('Заказы')
        if success1:
            driver.get(link)
        else:
            continue
        got_page = wait_till_load_by_text('Редактирование товара')
    radios = select_by_class('form__radio-title')

    for radio in radios:
        if radio.text == 'Одна для всех городов':
            break
    click_mouse()
    fill_by_class('form__col', str(new_price))

    buttons = select_by_tag('button')
    for button in buttons:
        if button.text == 'Сохранить':
            break
    press_enter()


def process_order(order_link, prices, min_price, to_skip, iter_no):
    sellers_links = []
    sellers_prices = []
    if my_link in prices:
        my_price = int(re.sub('[^0-9]', '', prices[my_link]))
    else:
        print('I AM NOT A SELLER')
        cursor = db.cursor()
        cursor.execute("UPDATE order_table "
                       "SET iter_no = %s, skip = %s "
                       "WHERE order_link = %s", (int(iter_no) + 1, True, link))
        db.commit()
        cursor.close()
        return

    for k in prices.keys():
        price = int(re.sub('[^0-9]', '', prices[k]))
        sellers_links.append(k)
        sellers_prices.append(price)

    cursor = db.cursor()
    cursor.execute("INSERT INTO scan_event (order_link, sellers_links, sellers_prices) "
                   "VALUES(%s, %s, %s)", (order_link, sellers_links, sellers_prices))

    cursor.execute("INSERT INTO current_price_status (order_link, curr_price) "
                   "VALUES (%s, %s) "
                   "ON CONFLICT (order_link) DO UPDATE "
                   "SET curr_price = %s", (order_link,
                                           my_price,
                                           my_price))

    if not to_skip:
        iam_top1 = sellers_links[0] == my_link

        desired_price = min(sellers_prices) - 1
        desired_price = max(min_price, desired_price)

        price_status = psql.read_sql(f"SELECT * FROM current_price_status where order_link=\'{order_link}\'", db)
        curr_price = price_status.curr_price.iloc[0]
        next_price = price_status.next_price.iloc[0]

        if not next_price or next_price == curr_price:
            if not iam_top1 and desired_price != curr_price:
                print(f'Updating price to {desired_price} for {link}')
                update_price(order_link, desired_price)
                cursor.execute("UPDATE current_price_status "
                               "SET next_price = %s "
                               "WHERE order_link = %s", (int(desired_price), link))
            else:
                print(f'Already top1 or price == min_price for {link}')
        else:
            print('Previous price update is pending')
    else:
        print(f'Skipping {link}')
    cursor.execute("UPDATE order_table "
                   "SET iter_no = %s "
                   "WHERE order_link = %s", (int(iter_no) + 1, link))
    db.commit()
    cursor.close()


def create_driver():
    global driver

    # chrome_options = Options()
    # chrome_options.add_argument("--window-size=1920,1080")
    # chrome_options.add_argument("--start-maximized")
    # chrome_options.headless = True
    fp = webdriver.FirefoxProfile()
    fp.set_preference("dom.popup_maximum", 0)
    options = Options()
    options.headless = True
    # options.add_argument('--headless')
    driver = webdriver.Firefox(options=options, firefox_profile=fp)

    # options = webdriver.ChromeOptions()
    # options.add_argument("headless")
    # driver = webdriver.Chrome(options=chrome_options)

# def get_prices(link):
#     driver.get(link)
#     if list(select_by_attr('a', 'data-city-id', "750000000")):
#         click_mouse()
#
#     rows = get_price_rows()
#     prices = {}
#     for row in rows:
#         a_tag = row.find_elements_by_tag_name('a')
#         prc = row.find_elements_by_class_name('sellers-table__price-cell-text')
#         if a_tag:
#             prices[a_tag[0].get_attribute('href')] = prc[0].text
#     return prices


start_time = [float('inf')]
def start_time_counter(timeout, start_time):
    global driver
    while True:
        if time.time() - start_time[0] > timeout:
            print(f'Elapsed more than {timeout} sec -> exit()')
            if wait_till_load_by_text('К сожалению, в настоящее время'):
                print('NO ANY SELLER')
                cursor = db.cursor()
                cursor.execute("UPDATE order_table "
                               "SET iter_no = %s, skip = %s "
                               "WHERE order_link = %s", (int(iter_no) + 1, True, link))
                db.commit()
                cursor.close()
                exit_handler()
            exit_handler()
        time.sleep(3)
th = Thread(target=start_time_counter, args=(60, start_time))
th.start()


def exit_handler():
    global driver, db
    driver.close()
    db.close()
    os.kill(os.getpid(), 9)
atexit.register(exit_handler)


if __name__ == '__main__':
    my_link = 'https://kaspi.kz/shop/info/merchant/11056023/address-tab/'

    db = pg.connect(user='batyagg',
                    password='asdasd',
                    database='postgres',
                    host='localhost',
                    port=5432)
    driver = None
    create_driver()
    # process_prices('https://kaspi.kz/shop/p/kompressor-masljanyi-mateus-ms03307-100935415/', {'https://kaspi.kz/shop/info/merchant/61012/address-tab/': '392 694 ₸', 'https://kaspi.kz/shop/info/merchant/tiyn/address-tab/': '392 694 ₸', 'https://kaspi.kz/shop/info/merchant/3122014/address-tab/': '392 695 ₸', 'https://kaspi.kz/shop/info/merchant/6416001/address-tab/': '392 697 ₸', 'https://kaspi.kz/shop/info/merchant/516011/address-tab/': '392 700 ₸', 'https://kaspi.kz/shop/info/merchant/5346003/address-tab/': '392 770 ₸', 'https://kaspi.kz/shop/info/merchant/11121004/address-tab/': '393 000 ₸', 'https://kaspi.kz/shop/info/merchant/10923000/address-tab/': '393 333 ₸', 'https://kaspi.kz/shop/info/merchant/polat/address-tab/': '401 539 ₸', 'https://kaspi.kz/shop/info/merchant/2731002/address-tab/': '418 691 ₸', 'https://kaspi.kz/shop/info/merchant/5336002/address-tab/': '444 599 ₸', 'https://kaspi.kz/shop/info/merchant/shelby/address-tab/': '444 611 ₸', 'https://kaspi.kz/shop/info/merchant/altynorda/address-tab/': '445 470 ₸', 'https://kaspi.kz/shop/info/merchant/8265001/address-tab/': '475 000 ₸'})

    login()

    # index_rows()
    orders = psql.read_sql('SELECT * from order_table', db)
    print('Orders quant total', len(orders))

    min_iter_no = min(orders.iter_no)
    max_iter_no = max(orders.iter_no)
    if max_iter_no - min_iter_no > 1:
        raise Exception('Divergence in iter_no')
    if min_iter_no == max_iter_no:
        print('Starting new cycle')
    else:
        orders = orders[orders.iter_no==min_iter_no]
        print(f'Continuing prev cycle: new size = {len(orders)}')

    for i in range(len(orders)):
        start_time[0] = time.time()

        order = orders.iloc[i]
        link = order.order_link
        min_price = order.min_price
        to_skip = order.skip
        iter_no = order.iter_no
        success = False

        print()
        print(f'{i + 1}/{len(orders)}')
        print(link)
        while not success:
            try:
                # driver.get(link)
                #
                prices = get_price_rows(link)
                process_order(link, prices, min_price, to_skip, iter_no)
                # print(prices)
                success = True
            except:
                success = False
                driver.back()
                time.sleep(5)

    driver.close()
    db.close()


# if __name__ == '__main__':
#     a = 'https://kaspi.kz/shop/p/vicalina-kazan-vl0128-7-5-l-stal--104165439/'
#     # 'https://kaspi.kz/merchantcabinet/#/offers/edit/104165439_11121004'
#
#     b = f"https://kaspi.kz/merchantcabinet/#/offers/edit/{a.split('-')[-1][:-1]}_11121004"
#     print(a)
#     print(b)

#     a = 'https://kaspi.kz/shop/p/fissman-311-102372860/?c=750000000'
#     'https://kaspi.kz/merchantcabinet/#/offers/edit/102372860_11121004'
#
#     # a = 'https://kaspi.kz/shop/p/makute-hd012-8467299000-3-5-dzh-sds-max-udlinitel-i-perchatki-103515473/?c=750000000'
#
#     driver.get(a)
#     if list(select_by_attr('a', 'data-city-id', "750000000")):
#         click_mouse()
#
#     print(get_price_rows())
