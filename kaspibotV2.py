import os
import re
import time
import atexit
import traceback
import warnings
import datetime
import sys
import os
import html
import argparse
from html.parser import HTMLParser

import pandas as pd
from bs4 import BeautifulSoup

import config
from threading import Thread
warnings.filterwarnings("ignore")

import numpy as np
import psycopg2 as pg
import pandas.io.sql as psql
import cx_Oracle
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options


sys.path.append('Customer_data')
from Customer_data.Customers import customers

customer_id = 0
nickname = customers[0]['email']
password = customers[0]['password']
num_tabs = 5

driver = None
elem = []

tab_status = pd.DataFrame({'idx': [0] * num_tabs,
                           'action': ['None'] * num_tabs,
                           'status': ['None'] * num_tabs,
                           'start_t': [0] * num_tabs})


def create_driver():
    global driver
    fp = webdriver.FirefoxProfile()
    fp.set_preference("dom.popup_maximum", 0)
    fp.set_preference("browser.link.open_newwindow", 0)
    fp.set_preference("browser.link.open_newwindow.restriction", 0)
    fp.set_preference("browser.tabs.remote.autostart", False)
    fp.set_preference("browser.tabs.remote.autostart.1", False)
    fp.set_preference("browser.tabs.remote.autostart.2", False)
    options = Options()
    options.headless = False
    options.page_load_strategy = 'none'
    driver = webdriver.Firefox(options=options,
                               firefox_profile=fp)

def open_new_tabs():
    for i in range(num_tabs - 1):
        driver.switch_to.new_window('TAB')


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
        # fill_by_name('username', 'Kuanishbekkyzy@mail.ru')
        # fill_by_name('password', 'Nurislam177@')

        fill_by_name('username', nickname)
        fill_by_name('password', password)
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
    # exit_handler()
    write_logs_out(f'Exit handler called by wait_till_load_by_text: {text}')
    exit()
    return False


def fill_by_name(name, fill):
    global elem
    if len(elem) > 0:
        elem.pop()
    elem.append(driver.find_element_by_name(name))
    elem[0].clear()
    elem[0].send_keys(fill)


def select_by_attr(tag_name, attr_name, attr):
    global elem
    elems = driver.find_elements(By.XPATH, f"//{tag_name}[@{attr_name}='{attr}']")
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


def select_by_class(name):
    global elem
    elems = driver.find_elements_by_class_name(name)
    for el in elems:
        if len(elem) > 0:
            elem.pop()
        elem.append(el)
        yield el


def exit_handler():
    global driver, db
    for h in driver.window_handles:
        driver.switch_to.window(h)
        driver.close()

    db.close()
    # write_logs_out('Got kill signal')
    # write_logs_out('Closed driver and db')
    # os.kill(os.getpid(), 9)
atexit.register(exit_handler)


def press_enter():
    elem[0].send_keys(Keys.RETURN)


def click_mouse():
    elem[0].click()


def write_logs_out(text):
    print(text)
    cursor = db.cursor()
    cursor.execute(f"INSERT INTO logs_{customer_id} "
                   "VALUES (:2) ", (text))
    db.commit()
    cursor.close()


def get_price_rows():

    prices = {}
    while True:
        # print(thread_name, 'next loop')
        rows = list(select_by_tag('tr'))
        while len(rows) == 0:
            time.sleep(0.1)
            rows = list(select_by_tag('tr'))

            # print(thread_name, rows)
        potential_prices = list(select_by_class('sellers-table__price-cell-text'))
        for p in potential_prices:
            pp = p.get_attribute('innerText')
            if pp:
                pp
            result = re.sub('[^0-9]', '', a)


        for row in select_by_tag('tr'):
            inner_html = row.get_attribute('innerHTML')
            # print(inner_html)
            soup = BeautifulSoup(inner_html, "html.parser")
            # print(type(soup.find('a')), soup.find('a'))
            a_tags = soup.find('a')
            if a_tags is None:
                continue
            print(a_tags.text)
            print('https://kaspi.kz' + a_tags.get('href'))

            soup = BeautifulSoup(inner_html, "html.parser")

            # print(list(soup.find('td').children))
            print(inner_html)

            href = 'https://kaspi.kz' + a_tags.get('href')


            print()
            # prices[]
            # a_tag = row.find_elements_by_tag_name('a')
            # prc = row.find_elements_by_class_name('sellers-table__price-cell-text')
            # if a_tag:
            #     prices[a_tag[0].get_attribute('href')] = prc[0].text
        pages_button = select_by_class('pagination__el')
        # print('[[[', len(pages_button), pages_button)
        # print(thread_name, 'bbb', pages_button.__len__())
        exit_by_break = False
        for pg in pages_button:
            # print(type(pg))
            # print(pg.get_attribute('innerText'))
            if pg.get_attribute('innerText') == 'Следующая':
                # while
                # click_mouse()
                for _ in range(10):
                    try:
                        # print(thread_name, 'click trial')
                        click_mouse()
                        exit_by_break = True
                        break
                    except:
                        time.sleep(0.1)
                        continue
                # exit_by_break = True
                break
        if exit_by_break:
            continue
        break

    # print(thread_name, [row.text for row in rows])
    return prices


# def check_curr_status(order):
#
#     print('currvava', order.ORDER_LINK)
#     print('uurvavav', driver.current_url)
#     if driver.current_url == order.ORDER_LINK:
#         return 1
#     return 0


def truncate_tables():
    global db
    try:
        db.close()
    except:
        pass
    # db = pg.connect(user=config.db_user,
    #                 password=config.db_pass,
    #                 database=config.db,
    #                 host=config.host,
    #                 port=config.port)
    cursor = db.cursor()
    cursor.execute(f"truncate _{customer_id}_current_price_status")
    db.commit()
    cursor.close()
    customer = customers[int(customer_id)]
    os.system(f"python3 order_list_to_db.py {customer_id} Customer_data/{customer['filename']} link price")
    time.sleep(5)


if __name__ == '__main__':
    create_driver()
    login()
    open_new_tabs()
    cx_Oracle.init_oracle_client(config_dir=config.wallet_dir,
                                 lib_dir=config.db_lib_dir)
    db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')
    orders = psql.read_sql(f'SELECT * from order_table_{customer_id} order by order_link', db)

    idx = np.linspace(0, orders.shape[0], num_tabs + 1)
    # tab_status = [[0, -1, 0, 0]] * num_tabs  # order_n, curr_phase, next_phase, time_elps

    while True:
        print(tab_status)
        for i, h in enumerate(driver.window_handles):
            mini_orders = orders.iloc[int(idx[i]) + 1:int(idx[i + 1])]
            driver.switch_to.window(h)
            if list(select_by_attr('a', 'data-city-id', "750000000")):
                click_mouse()

            if tab_status.loc[i, 'action'] == 'None':
                # start cycle
                driver.get(mini_orders.iloc[tab_status.loc[i, 'idx']].ORDER_LINK)
                tab_status.loc[i, 'action'] = 'open_order'
                tab_status.loc[i, 'status'] = 'pending'
                tab_status.loc[i, 'start_t'] = int(time.time())
            elif tab_status.loc[i, 'action'] == 'open_order':
                if tab_status.loc[i, 'status'] == 'pending':
                    rows = list(select_by_tag('tr'))
                    if len(rows) != 0:
                        prices = get_price_rows()
                        print(prices)
            else:
                raise Exception
            time.sleep(1)

            # if curr_stat == 0:
            #     driver.get(mini_orders.iloc[status[0]])
            #     tab_status[i][2] = 1
            #     tab_status[i][3] = int(time.time())
            # elif curr_stat == 1:
            #
            # print('currstat', curr_stat)

    # body = driver.find_element_by_tag_name("body")
    # print(body)
    # body.send_keys(Keys.COMMAND + 't')
    # driver.execute_script(f"window.open(\"http://www.mozilla.org/\", \"asdasd\", \"popup\")")
    # current_tab = driver.current_window_handle
    # driver.execute_script('''window.open("https://www.google.com", "_blank");''')
    # driver.find_element_by_tag_name('body').send_keys(Keys.COMMAND + 't')
    # login()
    # time.sleep(10)