import json
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
import json
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
from selenium.common.exceptions import StaleElementReferenceException


sys.path.append('Customer_data')
from Customer_data.Customers import customers

customer_id = 0
num_tabs = 10
timeout = 120

nickname = customers[customer_id]['email']
password = customers[customer_id]['password']
my_link = customers[customer_id]['link']
my_id = my_link.split('/')[-3]

driver = None
elem = []
curr_order_link = 'None'

tab_status = pd.DataFrame({'idx': [0] * num_tabs,
                           'action': ['None'] * num_tabs,
                           'status': ['None'] * num_tabs,
                           'start_t': [0] * num_tabs,
                           'strings': [''] * num_tabs})


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
    print(orders)
    driver.get(orders.iloc[0].ORDER_LINK)
    city_el = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//a[@data-city-id=750000000]")))
    if len(elem) > 0:
        elem.pop()
    elem.append(city_el)
    click_mouse()

    # if list(select_by_attr('a', 'data-city-id', "750000000")):
    #     click_mouse()


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
    # for h in driver.window_handles:
    #     driver.switch_to.window(h)
    #     driver.close()
    # driver.quit()
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
    global curr_order_link
    print('_______________________')
    print(curr_order_link)
    print(text)
    print()
    cursor = db.cursor()
    cursor.execute(f"INSERT INTO LOGS_{customer_id} (ORDER_LINK, LOG_TEXT) "
                   "VALUES (:2, :3) ", (curr_order_link, text))
    db.commit()
    cursor.close()


def page_is_loaded():
    try:
        WebDriverWait(driver, 0.2).until(EC.element_to_be_clickable((By.CLASS_NAME, 'topbar__logo')))
        return True
    except:
        return False


def init_tables():
    global db
    cursor = db.cursor()
    cursor.execute(f"truncate table current_price_status_{customer_id}")
    db.commit()
    cursor.close()
    customer = customers[int(customer_id)]
    os.system(f"python3 order_list_to_db.py {customer_id} Customer_data/{customer['filename']} link price")
    time.sleep(5)


def get_price_rows():
    prices = {}

    # while True:
    try:
        WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CLASS_NAME, 'sellers-table__buy-cell-button')))
    except TimeoutException:
        write_logs_out(traceback.format_exc())
        try:
            WebDriverWait(driver, 3).until(
                EC.text_to_be_present_in_element((By.CLASS_NAME, 'layout'), 'К сожалению, в настоящее время'))
            write_logs_out('No seller')
        except TimeoutException:
            write_logs_out(traceback.format_exc())
            raise Exception('MANUAL EXCEPTION 1')
    was_exception = False
    rows_scanned = False
    while not rows_scanned:
        if was_exception:
            write_detailed_logs = True
        else:
            write_detailed_logs = False
        was_exception = False
        for j, row in enumerate(select_by_tag('tr')):
            try:
                if write_detailed_logs:
                    write_logs_out(f'Row no {j}')
                rr = row.get_attribute('innerText')
                if write_detailed_logs:
                    write_logs_out(f'Row no {j}: innerText={rr}')
                inner_html = row.get_attribute('innerHTML')
                if write_detailed_logs:
                    write_logs_out(f'Row no {j}: innerHTML={inner_html}')
                if rr is None or inner_html is None:
                    raise StaleElementReferenceException('rr or inner_html is None')
                # print(rr)
            except StaleElementReferenceException:
                was_exception = True
                write_logs_out(traceback.format_exc())
                # print(row.)
                # rows = list(select_by_tag('tr'))
                # if rows_len != len(potential_rows):
                #     raise Exception(f'new list is different {rows_len} {len(potential_rows)}')
                # rows = potential_rows
                # break
                break
            if 'Выбрать' not in rr:
                if write_detailed_logs:
                    write_logs_out(f'Row no {j}: Выбрать not in rr')
                continue

            name = rr.split('\n')[0]
            prc = [re.sub('[^0-9]', '', pp) for pp in rr.split('\n')]
            prc = max([int(pp) for pp in prc if pp != ''])
            # print(name)
            # print(prc)
            href = 'https://kaspi.kz' + inner_html[inner_html.find('href'):].split('"')[1]
            # print(href)
            prices[href] = (name, prc)
            # print(name)
            # print('------------------------\n')
            # if not was_exception:x
            #     success_parsing_page_prices = True
        # time.sleep(0.5)
        if not was_exception:
            if write_detailed_logs:
                write_logs_out(f'NO EXCEPTION FOR PAGE')
            rows_scanned = True
        else:
            if write_detailed_logs:
                write_logs_out('EXCEPTION FOR PAGE')
    pagination_exists = len(list(select_by_class('pagination__el'))) > 0
    if pagination_exists:
        finished = False
        next_pressed = False
        while not finished and not next_pressed:
            try:
                for page_button in select_by_class('pagination__el'):
                    if page_button.get_attribute('innerText') == 'Следующая':
                        if page_button.get_attribute('class') == 'pagination__el':
                            # press
                            click_mouse()
                            # print(prices)
                            # print('CLICK...')
                            next_pressed = True
                            break
                        elif page_button.get_attribute('class') == 'pagination__el _disabled':
                            finished = True
            except Exception:
                write_logs_out(traceback.format_exc())
                # print('e2', e)
                pass
        if finished:
            return False, prices
        if next_pressed:
            return True, prices
    else:
        return False, prices


# def get_price_rows():
#     'https://kaspi.kz/shop/p/artel-art-0960-l-punto-belyi-100082826'  # check error
#     prices = {}
#
#     # while True:
#     success_parsing_all_prices = False
#     while not success_parsing_all_prices:
#         time.sleep(0.1)
#         try:
#             WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, 'sellers-table__buy-cell-button')))
#         except TimeoutException:
#             write_logs_out(traceback.format_exc())
#             try:
#                 WebDriverWait(driver, 5).until(
#                     EC.text_to_be_present_in_element((By.CLASS_NAME, 'layout'), 'К сожалению, в настоящее время'))
#                 write_logs_out('No seller')
#                 break
#             except TimeoutException:
#                 write_logs_out(traceback.format_exc())
#                 raise Exception('MANUAL EXCEPTION 1')
#         was_exception = False
#         rows_scanned = False
#         while not rows_scanned:
#             if was_exception:
#                 write_detailed_logs = True
#             else:
#                 write_detailed_logs = False
#             was_exception = False
#             for j, row in enumerate(select_by_tag('tr')):
#                 try:
#                     if write_detailed_logs:
#                         write_logs_out(f'Row no {j}')
#                     rr = row.get_attribute('innerText')
#                     if write_detailed_logs:
#                         write_logs_out(f'Row no {j}: innerText={rr}')
#                     inner_html = row.get_attribute('innerHTML')
#                     if write_detailed_logs:
#                         write_logs_out(f'Row no {j}: innerHTML={inner_html}')
#                     if rr is None or inner_html is None:
#                         raise StaleElementReferenceException('rr or inner_html is None')
#                     # print(rr)
#                 except StaleElementReferenceException:
#                     was_exception = True
#                     write_logs_out(traceback.format_exc())
#                     # print(row.)
#                     # rows = list(select_by_tag('tr'))
#                     # if rows_len != len(potential_rows):
#                     #     raise Exception(f'new list is different {rows_len} {len(potential_rows)}')
#                     # rows = potential_rows
#                     # break
#                     break
#                 if 'Выбрать' not in rr:
#                     if write_detailed_logs:
#                         write_logs_out(f'Row no {j}: Выбрать not in rr')
#                     continue
#
#                 name = rr.split('\n')[0]
#                 prc = [re.sub('[^0-9]', '', pp) for pp in rr.split('\n')]
#                 prc = max([int(pp) for pp in prc if pp != ''])
#                 # print(name)
#                 # print(prc)
#                 href = 'https://kaspi.kz' + inner_html[inner_html.find('href'):].split('"')[1]
#                 # print(href)
#                 prices[href] = (name, prc)
#                 # print(name)
#                 # print('------------------------\n')
#                 # if not was_exception:x
#                 #     success_parsing_page_prices = True
#             # time.sleep(0.5)
#             if not was_exception:
#                 if write_detailed_logs:
#                     write_logs_out(f'NO EXCEPTION FOR PAGE')
#                 rows_scanned = True
#             else:
#                 if write_detailed_logs:
#                     write_logs_out('EXCEPTION FOR PAGE')
#         pagination_exists = len(list(select_by_class('pagination__el'))) > 0
#         if pagination_exists:
#             finished = False
#             next_pressed = False
#             while not finished and not next_pressed:
#                 try:
#                     for page_button in select_by_class('pagination__el'):
#                         if page_button.get_attribute('innerText') == 'Следующая':
#                             if page_button.get_attribute('class') == 'pagination__el':
#                                 # press
#                                 click_mouse()
#                                 # print(prices)
#                                 # print('CLICK...')
#                                 next_pressed = True
#                                 break
#                             elif page_button.get_attribute('class') == 'pagination__el _disabled':
#                                 finished = True
#                 except Exception:
#                     write_logs_out(traceback.format_exc())
#                     # print('e2', e)
#                     pass
#             if finished:
#                 success_parsing_all_prices = True
#         else:
#             success_parsing_all_prices = True
#     return prices

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


def change_tab_status(i, idx=None, action=None, status=None, start_t=False, strings=None):
    if idx:
        tab_status.loc[i, 'idx'] = idx
    if action:
        tab_status.loc[i, 'action'] = action
    if status:
        tab_status.loc[i, 'status'] = status
    if start_t:
       tab_status.loc[i, 'start_t'] = int(time.time())
    if strings:
        tab_status.loc[i, 'strings'] = strings


def write_prices(prices):
    prices_df = pd.DataFrame({'seller_link': list(prices.keys()),
                              'seller_name': [n for n, _ in prices.values()],
                              'seller_price': [p for _, p in prices.values()]})

    cursor = db.cursor()
    cursor.execute(
        f"INSERT INTO scan_event_{customer_id} (order_link, sellers_links, sellers_names, sellers_prices) "
        "VALUES (:1, :2, :3, :4)", (str(mini_orders.iloc[tab_status.loc[i, 'idx']].ORDER_LINK),
                                    ','.join(prices_df.seller_link.values),
                                    ','.join(prices_df.seller_name.values),
                                    ','.join(prices_df.seller_price.astype('str').values)))
    db.commit()
    cursor.close()


def parse_prices():
    next_pressed, prices = get_price_rows()
    if len(prices) == 0:
        next_pressed, prices = get_price_rows()


    return next_pressed, prices


if __name__ == '__main__':
    create_driver()
    cx_Oracle.init_oracle_client(config_dir=config.wallet_dir,
                                 lib_dir=config.db_lib_dir)
    db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')
    init_tables()
    orders = psql.read_sql(f'SELECT * from order_table_{customer_id} order by order_link', db)
    login()
    open_new_tabs()

    idx = np.linspace(0, orders.shape[0], num_tabs + 1)
    # tab_status = [[0, -1, 0, 0]] * num_tabs  # order_n, curr_phase, next_phase, time_elps
    mini_orders_all = []
    for i in range(len(driver.window_handles)):
        mini_orders_all.append(orders.iloc[int(idx[i]):int(idx[i + 1])])
        min_iter_no = min(mini_orders_all[-1].ITER_NO)
        max_iter_no = max(mini_orders_all[-1].ITER_NO)
        if max_iter_no - min_iter_no > 1:
            write_logs_out(f'Divergence in iter_no for {int(idx[i])}:{int(idx[i + 1])}')
            raise Exception(f'Divergence in iter_no for {int(idx[i])}:{int(idx[i + 1])}')
        if min_iter_no == max_iter_no:
            write_logs_out(f'New cycle for {int(idx[i])}:{int(idx[i + 1])}')
        else:
            mini_orders_all[-1] = mini_orders_all[-1][mini_orders_all[-1].ITER_NO == min_iter_no]
            write_logs_out(f'Continuing prev cycle for {int(idx[i])}:{int(idx[i + 1])}: smallest iterated orders len = {len(orders)}')

    while True:
        print(tab_status)
        for i, h in enumerate(driver.window_handles):
            try:

                mini_orders = mini_orders_all[i]

                # mini_orders.loc[0] = {
                #     'ORDER_LINK': 'https://kaspi.kz/shop/p/almacom-lux-chanel-ach-12lc-belyi-4200972/?c=750000000',
                #     'MIN_PRICE': 100000,
                #     'CLS': 0,
                #     'SKIP': 0,
                #     'SKIP_REASON': 'None',
                #     'ITER_NO': 0}
                driver.switch_to.window(h)
                # if list(select_by_attr('a', 'data-city-id', "750000000")):
                #     click_mouse()
                curr_order_link = mini_orders.iloc[tab_status.loc[i % mini_orders.shape[0], 'idx']].ORDER_LINK
                curr_order_active = mini_orders.iloc[tab_status.loc[i % mini_orders.shape[0], 'idx']].ACTIVE
                if not curr_order_active:
                    tab_status.loc[i % mini_orders.shape[0], 'idx'] += 1
                    continue
                if tab_status.loc[i, 'action'] == 'None':
                    # start cycle
                    driver.get(curr_order_link)
                    change_tab_status(i, action='open_order', status='pending', start_t=True)
                elif tab_status.loc[i, 'action'] == 'open_order':
                    if tab_status.loc[i, 'status'] == 'pending':
                        if page_is_loaded():
                            change_tab_status(i, status='success')
                    if tab_status.loc[i, 'status'] == 'success':
                        change_tab_status(i, action='pricep_1', status='pending', start_t=True)
                        next_pressed, prices = get_price_rows()
                        if len(prices) == 0:
                            next_pressed, prices = get_price_rows()
                        if next_pressed:
                            action = tab_status.loc[i]['action']
                            action_no = int(action.split('_')[1])
                            change_tab_status(i, action=f'pricep_{action_no + 1}', status='pending', start_t=True,
                                              strings=json.dumps(prices))
                        else:
                            if len(prices) > 0:
                                write_prices(prices)
                                change_tab_status(i, action=f'None', idx=tab_status.loc[i]['idx'] + 1)
                            else:
                                change_tab_status(i, action=f'None', idx=tab_status.loc[i]['idx'] + 1)
                                raise Exception('No any seller')
                elif 'pricep' in tab_status.loc[i, 'action']:
                    next_pressed, prices = get_price_rows()
                    if len(prices) == 0:
                        next_pressed, prices = get_price_rows()
                    if next_pressed:
                        action = tab_status.loc[i]['action']
                        action_no = int(action.split('_')[1])
                        change_tab_status(i, action=f'pricep_{action_no + 1}', status='pending', start_t=True,
                                          strings=tab_status.loc[i]['strings'] + '|+|' + json.dumps(prices))
                    else:
                        strings = tab_status.loc[i]['strings']
                        strings = strings.split('|+|')
                        strings = [json.loads(s) for s in strings]
                        res = {}
                        for p in strings + [prices]:
                            res.update(p)
                        write_prices(res)
                        if len(prices) == 0:
                            raise Exception('No seller on last page')
                        change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                else:
                    raise NotImplementedError('Not implemented action')
            except Exception as e:
                change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                write_logs_out(traceback.format_exc())
            # time.sleep(1)

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