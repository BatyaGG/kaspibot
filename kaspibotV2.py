import pickle
import re
import time
import atexit
import traceback
import warnings
import sys
import os
import json
from collections import defaultdict
from threading import Thread

import pandas as pd
import psycopg2 as pg
from selenium.webdriver import ActionChains, DesiredCapabilities

import config
warnings.filterwarnings("ignore")

import numpy as np
import pandas.io.sql as psql
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import StaleElementReferenceException

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_colwidth', None)

print(os.path.abspath(os.getcwd()))

sys.path.append('Customer_data')
from Customer_data.Customers import customers

customer_id = config.merchant_id
num_tabs = 10
timeout_tab = 5 * 60
timeout_restart = 60 * 60
update_fact_t = 60
price_step = 2

nickname = customers[customer_id]['email']
password = customers[customer_id]['password']
my_link = customers[customer_id]['link']
my_id = my_link.split('/')[-3]

driver = None
elem = []
curr_order_link = 'None'
city_inited = False

tab_status = pd.DataFrame({'idx': [0] * num_tabs,
                           'action': ['None'] * num_tabs,
                           'status': ['None'] * num_tabs,
                           'start_t': [time.time()] * num_tabs,
                           'strings': [''] * num_tabs})


def init_vars():
    global driver, elem, curr_order_link, city_inited, tab_status
    driver = None
    elem = []
    curr_order_link = 'None'
    city_inited = False

    tab_status = pd.DataFrame({'idx': [0] * num_tabs,
                               'action': ['None'] * num_tabs,
                               'status': ['None'] * num_tabs,
                               'start_t': [time.time()] * num_tabs,
                               'strings': [''] * num_tabs})


def exit_handler():
    global driver, db
    try:
        db.close()
        print('closed db')
    except:
        print('db already closed')
    try:
        driver.quit()
        print('closed driver')
    except:
        print('driver already closed')
    os.system('pkill -9 firefox')
    print('called pkill')
atexit.register(exit_handler)


def create_driver():
    global driver
    # fp = webdriver.FirefoxProfile()
    # fp.set_preference("dom.popup_maximum", 0)
    # fp.set_preference("browser.link.open_newwindow", 0)
    # fp.set_preference("browser.link.open_newwindow.restriction", 0)
    # fp.set_preference("browser.tabs.remote.autostart", False)
    # fp.set_preference("browser.tabs.remote.autostart.1", False)
    # fp.set_preference("browser.tabs.remote.autostart.2", False)
    caps = DesiredCapabilities().FIREFOX
    # caps["pageLoadStrategy"] = 'none'
    options = Options()
    options.headless = True
    options.page_load_strategy = 'none'
    driver = webdriver.Firefox(options=options,
                               # firefox_profile=fp,
                               # capabilities=caps,
                               # executable_path='/main/drivers/geckodriver'
                               )


def open_new_tabs():
    for i in range(num_tabs - 1):
        driver.switch_to.new_window('TAB')
    write_logs_out('DEBUG', f'Opened new {num_tabs} tabs')


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


    # if list(select_by_attr('a', 'data-city-id', "750000000")):
    #     click_mouse()

def init_city():
    # driver.get(mini_orders_all[0].iloc[0].ORDER_LINK)
    driver.get('https://kaspi.kz/shop/p/almacom-ach-18as-belyi-montazhnyi-komplekt-101215645/?c=750000000')
    city_el = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, f"//a[@data-city-id=750000000]")))
    if len(elem) > 0:
        elem.pop()
    elem.append(city_el)
    click_mouse()


def wait_till_load_by_text(text, t=5.0):
    # driver.find_elements_by_xpath(f"//*[contains(text(), '{text}')]")
    trials = 1
    for i in range(trials):
        try:
            myElem = WebDriverWait(driver, t).until(EC.text_to_be_present_in_element((By.CLASS_NAME, 'layout'), text))
            return True
        except TimeoutException:
            driver.back()
            # time.sleep(1)
            driver.forward()
            # driver.refresh()
    # exit_handler()
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


def press_enter():
    elem[0].send_keys(Keys.RETURN)


def click_mouse():
    elem[0].click()


def write_logs_out(lvl, text, write_db=True):
    global curr_order_link
    print('_______________________')
    print(curr_order_link)
    print(lvl)
    print(text)
    print()
    if write_db:
        cursor = db.cursor()
        cursor.execute(f"INSERT INTO _{customer_id}_LOGS (ORDER_LINK, LOG_LEVEL, LOG_TEXT) "
                       "VALUES (%s, %s, %s) ", (curr_order_link, lvl, text))
        db.commit()
        cursor.close()


def page_is_loaded():
    try:
        WebDriverWait(driver, 0.2).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h2')))
        # WebDriverWait(driver, 0.2).until(EC.element_to_be_clickable((By.CLASS_NAME, 'topbar__logo')))
        return True
    except:
        return False


# def init_orders_table():
#     try:
#         create_tables_and_load(customer_id, db)
#         return True
#     except:
#         return False


def get_price_rows():
    prices = {}
    no_fail = False
    # while True:
    while not no_fail:
        try:
            WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CLASS_NAME, 'sellers-table__buy-cell-button')))
            no_fail = True
        except TimeoutException:
            write_logs_out('DEBUG', 'Seller table load fail')
            return False, None
            # try:
            #     WebDriverWait(driver, 2).until(
            #         EC.text_to_be_present_in_element((By.CLASS_NAME, 'layout'), 'К сожалению, в настоящее время'))
            #     write_logs_out('No seller')
            #     print('refresh 1')
            #     driver.refresh()
            # except TimeoutException:
            #     write_logs_out(traceback.format_exc())
            #     print('refresh 2')
            #     driver.refresh()
            #     raise Exception('MANUAL EXCEPTION 1')
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
                    write_logs_out('DEBUG', f'Row no {j}')
                rr = row.get_attribute('innerText')
                if write_detailed_logs:
                    write_logs_out('DEBUG', f'Row no {j}: innerText={rr}')
                inner_html = row.get_attribute('innerHTML')
                if write_detailed_logs:
                    write_logs_out('DEBUG', f'Row no {j}: innerHTML={inner_html}')
                if rr is None or inner_html is None:
                    raise StaleElementReferenceException('rr or inner_html is None')
                # print(rr)
            except StaleElementReferenceException:
                was_exception = True
                write_logs_out('DEBUG', traceback.format_exc())
                break
            if 'Выбрать' not in rr:
                if write_detailed_logs:
                    write_logs_out('DEBUG', f'Row no {j}: Выбрать not in rr')
                continue

            name = rr.split('\n')[0]
            prc = [re.sub('[^0-9]', '', pp) for pp in rr.split('\n')]
            prc = max([int(pp) for pp in prc if pp != ''])
            # print(name)
            # print(prc)
            href = 'https://kaspi.kz' + inner_html[inner_html.find('href'):].split('"')[1]
            href = href.split('?')[0]
            # print(href)
            prices[href] = (name, prc)
            # print(name)
            # print('------------------------\n')
            # if not was_exception:x
            #     success_parsing_page_prices = True
        # time.sleep(0.5)
        if not was_exception:
            if write_detailed_logs:
                write_logs_out('DEBUG', f'NO EXCEPTION FOR PAGE')
            rows_scanned = True
        else:
            if write_detailed_logs:
                write_logs_out('DEBUG', 'EXCEPTION FOR PAGE')
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
                write_logs_out('DEBUG', traceback.format_exc())
                # print('e2', e)
                pass
        if finished:
            return False, prices
        if next_pressed:
            return True, prices
    return False, prices


def refresh_at_page(page_num):
    driver.refresh()
    for page_button in select_by_class('pagination__el'):
        if page_button.get_attribute('innerText') == page_num:
            click_mouse()


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


def fill_by_class(name, fill):
    classes = select_by_class(name)
    for c in classes:
        if c.get_attribute('class') == 'form__col _12-12 _medium_6-12':
            break
    elem[0].clear()
    elem[0].send_keys(fill)


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
        f"INSERT INTO _{customer_id}_scan_event (order_link, sellers_links, sellers_names, sellers_prices) "
        "VALUES (%s, %s, %s, %s)", (str(mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].order_link),
                                    ','.join(prices_df.seller_link.values),
                                    ','.join(prices_df.seller_name.values),
                                    ','.join(prices_df.seller_price.astype('str').values)))
    db.commit()
    cursor.close()


def index_rows():
    write_logs_out('DEBUG', 'Indexing orders')
    success_open_offers = False
    while not success_open_offers:
        driver.get("https://kaspi.kz/merchantcabinet/#/offers")
        write_logs_out('DEBUG', 'Opened zakazy')

        # success1 = False
        # if not success1:
        success2 = wait_till_load_by_text('Заказы')
        write_logs_out('DEBUG', f'Tried zakazy {success2}' )
        if success2:
            try:
                element = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, f"//a[@class='main-nav__el-link'][@href='#/offers']")))
                write_logs_out('DEBUG', f'Found tovary button')
                actions = ActionChains(driver)
                actions.move_to_element(element)
                actions.perform()
                element = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, f"//a[@class='main-nav__sub-el-link'][@href='#/offers']")))
                write_logs_out('DEBUG', f'Found button upravlenie tovarami {success2}')
                element.click()
                success_open_offers = True
            except:
                write_logs_out('ERROR', traceback.format_exc())
                pass
        # else:
        #     success_open_offers = True
    try:
        # button = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'gwt-Button button')))
        curtain = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CLASS_NAME, 'ks-gwt-dialog _small g-ta-c')))
        button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'gwt-Button button')))
        button.click()
    except TimeoutException:
        write_logs_out('INFO', 'NO SERVICE NEDOSTUPEN_BUTTON')
    links = set()
    finished = False
    while not finished:
        rows = []
        while len(rows) == 0:
            rows = list(select_by_class('offer-managment__product-cell-link'))
            time.sleep(1)

        links.update([el.get_attribute('href')[:-1] for el in rows])
        write_logs_out('DEBUG', f'{len(links)}', write_db=False)
        write_logs_out('DEBUG', json.dumps(list(links)), write_db=False)
        # for el in rows:
            # print(thread_name, el)
            # el.get_attribute('href')
            # press_enter()

        page_info = list(select_by_attr('div', 'class', 'gwt-HTML'))[-1].text
        if page_info != '':
            page_info = page_info.split()
            finished = page_info[0].split('-')[1] == page_info[-1]
            if finished:
                # print(thread_name, 'after_finished', rows)
                try:
                    # print(thread_name, 'after_finished', [r.get_attribute('href') for r in rows])
                    [r.get_attribute('href') for r in rows]
                except:
                    finished = False
                    # print(thread_name, 'after_failure')
                # print(thread_name, '---')
        if not finished:
            if len(links) % 10 == 0:
                write_logs_out('DEBUG', 'links num is divisible by 10 -> pressing next')
                while not list(select_by_attr('img', 'aria-label', 'Next page')):
                    time.sleep(0.1)
                next_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, f"//img[@aria-label='Next page']")))
                next_button.click()
                # wait_till_load_by_text(' из ')
                # click_mouse()
                wait_till_load_by_text(' из ')
            else:
                write_logs_out('DEBUG', 'links num NOT divisible by 10 -> try page again')
    write_logs_out('DEBUG', f'Len of links is {len(links)}')
    return list(links)


def update_fact():
    write_logs_out('DEBUG', 'update_fact called')
    orders_fact = index_rows()
    # orders_fact = [l[:-1] for l in rows]
    # orders_fact = list(set(orders_fact))
    # orders_fact = [of for of in orders_fact if of!='']
    print(orders_fact)
    cursor = db.cursor()
    # cursor.execute(f'truncate table _{customer_id}_order_fact')
    cursor.execute(f'insert into _{customer_id}_order_fact (order_count, order_links) values (%s, %s)', (len(orders_fact), ','.join(orders_fact)))
    db.commit()
    cursor.close()
    write_logs_out('DEBUG', f'update_fact len of fact {len(orders_fact)}')
    return orders_fact


def prepare_orders():
    orders = psql.read_sql(f'SELECT * from _{customer_id}_order_table', db)
    orders = orders.sample(frac=1)

    write_logs_out('DEBUG', f'Total orders len: {len(orders)}')

    # TODO: change orders_face

    orders_fact = psql.read_sql(f'select Extract(epoch from (now() - created_at)/60) as minutes_ago, order_links '
                                f'from _{customer_id}_order_fact order by created_at desc limit 1', db)
    # print(orders_fact)
    assert len(orders_fact) < 2
    reserve_tab = False
    if len(orders_fact) == 0:
        write_logs_out('DEBUG', 'Empty order_fact table\n'
                                'Index orders')
        orders_fact = update_fact()
    elif orders_fact.iloc[0].minutes_ago > update_fact_t:
        write_logs_out('DEBUG', f'order_fact is old: {orders_fact.iloc[0].minutes_ago} minutes ago')
        change_tab_status(0, action='index_orders', start_t=True)
        reserve_tab = True
        orders_fact = orders_fact.iloc[0].order_links.split(',')
        write_logs_out('DEBUG', 'Loaded orders fact from db')
    else:
        write_logs_out('DEBUG', f'order_fact is OK: {orders_fact.iloc[0].minutes_ago} minutes ago')
        orders_fact = orders_fact.iloc[0].order_links.split(',')
        write_logs_out('DEBUG', 'Loaded orders fact from db')
    # with open('order_fact.pk', 'wb') as file:
    #     pickle.dump(orders_fact, file, protocol=pickle.HIGHEST_PROTOCOL)
    # with open('order_fact.pk', 'rb') as file:
    #     orders_fact = pickle.load(file)
    # print(orders_fact)
    # print(orders.order_link.iloc[0])
    isin_orders = set(orders_fact) & set(orders.order_link)
    not_isin = set(orders_fact) - set(orders.order_link)
    print(len(isin_orders))
    print(len(not_isin))
    print('isin')
    for n in isin_orders:
        print(n)
    print('\n\n\n')
    print('notisin')
    for n in not_isin:
        print(n)
    # print()
    # print('not isin')
    # print(orders[~orders.order_link.isin(orders_fact)])
    orders = orders[orders.order_link.isin(orders_fact)]

    write_logs_out('INFO', f'Orders fact count: {len(orders_fact)}\n'
                           f'Orders included in input: {orders.shape[0]}')

    orders = orders[orders.active==True]
    write_logs_out('INFO', f'Active orders count: {orders.shape[0]}')

    last_scans = pd.read_sql(f'select * from _{customer_id}_scan_event order by created_at desc limit {orders.shape[0] * 2}', db)
    last_scans['priority'] = list(range(1, len(last_scans) + 1))
    last_scans.drop_duplicates('order_link', keep='first', inplace=True)
    idx = np.linspace(0, orders.shape[0], num_tabs + 1 - reserve_tab)
    # tab_status = [[0, -1, 0, 0]] * num_tabs  # order_n, curr_phase, next_phase, time_elps
    mini_orders_all = []
    for i in range(len(driver.window_handles) - reserve_tab):
        mini_order = orders.iloc[int(idx[i]):int(idx[i + 1])]
        mini_order = mini_order.merge(last_scans, on='order_link', how='left')
        mini_order['priority'].fillna(float('inf'), inplace=True)
        mini_order.sort_values(by='priority', ascending=False, inplace=True)
        mini_order.reset_index(drop=True, inplace=True)
        mini_orders_all.append(mini_order)
        # min_iter_no = min(mini_orders_all[-1].ITER_NO)
        # max_iter_no = max(mini_orders_all[-1].ITER_NO)
        # if max_iter_no - min_iter_no > 1:
        #     write_logs_out(f'Divergence in iter_no for {int(idx[i])}:{int(idx[i + 1])}')
        #     raise Exception(f'Divergence in iter_no for {int(idx[i])}:{int(idx[i + 1])}')
        # if min_iter_no == max_iter_no:
        #     write_logs_out(f'New cycle for {int(idx[i])}:{int(idx[i + 1])}')
        # else:
        #     mini_orders_all[-1] = mini_orders_all[-1][mini_orders_all[-1].ITER_NO == min_iter_no]
        #     write_logs_out(
        #         f'Continuing prev cycle for {int(idx[i])}:{int(idx[i + 1])}: smallest iterated orders len = {len(orders)}')
    write_logs_out('DEBUG', f'mini_orders sizes {[mo.shape[0] for mo in mini_orders_all]}')
    return mini_orders_all


# def parse_prices():
#     next_pressed, prices = get_price_rows()
#     if len(prices) == 0:
#         next_pressed, prices = get_price_rows()
#     return next_pressed, prices

tab_timeout_on = False


def check_tab_timeout():
    global tab_timeout_on
    tab_timeout_on = False
    time.sleep(5)
    tab_timeout_on = True
    write_logs_out('DEBUG', 'Timeout checker is on')

    def check_tab_timeout_helper():
        while tab_timeout_on:
            if (not im_fact_updater and any(time.time() - tab_status.start_t > timeout_tab)) or \
                    (im_fact_updater and any(time.time() - tab_status.iloc[1:].start_t > timeout_tab)):
                write_logs_out('ERROR', 'Timeout at tab')
                if im_fact_updater:
                    timeout_tabs = time.time() - tab_status.iloc[1:].start_t > timeout_tab
                else:
                    timeout_tabs = time.time() - tab_status.start_t > timeout_tab
                for j in list(timeout_tabs.index.values):
                    timeout_t = timeout_tabs.loc[j].iloc[0]
                    if timeout_t:
                        write_logs_out('DEBUG', f'timeout at tab {j}')
                        change_tab_status(j, action='None', start_t=True)
                        write_logs_out('DEBUG', tab_status.to_string())
                # else:
                #     exit_handler()
                #     os._exit(os.EX_OK)
            time.sleep(3)
    th = Thread(target=check_tab_timeout_helper)
    th.start()


def stop_tab_timeout():
    global tab_timeout_on
    tab_timeout_on = False
    time.sleep(5)

# if __name__ == '__main__':
#     create_driver()
#     init_city()
#     time.sleep(10)
#     for page_button in select_by_class('pagination__el'):
#         if page_button.get_attribute('innerText') == '2':
#             click_mouse()
#
# exit()

im_fact_updater = False
indexing_page_fail_cnt = defaultdict(int)


def count_index_fails():
    # ['index_orders', 'open_zakazy', 'index_page1']

    global indexing_page_fail_cnt
    indexing_page_fail_cnt[tab_status.loc[i, 'action']] += 1
    # write_logs_out('DEBUG', 'ERROR at indexing')
    write_logs_out('DEBUG', 'ERROR at indexing: ' + str(indexing_page_fail_cnt))
    if indexing_page_fail_cnt[tab_status.loc[i, 'action']] > 5:
        if 'goto' in tab_status.loc[i, 'action']:
            stuck_page = int(tab_status.loc[i, 'strings'].split(',')[0].split('_')[1])
        elif 'index_page' in tab_status.loc[i, 'action']:
            stuck_page = int(tab_status.loc[i, 'action'].split('_')[1].split('e')[1])
        elif tab_status.loc[i, 'action'] in ('open_zakazy', 'index_orders'):
            stuck_page = None
        else:
            raise Exception(f"Unknown action {tab_status.loc[i, 'action']}")

        if 'index_page' in indexing_page_fail_cnt[tab_status.loc[i, 'action']] and stuck_page != 1:
            change_tab_status(i, action='index_orders', start_t=True,
                              strings=f'goto_{stuck_page},' + tab_status.loc[i, 'strings'], status='pending')
        else:
            change_tab_status(i, action='index_orders', start_t=True, strings='')
        indexing_page_fail_cnt = defaultdict(int)


if __name__ == '__main__':
    start_time = 0
    # cx_Oracle.init_oracle_client(config_dir=config.wallet_dir,
    #                              lib_dir=config.db_lib_dir)

    while True:
        init_vars()
        create_driver()
        # db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')
        db = pg.connect(user=config.db_user,
                        password=config.db_pass,
                        database=config.db,
                        host=config.host,
                        port=config.port)
        login()
        open_new_tabs()

        start_time = time.time()
        # db_inited = init_orders_table()
        # if db_inited:
        #     write_logs_out('DEBUG', 'Tables init success')
        # else:
        #     write_logs_out('FATAL', 'Tables init fail')
        mini_orders_all = prepare_orders()
        if not city_inited:
            init_city()
            city_inited = True
        while time.time() - start_time < timeout_restart:
            write_logs_out('DEBUG', str(tab_status[['idx', 'action', 'status']]))
            for i, h in enumerate(driver.window_handles):
                try:
                    driver.switch_to.window(h)
                    if (tab_status.iloc[0].action in ('index_orders', 'open_zakazy')
                            or 'index_page' in tab_status.iloc[0].action):
                        if i != 0:
                            mini_orders = mini_orders_all[i - 1]
                            curr_order_link = mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].order_link
                        else:
                            curr_order_link = 'indexing'
                    else:
                        mini_orders = mini_orders_all[i]
                        curr_order_link = mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].order_link
                    # curr_order_active = mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].active
                    # if not curr_order_active:
                    #     tab_status.loc[i, 'idx'] += 1
                    #     continue
                    if tab_status.loc[i, 'action'] == 'None':
                        # start cycle
                        driver.get(curr_order_link)
                        change_tab_status(i, action='open_order', status='pending', start_t=True)
                    elif tab_status.loc[i, 'action'] == 'open_order':
                        if tab_status.loc[i, 'status'] == 'pending':
                            if page_is_loaded():
                                change_tab_status(i, status='success')
                        if tab_status.loc[i, 'status'] == 'success':
                            change_tab_status(i, action='pricep_1', status='pending')
                            next_pressed, prices = get_price_rows()
                            if next_pressed:
                                if prices:
                                    action = tab_status.loc[i]['action']
                                    action_no = int(action.split('_')[1])
                                    change_tab_status(i, action=f'pricep_{action_no + 1}', status='pending',
                                                      strings=json.dumps(prices))
                                else:
                                    raise Exception('Next pressed, but prices is None')
                            else:
                                if prices:
                                    write_prices(prices)
                                    # change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                                    change_tab_status(i, action='process_order', status='pending',
                                                      strings=json.dumps(prices))
                                else:
                                    driver.refresh()
                                    change_tab_status(i, action='reload', status='pending')
                                    # raise Exception('No any seller')
                    elif tab_status.loc[i, 'action'] == 'reload':
                        if page_is_loaded():
                            change_tab_status(i, action='open_order', status='pending')
                    elif 'pricep' in tab_status.loc[i, 'action']:
                        next_pressed, prices = get_price_rows()
                        # if len(prices) == 0:
                        #     next_pressed, prices = get_price_rows()
                        if next_pressed:
                            if prices:
                                action = tab_status.loc[i]['action']
                                action_no = int(action.split('_')[1])
                                change_tab_status(i, action=f'pricep_{action_no + 1}', status='pending',
                                                  strings=tab_status.loc[i]['strings'] + '|+|' + json.dumps(prices))
                            else:
                                raise Exception('Next pressed, but prices is None')
                        else:
                            if prices:
                                strings = tab_status.loc[i]['strings']
                                strings = strings.split('|+|')
                                strings = [json.loads(s) for s in strings]
                                res = {}
                                for p in strings + [prices]:
                                    # try:
                                    res.update(p)
                                    # except Exception as e:
                                    #     write_logs_out('SPECIAL', f'p: {p}\n'
                                    #                               f'strings: {strings}\n'
                                    #                               f'prices: {prices}')
                                    #     raise e
                                write_prices(res)
                                # change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                                change_tab_status(i, action=f'process_order', status='pending',
                                                  strings=json.dumps(res))
                            else:
                                action = tab_status.loc[i]['action']
                                action_no = action.split('_')[1]
                                # refresh_at_page(action_no)
                                driver.refresh()
                                change_tab_status(i, action=f'reloadp_{action_no}', status='pending')
                    elif 'reloadp' in tab_status.loc[i, 'action']:
                        action = tab_status.loc[i]['action']
                        action_no = action.split('_')[1]
                        if page_is_loaded():
                            for page_button in select_by_class('pagination__el'):
                                if page_button.get_attribute('innerText') == action_no:
                                    click_mouse()
                            change_tab_status(i, action=f'pricep_{action_no}', status='pending')
                    elif tab_status.loc[i, 'action'] == 'process_order':
                        if tab_status.loc[i, 'status'] == 'pending':
                            prices = json.loads(tab_status.loc[i]['strings'])
                            write_logs_out('DEBUG', json.dumps(prices), write_db=False)
                            im_seller = any([my_id in p for p in prices])
                            if im_seller:
                                my_rank = [i for i, k in enumerate(prices) if my_id in k][0]
                                my_curr_price = prices[my_link][1]
                                min_price = mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].min_price

                                cursor = db.cursor()
                                # cursor.execute(f"""merge into current_price_status_{customer_id} tgt
                                #                 using (select :1 order_link, :2 curr_rank, :3 curr_price, :4 min_price from dual) src
                                #                 on (src.order_link = tgt.order_link)
                                #                 when matched then
                                #                 update set tgt.curr_rank = src.curr_rank, tgt.curr_price = src.curr_price, tgt.min_price = src.min_price
                                #                 when not matched then
                                #                 insert (order_link, curr_rank, curr_price, min_price)
                                #                 values (src.order_link, src.curr_rank, src.curr_price, src.min_price)""",
                                #                (curr_order_link, my_rank + 1, my_curr_price, int(min_price)))
                                cursor.execute(f"""insert into _{customer_id}_current_price_status 
                                                    (order_link, curr_rank, curr_price, min_price) values 
                                                    (%s, %s, %s, %s) on conflict (order_link) do update set 
                                    (curr_rank, curr_price, min_price) = (EXCLUDED.curr_rank, EXCLUDED.curr_price, EXCLUDED.min_price)""",
                                               (curr_order_link, my_rank + 1, my_curr_price, int(min_price)))
                                db.commit()
                                cursor.close()

                                top1_price = prices[list(prices)[0]][1]
                                top2_price = prices[list(prices)[1]][1]

                                if my_rank != 0:
                                    desired_price = top1_price - price_step
                                else:
                                    desired_price = top2_price - price_step
                                desired_price = max(min_price, desired_price)
                                write_logs_out('DEBUG',
                                               f'Curr rank {my_rank + 1}\n'
                                               f'Curr price {my_curr_price}\n'
                                               f'Min price {min_price}\n'
                                               f'Desired price {desired_price}\n'
                                               f'Top1 price {top1_price}\n'
                                               f'Top2 price {top2_price}')
                                if desired_price == min_price:
                                    write_logs_out('DEBUG', 'Desired price = min price')
                                if desired_price == my_curr_price:
                                    write_logs_out('DEBUG', 'Already desired price')
                                    change_tab_status(i, action='None', idx=tab_status.loc[i]['idx'] + 1)
                                else:
                                    link = f"https://kaspi.kz/merchantcabinet/#/offers/edit/{curr_order_link.split('-')[-1]}_{my_id}"
                                    driver.get(link)
                                    # got_page = False
                                    # while not got_page:
                                    #     success1 = wait_till_load_by_text('Заказы')
                                    #     if success1:
                                    #         driver.get(link)
                                    #     else:
                                    #         continue
                                    #     got_page = wait_till_load_by_text('Редактирование товара')
                                    # radios = select_by_class('form__radio-title')
                                    # pass
                                    change_tab_status(i, action='process_order2', status='pending',
                                                      strings=link + '|+|' + str(desired_price))
                            else:
                                # change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                                raise Exception('I am not seller')
                    elif tab_status.iloc[i]['action'] == 'process_order2':
                        if tab_status.iloc[i]['status'] == 'pending':
                            # 'main-nav__el-link'
                            try:
                                success1 = wait_till_load_by_text('Заказы', t=0.5)
                                if success1:
                                    change_tab_status(i, status='success')
                            except TimeoutException:
                                write_logs_out('ERROR', 'ZAKAZY NOT LOADED at process_order2')
                        if tab_status.loc[i]['status'] == 'success':
                            link = tab_status.loc[i]['strings'].split('|+|')[0]
                            driver.get(link)
                            change_tab_status(i, action='process_order3', status='pending')
                    elif tab_status.loc[i]['action'] == 'process_order3':
                        if tab_status.loc[i]['status'] == 'pending':
                            try:
                                got_page = wait_till_load_by_text('Редактирование товара', t=0.5)
                                if got_page:
                                    change_tab_status(i, status='success')
                                    write_logs_out('DEBUG', 'Редактирование товара loaded')
                                else:
                                    write_logs_out('ERROR', 'UNKNOWN ERROR at process_order3')
                            except TimeoutException:
                                write_logs_out('ERROR', 'REDAKTIROVANIE TOVARA NOT LOADED at process_order3')
                        if tab_status.loc[i]['status'] == 'success':
                            was_exception = False
                            try:
                                new_price = tab_status.loc[i]['strings'].split('|+|')[1]
                                radios = select_by_class('form__radio-title')

                                for radio in radios:
                                    if radio.text == 'Одна для всех городов':
                                        break
                                click_mouse()
                                fill_by_class('form__col', new_price)

                                buttons = select_by_tag('button')
                                for button in buttons:
                                    if button.text == 'Сохранить':
                                        break
                                press_enter()
                                cursor = db.cursor()
                                cursor.execute(f"""UPDATE _{customer_id}_CURRENT_PRICE_STATUS 
                                               SET NEXT_PRICE = %s, updated_at = now()
                                               WHERE ORDER_LINK = %s""", (int(new_price), curr_order_link))
                                write_logs_out('DEBUG', f'Updating price to {new_price}')
                                db.commit()
                                cursor.close()
                                change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                            except ElementClickInterceptedException:
                                was_exception = True
                                new_price = tab_status.loc[i]['strings'].split('|+|')[1]
                                write_logs_out('ERROR', f'Fail while updating price to {new_price}')
                            if was_exception:
                                try:
                                    curtain = WebDriverWait(driver, 1).until(EC.visibility_of_element_located(
                                        (By.CLASS_NAME, 'ks-gwt-dialog _small g-ta-c')))
                                    button = WebDriverWait(driver, 1).until(
                                        EC.element_to_be_clickable((By.CLASS_NAME, 'gwt-Button button')))
                                    button.click()
                                    write_logs_out('DEBUG', 'Curtain clicked')
                                except TimeoutException:
                                    write_logs_out('ERROR', 'Fail to click curtain but page is stale')
                                    write_logs_out('ERROR', traceback.format_exc())
                                    raise TimeoutException()
                    elif tab_status.loc[i, 'action'] == 'index_orders':
                        driver.get("https://kaspi.kz/merchantcabinet/#/offers")
                        time.sleep(1)
                        driver.refresh()
                        if tab_status.loc[i, 'strings'] != '':
                            if 'goto' in tab_status.loc[i, 'strings'].split(',')[0]:
                                goto_page = tab_status.loc[i, 'strings'].split(',')[0]
                                write_logs_out('DEBUG', f'Opening offers page with goto {goto_page}')
                            else:
                                raise Exception(f"No goto in strings, but strings is not empty: {tab_status.loc[i, 'strings']}")
                        else:
                            change_tab_status(i, action='open_zakazy', status='pending', start_t=True)
                    elif tab_status.loc[i, 'action'] == 'open_zakazy':
                        loaded = wait_till_load_by_text('Заказы')
                        write_logs_out('DEBUG', f'PAGE LOAD {loaded}')

                        if loaded:
                            change_tab_status(i, status='success')
                            write_logs_out('DEBUG', 'Opened zakazy')
                        else:
                            count_index_fails()
                            continue

                        if tab_status.loc[i, 'status'] == 'success':
                            # success2 = wait_till_load_by_text('Заказы')
                            # write_logs_out('DEBUG', f'Tried zakazy {success2}')
                            try:
                                element = WebDriverWait(driver, 5).until(EC.visibility_of_element_located(
                                    (By.XPATH, f"//a[@class='main-nav__el-link'][@href='#/offers']")))
                                write_logs_out('DEBUG', f'Found tovary button')
                                actions = ActionChains(driver)
                                actions.move_to_element(element)
                                actions.perform()
                                element = WebDriverWait(driver, 5).until(EC.visibility_of_element_located(
                                    (By.XPATH, f"//a[@class='main-nav__sub-el-link'][@href='#/offers']")))
                                write_logs_out('DEBUG', f'Found button upravlenie tovarami')
                                element.click()
                                # TODO: consider goto
                                if tab_status.loc[i, 'strings'] == '':
                                    change_tab_status(i, action='index_page1', status='pending', start_t=True)
                                else:
                                    goto_page = int(tab_status.loc[i, 'strings'].split(',')[0].split('_')[1])
                                    change_tab_status(i, action='goto_2', status='pending', start_t=True)
                            except:
                                count_index_fails()
                                continue
                            # time.sleep(10)
                    elif 'index_page' in tab_status.loc[i, 'action']:
                        curr_page = int(tab_status.loc[i, 'action'].split('_')[1].split('e')[1])
                        if tab_status.loc[i, 'status'] == 'pending':
                            success1 = True
                            if curr_page == 1:
                                success1 = wait_till_load_by_text('Управление товарами')
                                write_logs_out('DEBUG', f'Search Управление товарами {success1}')
                            if success1:
                                try:
                                    page_loaded = wait_till_load_by_text(' из ', t=0.2)
                                    write_logs_out('DEBUG', f'iz is loaded {page_loaded}')
                                    if page_loaded:
                                        change_tab_status(i, status='success')
                                    else:
                                        count_index_fails()
                                        continue
                                except:
                                    count_index_fails()
                                    continue
                            else:
                                count_index_fails()
                                continue
                                # change_tab_status(i, action='index_orders', status='pending', start_t=True)
                        if tab_status.loc[i, 'status'] == 'success':
                            if curr_page == 1:
                                try:
                                    # button = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, 'gwt-Button button')))
                                    curtain = WebDriverWait(driver, 5).until(
                                        EC.visibility_of_element_located((By.CLASS_NAME, 'ks-gwt-dialog _small g-ta-c')))
                                    button = WebDriverWait(driver, 5).until(
                                        EC.element_to_be_clickable((By.CLASS_NAME, 'gwt-Button button')))
                                    button.click()
                                    write_logs_out('DEBUG', 'Clicked SERVICE_NEDOSTUPEN_BUTTON')
                                except TimeoutException:
                                    write_logs_out('DEBUG', 'NO SERVICE_NEDOSTUPEN_BUTTON')
                                links = []
                            else:
                                links = tab_status.loc[i, 'strings'].split(',')

                            try:
                                rows = []
                                while len(rows) == 0:
                                    rows = list(select_by_class('offer-managment__product-cell-link'))
                                    time.sleep(1)
                                curr_page_links = [el.get_attribute('href')[:-1] for el in rows]
                                # links = list(set(links))
                            except:
                                count_index_fails()
                                continue
                            write_logs_out('DEBUG', f'index_orders len links {len(set(links + curr_page_links))} page {curr_page}')
                            # write_logs_out('DEBUG', json.dumps(links), write_db=False)
                            # for el in rows:
                            # print(thread_name, el)
                            # el.get_attribute('href')
                            # press_enter()

                            finished = False
                            page_info = list(select_by_attr('div', 'class', 'gwt-HTML'))[-1].text
                            if page_info != '':
                                page_info = page_info.split()
                                finished = page_info[0].split('-')[1] == page_info[-1]
                                if finished:
                                    # print(thread_name, 'after_finished', rows)
                                    try:
                                        # print(thread_name, 'after_finished', [r.get_attribute('href') for r in rows])
                                        [r.get_attribute('href') for r in rows]
                                    except:
                                        finished = False
                                        # print(thread_name, 'after_failure')
                                    # print(thread_name, '---')
                            else:
                                count_index_fails()
                                continue
                            if not finished:
                                if len(set(links + curr_page_links)) / 10 == curr_page:
                                    links = set(links + curr_page_links)
                                    write_logs_out('DEBUG', f'links num is divisible by 10 (len {len(links)} / page {curr_page}) -> pressing next')
                                    try:
                                        while not list(select_by_attr('img', 'aria-label', 'Next page')):
                                            time.sleep(0.1)
                                        next_button = WebDriverWait(driver, 5).until(
                                            EC.element_to_be_clickable((By.XPATH, f"//img[@aria-label='Next page']")))
                                        next_button.click()
                                        change_tab_status(i, action=f'index_page{curr_page + 1}', status='pending', start_t=True,
                                                          strings=','.join(links))
                                    except:
                                        count_index_fails()
                                        continue
                                else:
                                    write_logs_out('DEBUG', f'links num NOT divisible by 10 (len {len(set(links + curr_page_links))} / page {curr_page}) -> try page again')
                                    count_index_fails()
                                    continue
                            else:
                                links = list(set(links + curr_page_links))
                                write_logs_out('DEBUG', f'Len of links is {len(links)}')
                                old_order_fact = psql.read_sql(f'select * from _{customer_id}_order_fact', db)
                                write_logs_out('DEBUG', f"Len of old links is "
                                                        f"{len(old_order_fact.iloc[0].order_links.split(','))}")
                                cursor = db.cursor()
                                # cursor.execute(f'truncate _{customer_id}_order_fact')
                                cursor.execute(f'insert into _{customer_id}_order_fact (order_count, order_links) '
                                               f'values (%s, %s)',
                                               (len(links), ','.join(links),))
                                db.commit()
                                cursor.close()
                                write_logs_out('DEBUG', 'order fact updated -> exit()')
                                sys.exit()
                    elif 'goto' in tab_status.loc[i, 'action']:
                        curr_goto_page = int(tab_status.loc[i, 'action'].split('_')[1])  # not went yet (starts with 2)
                        target_goto_page = int(tab_status.loc[i, 'strings'].split(',')[0].split('_')[1])

                        if tab_status.loc[i, 'status'] == 'pending':
                            success1 = True
                            if curr_goto_page == 1:
                                success1 = wait_till_load_by_text('Управление товарами')
                                write_logs_out('DEBUG', f'Search Управление товарами {success1}')
                            if success1:
                                try:
                                    page_loaded = wait_till_load_by_text(' из ', t=0.2)
                                    write_logs_out('DEBUG', f'iz is loaded {page_loaded}')
                                    if page_loaded:
                                        change_tab_status(i, status='success')
                                    else:
                                        count_index_fails()
                                        continue
                                except:
                                    count_index_fails()
                                    continue
                            else:
                                count_index_fails()
                                continue
                    else:
                        # print(tab_status.loc[i, 'action'])
                        raise NotImplementedError(f"Not implemented action {tab_status.loc[i, 'action']}")
                except Exception as e:
                    # if i != 0:
                    if im_fact_updater and i == 0:
                        change_tab_status(i, action='index_orders', start_t=True, strings='')
                        write_logs_out('DEBUG', 'UNKNOWN ERROR AT INDEXING')
                    else:
                        change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                    write_logs_out('FATAL', traceback.format_exc())
                # time.sleep(1)

            if tab_status.iloc[0].action == 'open_zakazy':
                im_fact_updater = True
            if not tab_timeout_on:
                check_tab_timeout()
        stop_tab_timeout()
        exit_handler()

# selenium.common.exceptions.ElementClickInterceptedException: Message: Element <label class="form__radio-title"> is not clickable at point (511,611) because another element <div class="ks-gwt-dialog _small g-ta-c"> obscures it
# https://kaspi.kz/shop/p/artel-dolce-21-ex-belyi-2602172/?c=750000000

# https://kaspi.kz/shop/p/ardesto-kastrjulja-ar1922as-stal-2-2-l-105339039,https://kaspi.kz/shop/p/samsung-ar12txhqasinua-belyi-104750094,https://kaspi.kz/shop/p/ardesto-kastrjulja-gemini-salerno-ar1908cs-stal-0-8-l-105363172,https://kaspi.kz/shop/p/ardesto-nozh-gemini-como-ar1906ck-6-sht-stal--105314266