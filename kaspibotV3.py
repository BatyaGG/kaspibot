import pickle
import re
import signal
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
from psycopg2.extras import RealDictCursor
from selenium.webdriver import DesiredCapabilities

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
print(os.path.abspath(os.getcwd()))

from status_codes import *
# sys.path.append('Customer_data')
# from Customer_data.Customers import customers

merchant_id = config.merchant_id
num_tabs = config.num_tabs
timeout_tab = config.timeout_tab
timeout_restart = config.timeout_end
price_step = config.price_step

kaspi_login = None
kaspi_password = None
my_link = None
my_id = None

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
    os.kill(os.getpid(), signal.SIGKILL)
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
    caps["pageLoadStrategy"] = 'none'
    options = Options()
    options.headless = config.headless
    options.page_load_strategy = 'none'
    driver = webdriver.Firefox(options=options,
                               # firefox_profile=fp,
                               capabilities=caps,
                               # executable_path='/main/drivers/geckodriver'
                               )


def open_new_tabs():
    for i in range(num_tabs - 1):
        driver.switch_to.new_window('TAB')
    write_logs_out('DEBUG', OPEN_TABS_SUCCESS, f'Opened new {num_tabs} tabs')


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

        fill_by_name('username', kaspi_login)
        fill_by_name('password', kaspi_password)
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


def write_logs_out(lvl, status_code, text, write_db=True):
    global curr_order_link
    print('_______________________')
    print(curr_order_link)
    print(lvl)
    print(text)
    print()
    if write_db:
        cursor = db.cursor()
        cursor.execute(f"INSERT INTO LOGS (MERCHANT_ID, ORDER_LINK, LOG_LEVEL, LOG_STATUS, LOG_TEXT) "
                       "VALUES (%s, %s, %s, %s, %s) ", (merchant_id, curr_order_link, lvl, status_code, text))
        db.commit()
        cursor.close()


def page_is_loaded():
    try:
        WebDriverWait(driver, 0.2).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h2')))
        # WebDriverWait(driver, 0.2).until(EC.element_to_be_clickable((By.CLASS_NAME, 'topbar__logo')))
        return True
    except:
        return False


def get_price_rows():
    prices = {}
    no_fail = False
    # while True:
    while not no_fail:
        try:
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'sellers-table__buy-cell-button')))
            no_fail = True
        except TimeoutException:
            write_logs_out('DEBUG', SELLERS_TABLE_ERROR, f'Seller table load fail:\n {traceback.format_exc()}')
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
                    write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, f'Row no {j}')
                rr = row.get_attribute('innerText')
                if write_detailed_logs:
                    write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, f'Row no {j}: innerText={rr}')
                inner_html = row.get_attribute('innerHTML')
                if write_detailed_logs:
                    write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, f'Row no {j}: innerHTML={inner_html}')
                if rr is None or inner_html is None:
                    raise StaleElementReferenceException('rr or inner_html is None')
                # print(rr)
            except StaleElementReferenceException:
                was_exception = True
                write_logs_out('DEBUG', PRICE_PAGE_STALE, traceback.format_exc())
                break
            if 'Выбрать' not in rr:
                if write_detailed_logs:
                    write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, f'Row no {j}: Выбрать not in rr')
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
                write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, f'NO EXCEPTION FOR ALL PAGES')
            rows_scanned = True
        else:
            if write_detailed_logs:
                write_logs_out('DEBUG', DETAILED_LOGS_PAGENUM, 'EXCEPTION FOR PAGE')
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
                write_logs_out('DEBUG', PRESS_NEXT_PAGE_ERROR, traceback.format_exc())
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


def fill_by_class(name, fill):
    classes = select_by_class(name)
    for c in classes:
        if c.get_attribute('class') == 'form__col _12-12 _medium_6-12':
            break
    elem[0].clear()
    elem[0].send_keys(fill)


def change_tab_status(i, idx=None, action=None, status=None, start_t=False, strings=None):
    # write_logs_out('DEBUG', CHANGE_TAB_STATUS,
    #                f'change_tab_status \ni={i} \nidx={idx} \naction={action} \nstatus={status} '
    #                f'\nstart_t={start_t}', write_db=False)
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
    write_logs_out('DEBUG', BOT_WRITING_PRICES, f'wrote prices, len prices = {len(prices)}')
    prices_df = pd.DataFrame({'seller_link': list(prices.keys()),
                              'seller_name': [n for n, _ in prices.values()],
                              'seller_price': [p for _, p in prices.values()]})

    cursor = db.cursor()
    cursor.execute(
        f"INSERT INTO scan_event (merchant_id, order_link, sellers_links, sellers_names, sellers_prices) "
        "VALUES (%s, %s, %s, %s, %s)", (merchant_id,
                                        str(mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].order_link),
                                        ','.join(prices_df.seller_link.values),
                                        ','.join(prices_df.seller_name.values),
                                        ','.join(prices_df.seller_price.astype('str').values)))
    db.commit()
    cursor.close()


def prepare_orders():
    orders = psql.read_sql(f'SELECT * from order_table where merchant_id = {merchant_id}', db)
    orders = orders.sample(frac=1)
    # orders_fact = psql.read_sql(f'select Extract(epoch from (now() - created_at)/60) as minutes_ago, order_links '
    #                             f'from order_fact where merchant_id = {merchant_id} order by created_at desc limit 1', db)
    orders_fact = list(psql.read_sql(f'select * from order_fact where merchant_id = {merchant_id}', db).order_link.values)
    # orders_fact = orders_fact.iloc[0].order_links.split(',')
    write_logs_out('INTO', BOT_INFO, 'Loaded orders fact from db')
    orders = orders[orders.order_link.isin(orders_fact)]

    write_logs_out('INFO', BOT_INFO,
                   f'Orders fact count: {len(orders_fact)}\n'
                   f'Orders included in input: {orders.shape[0]}')

    orders = orders[orders.active==True]
    write_logs_out('INFO', BOT_INFO,
                   f'Active orders count: {orders.shape[0]}')

    last_scans = pd.read_sql(f'select * from scan_event where merchant_id = {merchant_id} order by created_at desc limit {orders.shape[0] * 2}', db)
    last_scans['priority'] = list(range(1, len(last_scans) + 1))
    last_scans.drop_duplicates('order_link', keep='first', inplace=True)
    idx = np.linspace(0, orders.shape[0], num_tabs + 1)
    # tab_status = [[0, -1, 0, 0]] * num_tabs  # order_n, curr_phase, next_phase, time_elps
    mini_orders_all = []
    for i in range(len(driver.window_handles)):
        mini_order = orders.iloc[int(idx[i]):int(idx[i + 1])]
        mini_order = mini_order.merge(last_scans, on='order_link', how='left')
        mini_order['priority'].fillna(float('inf'), inplace=True)
        mini_order.sort_values(by='priority', ascending=False, inplace=True)
        mini_order.reset_index(drop=True, inplace=True)
        mini_orders_all.append(mini_order)
    write_logs_out('DEBUG', MINI_ORDERS_SHAPES, f'mini_orders sizes {[mo.shape[0] for mo in mini_orders_all]}')
    return mini_orders_all


tab_timeout_on = False

tab_timeout_dict = []
for _ in range(num_tabs):
    tab_timeout_dict.append({'prev': 0, 'last_change': time.time()})


def check_tab_timeout():
    global tab_timeout_on
    tab_timeout_on = False
    time.sleep(5)
    tab_timeout_on = True
    write_logs_out('DEBUG', TIMEOUT_CHECKER_ON, 'Timeout checker is on')

    def check_tab_timeout_helper():
        cnt = 1
        while tab_timeout_on:
            for j in range(num_tabs):
                idc = tab_status.iloc[j].idx
                if idc != tab_timeout_dict[j]['prev']:
                    tab_timeout_dict[j]['last_change'] = time.time()
                tab_timeout_dict[j]['prev'] = idc

                if time.time() - tab_timeout_dict[j]['last_change'] > timeout_tab:
                    write_logs_out('DEBUG', TIMEOUT_AT_TAB, f"timeout at tab {j} \ntdiff: {int(time.time() - tab_timeout_dict[j]['last_change'])} secs")
                    change_tab_status(j, action='None', start_t=True, idx=tab_status.loc[j]['idx'] + 1)

            if cnt % 60 == 0:
                crs = db.cursor(cursor_factory=RealDictCursor)
                crs.execute('select Extract(epoch from (now() - created_at)/60) as minutes_ago '
                            'from scan_event order by created_at desc limit 1')
                rec = crs.fetchone()['minutes_ago']
                if rec > 2:
                    stop_tab_timeout()
                    write_logs_out('DEBUG', BOT_KILL_TIMEOUT, f'{time.time() - start_time} seconds after start')
                    os.kill(os.getpid(), signal.SIGKILL)
            # t_diff = time.time() - tab_status.start_t > timeout_tab
            # if any(t_diff):
            #     write_logs_out('ERROR', TIMEOUT_AT_TAB, f'Timeout at tab {t_diff}')
            #     timeout_tabs = time.time() - tab_status.start_t > timeout_tab
            #     for j in list(timeout_tabs.index.values):
            #         timeout_t = timeout_tabs.loc[j].iloc[0]
            #         if timeout_t:
            #             write_logs_out('DEBUG', TIMEOUT_AT_TAB, f'timeout at tab {j}')
            #             change_tab_status(j, action='None', start_t=True, idx=tab_status.loc[j]['idx'] + 1)
                # else:
                #     exit_handler()
                #     os._exit(os.EX_OK)
            time.sleep(3)
            cnt += 1
    th = Thread(target=check_tab_timeout_helper)
    th.start()


def stop_tab_timeout():
    global tab_timeout_on
    tab_timeout_on = False
    time.sleep(5)


def init_kaspi_vars():
    global kaspi_login, kaspi_password, my_link, my_id
    cursor = db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f'select * from merchants where merchant_id = {merchant_id}')
    rec = cursor.fetchone()
    kaspi_login = rec['kaspi_login']
    kaspi_password = rec['kaspi_password']
    my_link = rec['address_tab']
    my_id = my_link.split('/')[-3]


if __name__ == '__main__':
    start_time = 0

    init_vars()
    create_driver()
    # db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')
    db = pg.connect(user=config.db_user,
                    password=config.db_pass,
                    database=config.db,
                    host=config.host,
                    port=config.port)
    init_kaspi_vars()

    login()
    open_new_tabs()

    start_time = time.time()
    mini_orders_all = prepare_orders()
    if not city_inited:
        init_city()
        city_inited = True
    while time.time() - start_time < timeout_restart:
        print(tab_status[['idx', 'action', 'status']])
        for i, h in enumerate(driver.window_handles):
            try:
                driver.switch_to.window(h)
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
                                raise Exception('Next pressed, but prices is None')  # Never occurred
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
                            raise Exception('Next pressed, but prices is None')  # Never occurred
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
                        # write_logs_out('DEBUG', json.dumps(prices), write_db=False)
                        im_seller = any([my_id in p for p in prices])
                        if im_seller:
                            my_rank = [i for i, k in enumerate(prices) if my_id in k][0]
                            my_curr_price = prices[my_link][1]
                            min_price = mini_orders.iloc[tab_status.loc[i, 'idx'] % mini_orders.shape[0]].min_price
                            is_alone = len(prices) == 1
                            cursor = db.cursor()
                            cursor.execute(f"""insert into order_status 
                                                (merchant_id, order_link, ranking, curr_price, min_price, is_alone, scanned_at) values 
                                                (%s, %s, %s, %s, %s, %s, now()) on conflict (merchant_id, order_link) do update set 
                                    (ranking, curr_price, min_price, is_alone, scanned_at) = (EXCLUDED.ranking, EXCLUDED.curr_price, EXCLUDED.min_price, EXCLUDED.is_alone, EXCLUDED.scanned_at)""",
                                           (merchant_id, curr_order_link, my_rank + 1, my_curr_price, int(min_price), is_alone))
                            db.commit()
                            cursor.close()

                            top1_price = prices[list(prices)[0]][1]
                            if len(prices) > 1:
                                top2_price = prices[list(prices)[1]][1]
                            else:
                                top2_price = -top1_price
                            if my_rank != 0:
                                desired_price = top1_price - price_step
                            else:
                                desired_price = top2_price - price_step
                                if desired_price < 0:
                                    write_logs_out('DEBUG', BOT_ALONE_SELLER, f'I AM ALONE SELLER')
                                    desired_price = my_curr_price
                            desired_price = max(min_price, desired_price)
                            # write_logs_out('DEBUG',
                            #                f'Curr rank {my_rank + 1}\n'
                            #                f'Curr price {my_curr_price}\n'
                            #                f'Min price {min_price}\n'
                            #                f'Desired price {desired_price}\n'
                            #                f'Top1 price {top1_price}\n'
                            #                f'Top2 price {top2_price}')
                            if desired_price == min_price:
                                write_logs_out('DEBUG', DESIRED_EQ_MIN, 'Desired price = min price')
                            if desired_price == my_curr_price:
                                write_logs_out('DEBUG', ALREADY_DES_PRICE, 'Already desired price')
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
                            write_logs_out('ERROR', ZAKAZY_NOTLOADED,
                                           f'ZAKAZY NOT LOADED at process_order2 {traceback.format_exc()}')
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
                                write_logs_out('DEBUG', REDAKTIROVANIE_LOADED, 'Редактирование товара loaded')
                            else:
                                write_logs_out('ERROR', UNKNOWN_ERROR_PROCESS_ORDER3,
                                               'UNKNOWN ERROR at process_order3')
                        except TimeoutException:
                            write_logs_out('ERROR', REDAKTIROVANIE_NOTLOADED3,
                                           'REDAKTIROVANIE TOVARA NOT LOADED at process_order3')
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
                            cursor.execute(f"""UPDATE ORDER_STATUS 
                                           SET NEXT_PRICE = %s, updated_at = now()
                                           WHERE merchant_id = %s and ORDER_LINK = %s""",
                                           (int(new_price), merchant_id, curr_order_link))
                            write_logs_out('DEBUG', UPDATING_PRICE, f'Updating price to {new_price}')
                            db.commit()
                            cursor.close()
                            change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                        except ElementClickInterceptedException:
                            was_exception = True
                            new_price = tab_status.loc[i]['strings'].split('|+|')[1]
                            write_logs_out('ERROR', UPDATING_PRICE_ERROR,
                                           f'Fail while updating price to {new_price}:\n {traceback.format_exc()}')
                        if was_exception:
                            try:
                                curtain = WebDriverWait(driver, 1).until(EC.visibility_of_element_located(
                                    (By.CLASS_NAME, 'ks-gwt-dialog _small g-ta-c')))
                                button = WebDriverWait(driver, 1).until(
                                    EC.element_to_be_clickable((By.CLASS_NAME, 'gwt-Button button')))
                                button.click()
                                write_logs_out('DEBUG', CURTAIN_CLICKED, 'Curtain clicked')
                            except TimeoutException:
                                write_logs_out('ERROR', CURTAIN_CLICK_ERROR,
                                               f'Fail to click curtain but page is stale:\n {traceback.format_exc()}')
                                raise TimeoutException()
                else:
                    # print(tab_status.loc[i, 'action'])
                    raise NotImplementedError(f"Not implemented action {tab_status.loc[i, 'action']}")
            except Exception as e:
                change_tab_status(i, idx=tab_status.loc[i]['idx'] + 1, action='None')
                write_logs_out('FATAL', BOT_ERROR, traceback.format_exc())
            # time.sleep(1)

        if not tab_timeout_on:
            check_tab_timeout()
    stop_tab_timeout()
    write_logs_out('DEBUG', BOT_END_SECONDS_AFTER, f'{time.time() - start_time} seconds after start')
    os.kill(os.getpid(), signal.SIGKILL)
# selenium.common.exceptions.ElementClickInterceptedException: Message: Element <label class="form__radio-title"> is not clickable at point (511,611) because another element <div class="ks-gwt-dialog _small g-ta-c"> obscures it
# https://kaspi.kz/shop/p/artel-dolce-21-ex-belyi-2602172/?c=750000000

# https://kaspi.kz/shop/p/ardesto-kastrjulja-ar1922as-stal-2-2-l-105339039,https://kaspi.kz/shop/p/samsung-ar12txhqasinua-belyi-104750094,https://kaspi.kz/shop/p/ardesto-kastrjulja-gemini-salerno-ar1908cs-stal-0-8-l-105363172,https://kaspi.kz/shop/p/ardesto-nozh-gemini-como-ar1906ck-6-sht-stal--105314266