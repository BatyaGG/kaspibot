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
from psycopg2.extras import RealDictCursor
from selenium.webdriver import ActionChains, DesiredCapabilities
from selenium.webdriver.support.ui import Select

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

from status_codes import *


merchant_id = config.merchant_id
driver = None
elem = []
city_inited = False

kaspi_login = None
kaspi_password = None
my_link = None
my_id = None


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

def init_vars():
    global driver, elem, city_inited
    driver = None
    elem = []
    city_inited = False


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


def wait_next(tag_name, attr_name, attr):
    global elem
    el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable(
        (By.XPATH, f"//{tag_name}[@{attr_name}='{attr}']")))
    if len(elem) > 0:
        elem.pop()
    elem.append(el)


def select_by_class(name):
    global elem
    elems = driver.find_elements_by_class_name(name)
    for el in elems:
        if len(elem) > 0:
            elem.pop()
        elem.append(el)
        yield el


def write_logs_out(lvl, status_code, text, write_db=True):
    print('_______________________')
    print(lvl)
    print(text)
    print()
    if write_db:
        cursor = db.cursor()
        cursor.execute(f"INSERT INTO LOGS (MERCHANT_ID, LOG_LEVEL, LOG_STATUS, LOG_TEXT) "
                       "VALUES (%s, %s, %s, %s) ", (merchant_id, lvl, status_code, text))
        db.commit()
        cursor.close()


def press_enter():
    elem[0].send_keys(Keys.RETURN)


def click_mouse():
    elem[0].click()


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


def init_kaspi_vars():
    global kaspi_login, kaspi_password, my_link, my_id
    cursor = db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f'select * from merchants where merchant_id = {merchant_id}')
    rec = cursor.fetchone()
    kaspi_login = rec['kaspi_login']
    kaspi_password = rec['kaspi_password']
    # my_link = rec['address_tab']
    # my_id = my_link.split('/')[-3]


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
        success2 = wait_till_load_by_text('Заказы', t=15)
        if success2:
            break
        else:
            driver.close()
            # driver = webdriver.Firefox(firefox_profile=fp)
            create_driver()


def index_rows(mode, start_at=1):
    write_logs_out('DEBUG', INDEXER_MODE, f'{mode}')
    write_logs_out('DEBUG', INDEXER_START_PAGE, f'start_at = {start_at}')
    try:
        assert type(start_at) is int
        driver.get("https://kaspi.kz/merchantcabinet/#/offers")
        loaded = wait_till_load_by_text('Управление товарами', t=5)
        write_logs_out('DEBUG', INDEXER_UPRAVLENIE_LOADED, f'upravlenie loaded: {loaded}')
        if mode == 'archive':
            select = Select(WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'form__col._12-12'))))
            select.select_by_value('ARCHIVE')
        if not loaded:
            write_logs_out('ERROR', INDEXER_UPRAVLENIE_NOTLOADED, 'UPRAVLENIE NOT LOADED')
            raise Exception('UPRAVLENIE TOVARAMI NOT LOADED')
        loaded = wait_till_load_by_text(' из ', t=15)
        if not loaded:
            write_logs_out('ERROR', INDEXER_IZ_NOTLOADED, 'UPRAVLENIE NOT LOADED')
            raise Exception('UPRAVLENIE TOVARAMI NOT LOADED')
        write_logs_out('DEBUG', INDEXER_IZ_LOADED, f'iz loaded: {loaded}')
        curr_page = 1
        finished = False
        while not finished:
            if curr_page >= start_at:
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, 'offer-managment__product-cell-link')))
                rows = list(select_by_class('offer-managment__product-cell-link'))
                new_links = set([el.get_attribute('href')[:-1] for el in rows])
                links.update(new_links)
                write_logs_out('DEBUG', INDEXER_LINKS_AT_PAGE, f'Page: {curr_page} \nLen: {len(new_links)} \nTotLen: {len(fact_links)}'
                                                               f'\ntotlens {len(fact_links)} and {len(archive_links)}')
                page_info = list(select_by_attr('div', 'class', 'gwt-HTML'))[-1].text
                if page_info != '':
                    page_info = page_info.split()
                    finished = page_info[0].split('-')[1] == page_info[-1]
                    write_logs_out('DEBUG', INDEXER_FINISHED_TRYES, f'check finished first: {finished}')
                    if finished:
                        try:
                            [r.get_attribute('href') for r in rows]
                        except:
                            finished = False
                        write_logs_out('DEBUG', INDEXER_FINISHED_TRYES, f'check finished second: {finished}')
                else:
                    write_logs_out('ERROR', INDEXER_PAGEINFO_EMPTY, f'empty at page {curr_page}')
                if not finished:
                    # if len(fact_links) % 10 != 0:
                    #     write_logs_out('ERROR', INDEXER_LT10_ERROR, f'bad cnt fact_links len prev: {len(fact_links)}')
                    #     fact_links.difference_update(new_links)
                    #     write_logs_out('ERROR', INDEXER_LT10_ERROR, f'bad cntfact_links len after: {len(fact_links)}')
                    #     return 1
                    # except_n = 0
                    # except_msg = ''
                    # while except_n < 10:
                    try:
                        # while not list(select_by_attr('img', 'aria-label', 'Next page')):
                        #     time.sleep(0.1)
                        # select_by_attr('img', 'aria-label', 'Next page')
                        wait_next('img', 'aria-label', 'Next page')
                        click_mouse()
                    except Exception as e:
                        write_logs_out('ERROR', INDEXER_IZ_NOTLOADED, f'IZ NOT LOADED at page {curr_page}')
                        raise Exception(e)
                        # except_n += 1
                    # if except_n >= 10:
                    #     raise Exception(except_msg)
                    write_logs_out('DEBUG', INDEXER_NEXT_PRESSED, f'next pressed at page {curr_page}')
                    wait_till_load_by_text(' из ')
                    curr_page += 1
                    write_logs_out('DEBUG', INDEXER_NEXTPAGE_LOADED, f'new page {curr_page}')
            else:
                while not list(select_by_attr('img', 'aria-label', 'Next page')):
                    time.sleep(0.1)
                click_mouse()
                write_logs_out('DEBUG', INDEXER_SKIP_PAGE, f'skip page {curr_page}')
                wait_till_load_by_text(' из ')
                curr_page += 1
                write_logs_out('DEBUG', INDEXER_NEXTPAGE_LOADED, f'new page {curr_page}')
        write_logs_out('DEBUG', INDEXER_ALLSCANNED_SUCCESS, f'totlen: {len(fact_links)} \ntotpages: {curr_page}')
        return 0
    except:
        write_logs_out('FATAL', INDEXER_FATAL_ERROR, f'{traceback.format_exc()}')
        return 1


def write_to_db(mode):
    orders = psql.read_sql(f'select * from order_table where merchant_id = {merchant_id}', db)
    order_links = list(orders['order_link'].values)
    cursor = db.cursor()
    cursor.execute(f"delete from {'order_fact' if mode == 'fact' else 'order_archive'} where merchant_id = {merchant_id}")
    temp_links = fact_links if mode == 'fact' else archive_links
    rows = ','.join([str((merchant_id, fl, fl in order_links)) for fl in temp_links])
    cursor.execute(f"insert into {'order_fact' if mode == 'fact' else 'order_archive'} "
                   f"(merchant_id, order_link, main_includes) values " + rows)
    db.commit()
    cursor.close()
    write_logs_out('DEBUG', INDEXER_ORDERFACT_UPDATED, f'{mode}: inserted {len(temp_links)} new orders')


def write_seller_info():
    driver.get("https://kaspi.kz/merchantcabinet/#/settings")
    tb = WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located((By.TAG_NAME, 'table')))
    tb = tb.find_element_by_tag_name('tbody')
    els = tb.find_elements_by_tag_name('tr')
    kaspi_id = int(els[0].get_attribute('innerText').split('\n')[1])
    kaspi_name = els[1].get_attribute('innerText').split('\n')[1]
    link_tag = els[2].find_elements_by_tag_name('td')[1]
    address_tab = link_tag.find_element_by_tag_name('a').get_attribute('href')
    cursor = db.cursor()
    cursor.execute(f'update merchants '
                   f'set kaspi_id = {kaspi_id},'
                   f"kaspi_name = '{kaspi_name}', "
                   f"address_tab = '{address_tab}',"
                   f'last_active = now() where merchant_id = {merchant_id}')
    db.commit()
    cursor.close()
    write_logs_out('DEBUG', INDEXER_SELLER_INFO_SUCCESS, 'wrote to db')


if __name__ == '__main__':
    db = pg.connect(user=config.db_user,
                    password=config.db_pass,
                    database=config.db,
                    host=config.host,
                    port=config.port)
    init_vars()
    init_kaspi_vars()
    write_logs_out('INFO', INDEXER_STARTED, 'Indexer start')
    fact_links = set()
    archive_links = set()
    kaspi_info_inited = False
    for i, links in enumerate([fact_links, archive_links]):
        mode = 'fact' if i == 0 else 'archive'
        while True:
            create_driver()
            write_logs_out('INFO', INDEXER_DRIVER_OPENED, 'open')
            login()
            if not kaspi_info_inited:
                try:
                    write_seller_info()
                    kaspi_info_inited = True
                except:
                    write_logs_out('ERROR', INDEXER_SELLER_INFO_ERROR, f'{traceback.format_exc()}')
                    driver.quit()
                    continue
            status = index_rows(mode, start_at=len(links) % 10 + 1)
            # status = index_rows(5)
            driver.quit()
            write_logs_out('INFO', INDEXER_DRIVER_CLOSED, 'close')
            if status == 0:
                write_logs_out('DEBUG', INDEXER_ALLSCANNED_SUCCESS, f'{mode}: break at len {len(fact_links)}')
                write_to_db(mode)
                break
    exit_handler()
