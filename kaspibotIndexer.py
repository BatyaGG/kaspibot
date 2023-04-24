import pickle
import re
import time
import atexit
import traceback
import warnings
import sys
import os
import argparse
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
    # caps = DesiredCapabilities().FIREFOX
    # caps["pageLoadStrategy"] = 'none'
    options = Options()
    options.headless = config.headless
    # options.page_load_strategy = 'none'
    # driver = webdriver.Firefox(options=options,
    #                            # firefox_profile=fp,
    #                            capabilities=caps,
    #                            # executable_path='/main/drivers/geckodriver'
    #                            )
    driver = webdriver.Firefox(options=options)


def init_vars():
    global driver, elem, city_inited
    driver = None
    elem = []
    city_inited = False


def wait_till_load_by_text(text, t=15.0):
    # driver.find_elements_by_xpath(f"//*[contains(text(), '{text}')]")
    trials = 1
    for i in range(trials):
        try:
            myElem = WebDriverWait(driver, t).until(EC.text_to_be_present_in_element((By.CLASS_NAME, 'body'), text))
            return True
        except TimeoutException:
            driver.back()
            # time.sleep(1)
            driver.forward()
            # driver.refresh()
    # exit_handler()
    return False


def wait_till_load_button(text, t=15.0):
    # driver.find_elements_by_xpath(f"//*[contains(text(), '{text}')]")
    trials = 1
    for i in range(trials):
        try:
            myElem = WebDriverWait(driver, t).until(EC.element_to_be_clickable((By.CLASS_NAME, text)))
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


def write_logs_out(lvl, status_code, text):
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
    try:
        driver.get("https://kaspi.kz/mc/#/login")
        # success1 = wait_till_load_button('button.is-primary')
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.CLASS_NAME, 'button.is-primary')))
        time.sleep(1)
        el = driver.find_element(By.XPATH, "//span[text()='Email']")
        el.click()
        time.sleep(1)

        el = driver.find_elements_by_name('username')[1]
        el.send_keys(kaspi_login)
        time.sleep(1)
        el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CLASS_NAME, 'button.is-primary')))
        el.click()
        time.sleep(1)
        el = driver.find_elements_by_class_name('text-field')[2]
        el.send_keys(kaspi_password)
        el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.CLASS_NAME, 'button.is-primary')))
        el.click()
        WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.CLASS_NAME, 'menu__item--text')))
        return True
    except:
        return False


def get_db_fact(mode):
    cursor = db.cursor(cursor_factory=RealDictCursor)
    cursor.execute(f'select * from order_{mode} where merchant_id = {merchant_id}')
    fact_links_db = cursor.fetchall()
    if any([any([row[k] == 'none' for k in row]) for row in fact_links_db]):
        return fact_links_db, False
    return fact_links_db, True


def wait_curtain():
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'loading-overlay.is-active.is-full-page')))
        WebDriverWait(driver, 15).until_not(
            EC.presence_of_element_located((By.CLASS_NAME, 'loading-overlay.is-active.is-full-page')))
    except:
        pass


def index_rows(mode):
    write_logs_out('DEBUG', INDEXER_MODE, f'{mode}')
    try:
        driver.switch_to.new_window('TAB')
        driver.get("https://kaspi.kz/mc/#/products/ACTIVE/1")

        wait_curtain()
        write_logs_out('DEBUG', INDEXER_PAGE_LOADED, f'Page is loaded')
        select = Select(WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='select']//select"))))
        if mode == 'archive':
            select.select_by_value('ARCHIVE')
            wait_curtain()
        cnt_kaspi = int(select.first_selected_option.text.split(' ')[1][1:-1])
        fact_links_db, is_good = get_db_fact(mode)

        if cnt_kaspi != len(fact_links_db) or not is_good:
            write_logs_out('DEBUG', INDEXER_ORDERS_N_CHANGED, f'{mode} prev orders len {len(fact_links_db)}\n new orders len {cnt_kaspi}')
            curr_page = 1
            finished = False
        else:
            links.update([(li['order_link'], li['image_link'], li['order_name']) for li in fact_links_db])
            write_logs_out('DEBUG', INDEXER_FACT_NOTCHANGED, f'{mode} len is same {cnt_kaspi}')
            curr_page = 1
            finished = True
        while not finished:
            rows = driver.find_elements_by_xpath('//tbody')[0].get_attribute('innerHTML')
            new_links = []
            for r in rows.split('\n'):
                img_src = r.find('img src=')
                if img_src > 0:
                    order_link = r[r.find('a href='):].split('"')[1][:-1]
                    img_src = r[img_src:].split('"')[1]
                    order_name = r[r.find('jpg" alt=')+5:].split('"')[1]
                    new_links.append((order_link, img_src, order_name))
            links.update(new_links)
            write_logs_out('DEBUG', INDEXER_LINKS_AT_PAGE, f'Page: {curr_page} \nLen: {len(new_links)} \nTotLen: {len(fact_links)}'
                                                           f'\ntotlens {len(fact_links)} and {len(archive_links)}')
            next_button = driver.find_element_by_xpath("//div[@class='top level']//a[2]")
            print(next_button.get_attribute('disabled'))
            if next_button.get_attribute('disabled'):
                finished = True
            else:
                next_button.click()
                wait_curtain()
        write_logs_out('DEBUG', INDEXER_ALLSCANNED_SUCCESS, f'totlen: {len(fact_links)} and {len(archive_links)}')
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
    rows = ','.join([str((merchant_id, link, link in order_links, img if img else 'none', name if name else 'none')) for link, img, name in temp_links])
    cursor.execute(f"insert into {'order_fact' if mode == 'fact' else 'order_archive'} "
                   f"(merchant_id, order_link, main_includes, image_link, order_name) values " + rows)

    db.commit()
    cursor.close()
    write_logs_out('DEBUG', INDEXER_ORDERFACT_UPDATED, f'{mode}: inserted {len(temp_links)} new orders')


def write_seller_info():
    driver.switch_to.new_window('TAB')
    driver.get('https://kaspi.kz/mc/#/settings')
    # settings_xpath = "//body/div[@id='app']/div/section[@class='app-section']/div[@class='columns is-fullheight']/div[@class='column is-3 is-sidebar-menu desktop-sidebar']/div[@class='menu container sidebar']/ul[1]/li[1]"
    # sett_butt = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, settings_xpath)))
    # sett_butt.click()
    info = WebDriverWait(
        driver, 15).until(EC.presence_of_element_located((By.XPATH, "//body/div[@id='app']/div/section[@class='app-section']/div[@class='columns is-fullheight']/div[@class='column is-main-content']/div[@class='settings']/section/div[@class='b-tabs']/section[@class='tab-content']/div[1]/section[1]/div[1]")))
    info_it = info.get_attribute('innerText')
    info_it = info_it.split('\n')
    kaspi_id = info_it[2]
    kaspi_name = info_it[4]
    links = info.find_elements_by_tag_name('a')
    address_tab = links[0].get_attribute('href')
    orders_link = links[1].get_attribute('href')

    cursor = db.cursor()
    cursor.execute(f'update merchants '
                   f'set kaspi_id = {kaspi_id},'
                   f"kaspi_name = '{kaspi_name}', "
                   f"address_tab = '{address_tab}',"
                   f"orders_link = '{orders_link}',"
                   f'last_active = now() where merchant_id = {merchant_id}')
    db.commit()
    cursor.close()
    write_logs_out('DEBUG', INDEXER_SELLER_INFO_SUCCESS, 'wrote to db')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('write_logs',  nargs='?', default='False')
    args = parser.parse_args()
    write_db = args.write_logs == 'True'

    db = pg.connect(user=config.db_user,
                    password=config.db_pass,
                    database=config.db,
                    host=config.host,
                    port=config.port)

    init_vars()
    write_logs_out('INFO', INDEXER_STARTED, 'Indexer start')
    fact_links = set()
    archive_links = set()
    kaspi_info_inited = False
    for i, links in enumerate([fact_links, archive_links]):
        mode = 'fact' if i == 0 else 'archive'
        while True:
            init_kaspi_vars()
            create_driver()
            write_logs_out('INFO', INDEXER_DRIVER_OPENED, 'open')
            status = login()
            # wait_curtain()
            if not status:
                driver.quit()
                continue
            if not kaspi_info_inited:
                try:
                    write_seller_info()
                    kaspi_info_inited = True
                except:
                    write_logs_out('ERROR', INDEXER_SELLER_INFO_ERROR, f'{traceback.format_exc()}')
                    driver.quit()
                    continue
            status = index_rows(mode)
            driver.quit()
            write_logs_out('INFO', INDEXER_DRIVER_CLOSED, 'close')
            if status == 0:
                write_logs_out('DEBUG', INDEXER_ALLSCANNED_SUCCESS, f'{mode}: break at len {len(fact_links), len(archive_links)}')
                write_to_db(mode)
                break
    exit_handler()
