import os
import re
import time
import atexit
import traceback
import warnings
import datetime
import sys
import os
import argparse
import config
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

sys.path.append('Customer_data')
from Customer_data.Customers import customers

driver = None


def create_driver():
    global driver
    fp = webdriver.FirefoxProfile()
    fp.set_preference("dom.popup_maximum", 0)
    options = Options()
    options.headless = False
    driver = webdriver.Firefox(options=options, firefox_profile=fp)


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
    exit_handler()
    # write_logs_out(thread_id, f'Exit handler called by wait_till_load_by_text: {text}')
    exit()
    return False


def fill_by_name(name, fill):
    global elem
    if len(elem) > 0:
        elem.pop()
    elem.append(driver.find_element_by_name(name))
    elem[0].clear()
    elem[0].send_keys(fill)


def exit_handler():
    global driver, db
    # write_logs_out(thread_id, 'Got kill signal')
    driver.close()
    db.close()
    # write_logs_out(thread_id, 'Closed driver and db')
    os.kill(os.getpid(), 9)


def press_enter():
    elem[0].send_keys(Keys.RETURN)


if __name__ == '__main__':
    create_driver()
