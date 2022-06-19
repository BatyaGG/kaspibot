from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.common.keys import Keys
driver = webdriver.Firefox()
driver.get('https://www.lambdatest.com/')
time.sleep(5)
# locates a link element on the loaded url using xpath
new_tab_link = driver.find_element_by_xpath('https://www.google.com')
time.sleep(5)
# instantiates ActionChains class of selenium webDriver
action = ActionChains(driver)
# clicks on located kink element with CONTROL button in pressed state using actionChains class. This opens the link in new tab 
action.key_down(Keys.CONTROL).click(new_tab_link).key_up(Keys.CONTROL).perform() 
time.sleep(3)
driver.quit()