from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.headless = True
driver = webdriver.Chrome(options=options, executable_path=r'/chromedriver/chromedriver')
driver.get("http://google.com/")
print ("Headless Chrome Initialized")
driver.quit()