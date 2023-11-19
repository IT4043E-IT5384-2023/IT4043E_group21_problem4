from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--enable-javascript")
    chrome_driver_path = ChromeDriverManager().install()
    print(chrome_driver_path)
    return webdriver.Chrome(service=Service(chrome_driver_path), options=options)


url = "https://etherscan.io/address/0xbB854d5324906a93C79f35d4fe267C8150c795ab" 
driver = setup_driver()
driver.get(url)

html = driver.page_source

bs = BeautifulSoup(html, "html.parser")
with open("h.html", "w", encoding="utf-8") as f:
    f.write(html)

driver.close()