import pandas as pd
import time
import tempfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

def crawl_trading(url):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(5)
    display_data = driver.find_element(By.XPATH, '//*[@id="matrix"]/tbody')
    get_all_tr = display_data.find_elements(By.TAG_NAME, 'tr')
    trading_economics = []
    for tag in get_all_tr:
        get_all_td = tag.find_elements(By.TAG_NAME, 'td')
        trading_economics.append({
            'Country': get_all_td[0].text,
            'GDP': get_all_td[1].text,
            'GPD_growth': get_all_td[2].text,
            'Interest_rate': get_all_td[3].text,
            'Inflation_rate': get_all_td[4].text,
            'Jobless_rate': get_all_td[5].text,
            'Gov_budget': get_all_td[6].text,
            'Debt/GDP': get_all_td[7].text,
            'Current_account': get_all_td[8].text,
            'Population': get_all_td[9].text
        })
    driver.quit()
    print("Collection data successfully.")
    return trading_economics

def storage_data(trading_economics):
    df = pd.DataFrame(trading_economics)
    df.to_csv('/var/tmp/data/trading_economics.csv', encoding='utf-8', index=False)
    print("Storage data successfully.")
    
if __name__=='__main__':
    url = r'https://tradingeconomics.com/'
    trading_economics = crawl_trading(url)
    storage_data(trading_economics)
