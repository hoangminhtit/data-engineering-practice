import requests
from bs4 import BeautifulSoup
import pandas as pd

def crawl():
    quotes, authors, tags_list = [], [], []
    for page in range(1, 6):
        url = f"http://quotes.toscrape.com/page/{page}/"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        quotes_block = soup.find_all('div', class_='quote')
        for quote in quotes_block:
            quotes.append(quote.find('span', class_='text').get_text(strip=True))
            authors.append(quote.find('small', class_='author').text)
            tags = [tag.text for tag in quote.find_all('a', class_='tag')]
            tags_list.append(", ".join(tags))
    df = pd.DataFrame({'Quote': quotes, 'Author': authors, 'Tags': tags_list})
    df.to_csv('quotes.csv', index=False, encoding='utf-8')

if __name__ == '__main__':
    crawl()
