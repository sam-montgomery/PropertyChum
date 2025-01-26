import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin'
}
s = requests.Session()
s.headers.update(headers)
big_list = []
#Foreach postcode
#Poll postcode search page for no. of properties 
#Foreach page, loop below
for x in tqdm(range(1, 252)):
    soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/northern-ireland/page-{x}').text, 'html.parser')
#     print(soup)
    properties = soup.select('li.pp-property-box')
    i = int(0)
    for p in properties:
        name = p.select_one('h2').get_text(strip=True) if p.select_one('h2') else None
        url = 'https://www.propertypal.com' + p.select_one('a').get('href') if p.select_one('a') else None
        price = p.select_one('p.pp-property-price').get_text(strip=True) if p.select_one('p.pp-property-price') else None
        big_list.append((name, price, url))
        i = i + 1
    print(i)
big_df = pd.DataFrame(big_list, columns = ['Property', 'Price', 'Url'])
print(big_df)