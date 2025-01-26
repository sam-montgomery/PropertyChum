import requests
import re
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

def extract_page_count(html_string):
    # First, remove the HTML tags
    cleaned_string = re.sub(r'<[^>]+>', '', html_string)
    
    # Search for the pattern "Page X of Y" to extract the total page count
    match = re.search(r'Page\s\d+\s*of\s*(\d+)', cleaned_string)
    
    # If a match is found, return the total page count as an integer
    if match:
        return int(match.group(1))  # The total page count is captured in group(1)
    else:
        return None  # Return None if no match is found

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
soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/bt1').text, 'html.parser')
pagesString = str(soup.findAll(class_ = "sc-bbce18de-5 lkNuoZ"))
pages = extract_page_count(pagesString)
print(pages)
#for x in tqdm(range(1, 252)):
#     print(soup)
    #properties = soup.select('li.pp-property-box')
    #for p in properties:
        #name = p.select_one('h2').get_text(strip=True) if p.select_one('h2') else None
        #url = 'https://www.propertypal.com' + p.select_one('a').get('href') if p.select_one('a') else None
        #price = p.select_one('p.pp-property-price').get_text(strip=True) if p.select_one('p.pp-property-price') else None
        #big_list.append((name, price, url))
    #print(i)
#big_df = pd.DataFrame(big_list, columns = ['Property', 'Price', 'Url'])
#print(big_df)

