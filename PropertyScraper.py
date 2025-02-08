import requests
import psycopg2
import re
import pandas as pd
import string
from config import load_config
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

def extract_page_count(html_string):
    cleaned_string = re.sub(r'<[^>]+>', '', html_string)
    match = re.search(r'Page\s\d+\s*of\s*(\d+)', cleaned_string)
    if match:
        return int(match.group(1))  # The total page count is captured in group(1)
    else:
        return None  # Return None if no match is found

import re

def extract_address(html_string):
    # Remove all HTML tags
    cleaned_string = re.sub(r'<[^>]+>', '', html_string)
    # Strip leading/trailing whitespace and return the result
    return cleaned_string.strip()

def scrape_property_tiles(postcodes):
    for bt in tqdm(postcodes):
        #Poll postcode search page for no. of properties
        soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}').text, 'html.parser')
        pagesString = str(soup.findAll(class_ = "sc-bbce18de-5 lkNuoZ"))
        pages = int(extract_page_count(pagesString))
        print(f'BT{bt}: {pages} pages found.')
        for x in (range(1, (pages + 1))):
            #Foreach page, loop below
            soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}/page-{x}').text, 'html.parser')
            properties = soup.select('li.pp-property-box')
            for p in properties:
                name = p.select_one('h2').get_text(strip=True) if p.select_one('h2') else None
                url = 'https://www.propertypal.com' + p.select_one('a').get('href') if p.select_one('a') else None
                price = p.select_one('p.pp-property-price').get_text(strip=True) if p.select_one('p.pp-property-price') else None
                insert_property_tile(name,price,url)
                propertyTiles.append((name,url,price))
    return propertyTiles

def scrape_property(url):
    soup = bs(s.get(url).text, 'html.parser')
    address = extract_address(str(soup.findAll(class_ = "sc-bbce18de-0 ENffC")[0])) + ' ' + str(soup.select_one('p.sc-bbce18de-5').get_text(strip=True))
    propertyOverview =  soup.findAll(class_ = "sc-5ad00a7d-5 ghsGXG")
    style = re.search(r'<p[^>]*>(.*?)</p>', str(propertyOverview[1])).group(1)
    bedrooms = re.search(r'<span>(\d+)</span>', str(propertyOverview[2])).group(1)
    bathrooms = re.search(r'<span>(\d+)</span>', str(propertyOverview[3])).group(1)
    price = '£' + re.search(r'£([\d,]+)', str(propertyOverview[4])).group(1)
        
    return (address, style, bedrooms, bathrooms, price)
    


def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print("Connection error: " + error)

def insert_property_tile(price, address, link):
    sql = """INSERT INTO public."tPropertyTile"( "sPrice", "sAddress", "sLink") VALUES (%s,%s,%s);"""
    vendor_id = None
    config = load_config()
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql,(price, address, link))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("INSERT ERROR: " + error)
    finally:
        return vendor_id
    
    
def insert_property(address, style, beds, tenure, energyrating,heating,broadband,rates):
    sql = """INSERT INTO public."tProperty"(
    "sAddress", "sStyle", "iBeds", "sTenure", "sEnergyRating", "sHeating", "sBroadband", "dRates", "gHouseNoPostCode", "iBathrooms")
	VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    vendor_id = None
    config = load_config()
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql,(address, style, beds, tenure, energyrating,heating,broadband,rates))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("INSERT ERROR: " + error)
    finally:
        return vendor_id

########################################################################################


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
propertyTiles = []
postcodes = [1,2,7,15,8,3,6,4,5,16]

config=load_config()

#propertyTiles = scrape_property_tiles(postcodes)

print(scrape_property("https://www.propertypal.com/belgravia-house-2a-belgravia-avenue-belfast/929696/photo-2"))

