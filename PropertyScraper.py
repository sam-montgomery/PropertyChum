import requests
import psycopg2
import re
import html
import pandas as pd
import string
from config import load_config
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
from datetime import datetime
from lxml import etree
import re


#Global variable declaration. These are used to track the number of properties scraped and the number of duplicates found.
scraped = 0

def increment_scraped():
    global scraped  
    scraped += 1

def reset_scraped():
    scraped = 0

def get_scraped():
    return scraped
    
duplicates = 0

def increment_duplicates():
    global duplicates 
    duplicates += 1

def reset_duplicates():
    duplicates = 0

def get_duplicates():
    return duplicates


#Function to parse the property features from the html elements passed, and return a tuple of the feature name and value for the SQL command.
def parse_property_feature(feature):
    if "Bedrooms" in feature:
        return ("iBeds",feature.replace("Bedrooms",""))
    elif "Tenure" in feature:
        return ("sTenure",feature.replace("Tenure",""))
    elif "Energy Rating" in feature:
        return ("sEnergyRating",feature.replace("Energy Rating",""))
    elif "Heating" in feature:
        return ("sHeating",feature.replace("Heating",""))
    elif "Broadband" in feature:
        return ("sBroadband",feature.replace("Broadband",""))
    elif "Rates" in feature:
        return ("dRates",re.sub(r'[^\d.]', '', feature.replace("Rates","")))
    elif "Style" in feature:   
        return ("sStyle",feature.replace("Style",""))
    elif "Bathrooms" in feature:
        return ("iBathrooms",feature.replace("Bathrooms",""))
    else: return None

#Function to extract the total number of pages from the html of the search results page. Used to inform the bs loop how many pages to scrape.
def extract_page_count(html_string):
    cleaned_string = re.sub(r'<[^>]+>', '', html_string)
    match = re.search(r'Page\s\d+\s*of\s*(\d+)', cleaned_string)
    if match:
        return int(match.group(1))  
    else:
        return None  

#Regex filter to extract the address from the html string passed.
def extract_address(html_string):
    cleaned_string = re.sub(r'<[^>]+>', '', html_string)
    return cleaned_string.strip()

#Main function to scrape property tiles from the search results page, previously tracked in the database, this is now used to collect the url and the price of the property.
def scrape_property_tiles(postcodes):
    for bt in tqdm(postcodes):
        #Poll postcode search page for no. of properties
        soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}').text, 'html.parser')
        pagesString = str(soup.find_all(class_ = "sc-bbce18de-5 lkNuoZ"))
        pages = int(extract_page_count(pagesString))
        print(f'BT{bt}: {pages} pages found.')
        for x in (range(1, (pages + 1))):
            #Foreach page, loop below
            soup = bs(s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}/page-{x}').text, 'html.parser')
            properties = soup.select('li.pp-property-box')
            reset_scraped()
            reset_duplicates()
            for p in properties:
                name = p.select_one('h2').get_text(strip=True) if p.select_one('h2') else None
                url = 'https://www.propertypal.com' + p.select_one('a').get('href') if p.select_one('a') else None
                price = p.select_one('p.pp-property-price').get_text(strip=True) if p.select_one('p.pp-property-price') else None
                scrape_property(url, price)
                propertyTiles.append((name,url,price))
    
#Main function to scrape the property details from the property page, and insert into the database. Contains the logic to determine if a property is already tracked, and if not, insert the property details.
#Always inserts the price into the tPrice table.
# NOTE: There is a known bug with sPropertyGuid, in some cases where if the address contains a ', it can prevent the is_property_scraped function from working correctly. ' escapes SQL commands.
def scrape_property(url, price):
    try:
        soup = bs(s.get(url).text, 'html.parser')
        address = extract_address(str(soup.find_all(class_ = "sc-bbce18de-0 ENffC")[0])) + ' ' + str(soup.select_one('p.sc-bbce18de-5').get_text(strip=True)).replace(",","")
        address = html.unescape(address)
        address = address.replace("'","''")
        propertysummarytiles =  soup.find_all(class_ = "pp-property-summary")

        listed_features = []
        for featuretile in propertysummarytiles:
            feature = featuretile.find_all(class_ = "sc-5ad00a7d-3 bRGVlA")
            for f in feature:
                featuretext = f.get_text(strip=True)
                featuretext = parse_property_feature(featuretext)
                if featuretext is not None:
                    listed_features.append(featuretext)

        propertyGuid = ''

        match = re.search(r'\bBT\d{1,2}\s?\d[A-Z]{2}\b', address)
        propertyGuid += address.split(",")[0].strip().replace(" ","")
        if match:
            propertyGuid += match.group(0).replace(" ","")

        increment_scraped()
        #IF not found in tProperty insert:
        tracked = is_property_tracked(propertyGuid)
        print(str(scraped) + " | Property Key: " + propertyGuid + " Tracked: " + str(tracked) + " URL: " + url)      
        #Try catch loop added here to parse the price from the property page. In some cases 'Price available on request' or 'Sale Agreed' are listed, this parse doesn't work with these strings.
        #The price is parsed to make it consistent across all db listings, there is also the functionality to add cost, this has been used to test the inserts and create a price history for a listing.
        try:
            numeric_price = re.sub(r'[^\d.]', '', price)
            
            numeric_price = float(numeric_price)
            
            updated_price = numeric_price + 0
            
            updated_price_str = re.sub(r'\d[\d,]*\.?\d*', f'{updated_price:,.2f}', price)

            price = updated_price_str
        except Exception as e:
            print ("Error converting price: " + price)
            print(e)

        #If propertyGuid isn't in the property table, insert the property details.
        if tracked == 0:
            insert_property(address, listed_features) 
        else:
            increment_duplicates()
        
        #Always insert tPrice
        insert_price(price, propertyGuid)
        return listed_features
    except Exception as e:
        #This is usually called when a property is listed in the search result, but the property page is no longer available. Clicking the link in the log usually tells you. 
        print("Error parsing property: " + url)
        print(e)


########################################################################################
#Database functions
########################################################################################

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print("Connection error: " + error)
    
def is_property_tracked(property_key):
    sql = """SELECT COUNT(*) FROM public."tProperty" WHERE "tProperty"."gPropertyKey" = %s;"""
    config = load_config()
    property_key = property_key.replace("'","''")
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (property_key,))
                count = cur.fetchone()[0]
                return count
    except (Exception, psycopg2.DatabaseError) as error:
        print("SELECT ERROR: " + str(error))
        return None

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
  
def insert_price(price, propertyKey):
    sql = """INSERT INTO public."tPrice"("sPrice", "dTimeOfCapture", "gPropertyKey") VALUES (%s,%s,%s);"""
    vendor_id = None
    now = datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    config = load_config()
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql,(price, time_str, propertyKey))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("INSERT ERROR: " + error)
    finally:
        return vendor_id  
    
def insert_property(address, features):
    propertyGuid = ''

    match = re.search(r'\bBT\d{1,2}\s?\d[A-Z]{2}\b', address)
    propertyGuid += address.split(",")[0].strip().replace(" ","")
    if match:
        propertyGuid += match.group(0).replace(" ","")

    sql1 = """INSERT INTO public."tProperty"( "sAddress", "gPropertyKey"""
    sql2 = f"""VALUES ('{address}', '{propertyGuid}'"""

    for feature in features:
        if feature[1] == "":
            break
        else:
            sql1 += f'", "{feature[0]}'
            if feature[0].startswith("s"):
                sql2 += f", '{feature[1]}'"
            else:
                sql2 += f", {feature[1]}"

    sql = sql1 + '") ' + sql2 + ");"
        
    vendor_id = None
    config = load_config()
    try:
        with  psycopg2.connect(**config) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql)
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("INSERT ERROR: " + error)
    finally:
        return vendor_id

########################################################################################

#Headers used to scrape the property details from the property page.
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

print(scrape_property_tiles(postcodes))
print("Duplicates found: " + str(get_duplicates()))