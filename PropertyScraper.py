import requests
import re
import html
from StringHelper import StringHelper
from DatabaseInterface import DatabaseInterface
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
import re

class PropertyScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin'
        }
        self.s = requests.Session()
        self.s.headers.update(self.headers)
        self.scraped = 0
        self.duplicates = 0

    
    #Global variable declaration. These are used to track the number of properties scraped and the number of duplicates found.
    def increment_scraped(self):
        self.scraped += 1

    def reset_scraped(self):
        self.scraped = 0

    def get_scraped(self):
        return self.scraped
        

    def increment_duplicates(self):
        self.duplicates += 1

    def reset_duplicates(self):
        self.duplicates = 0

    def get_duplicates(self):
        return self.duplicates

    #Main function to scrape property tiles from the search results page, previously tracked in the database, this is now used to collect the url and the price of the property.
    def scrape_property_tiles(self, postcodes):
        self.reset_scraped()
        self.reset_duplicates()
        for bt in tqdm(postcodes):
            #Poll postcode search page for no. of properties
            soup = bs(self.s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}').text, 'html.parser')
            pagesString = str(soup.find_all(class_ = "sc-bbce18de-5 lkNuoZ"))
            pages = int(StringHelper.extract_page_count(pagesString))
            print(f'BT{bt}: {pages} pages found.')
            for x in (range(1, (pages + 1))):
                #Foreach page, loop below
                soup = bs(self.s.get(f'https://www.propertypal.com/property-for-sale/bt{bt}/page-{x}').text, 'html.parser')
                properties = soup.select('li.pp-property-box')
                for p in properties:
                    name = p.select_one('h2').get_text(strip=True) if p.select_one('h2') else None
                    url = 'https://www.propertypal.com' + p.select_one('a').get('href') if p.select_one('a') else None
                    price = p.select_one('p.pp-property-price').get_text(strip=True) if p.select_one('p.pp-property-price') else None
                    self.scrape_property(url, price)

    #Main function to scrape the property details from the property page, and insert into the database. Contains the logic to determine if a property is already tracked, and if not, insert the property details.
    #Always inserts the price into the tPrice table.
    # NOTE: There is a known bug with sPropertyGuid, in some cases where if the address contains a ', it can prevent the is_property_scraped function from working correctly. ' escapes SQL commands.
    def scrape_property(self, url, price):
        try:
            soup = bs(self.s.get(url).text, 'html.parser')
            address = StringHelper.extract_address(str(soup.find_all(class_ = "sc-bbce18de-0 ENffC")[0])) + ' ' + str(soup.select_one('p.sc-bbce18de-5').get_text(strip=True)).replace(",","")
            address = html.unescape(address)
            address = address.replace("'","''")
            propertysummarytiles =  soup.find_all(class_ = "pp-property-summary")

            listed_features = []
            for featuretile in propertysummarytiles:
                feature = featuretile.find_all(class_ = "sc-5ad00a7d-3 bRGVlA")
                for f in feature:
                    featuretext = f.get_text(strip=True)
                    featuretext = StringHelper.parse_property_feature(featuretext)
                    if featuretext is not None:
                        listed_features.append(featuretext)

            propertyGuid = ''

            match = re.search(r'\bBT\d{1,2}\s?\d[A-Z]{2}\b', address)
            propertyGuid += address.split(",")[0].strip().replace(" ","")
            if match:
                propertyGuid += match.group(0).replace(" ","")

            self.increment_scraped()
            #IF not found in tProperty insert:
            tracked = DatabaseInterface.is_property_tracked(propertyGuid)
            print(str(self.scraped) + " | Property Key: " + propertyGuid + " Tracked: " + str(tracked) + " URL: " + url)      
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
                DatabaseInterface.insert_property(address, listed_features) 
            else:
                self.increment_duplicates()
            
            #Always insert tPrice
            DatabaseInterface.insert_price(price, propertyGuid)
            return listed_features
        except Exception as e:
            #This is usually called when a property is listed in the search result, but the property page is no longer available. Clicking the link in the log usually tells you. 
            print("Error parsing property: " + url)
            print(e)