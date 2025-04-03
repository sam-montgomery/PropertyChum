import re

class StringHelper:
    #Function to parse the property features from the html elements passed, and return a tuple of the feature name and value for the SQL command.
    @staticmethod
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
    @staticmethod
    def extract_page_count(html_string):
        cleaned_string = re.sub(r'<[^>]+>', '', html_string)
        match = re.search(r'Page\s\d+\s*of\s*(\d+)', cleaned_string)
        if match:
            return int(match.group(1))  
        else:
            return None  

    #Regex filter to extract the address from the html string passed.
    @staticmethod
    def extract_address(html_string):
        cleaned_string = re.sub(r'<[^>]+>', '', html_string)
        return cleaned_string.strip()
