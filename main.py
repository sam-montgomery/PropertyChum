
postcodes = [1,2,7,15,8,3,6,4,5,16]

from PropertyScraper import PropertyScraper  # Replace with the actual module name

scraper = PropertyScraper()

print(scraper.scrape_property_tiles(postcodes))
print("Duplicates found: " + str(scraper.get_duplicates()))