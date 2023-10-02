import re
import json
import datetime as dt
from bs4 import BeautifulSoup, Tag
import selenium.webdriver as webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import requests as requests
from sqlalchemy import create_engine
import boto3
from botocore.exceptions import ClientError

def get_secret():
'''Retrieve RDS Secret from AWS Secret Manager'''
    secret_name = "prod/database"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client( service_name='secretsmanager', region_name=region_name)

    return json.loads(get_secret_value_response['SecretString'])

    
# config values
secret = get_secret()
endpoint = secret['host']
user_name = secret['username']
password = secret['password']
db_name = secret['dbInstanceIdentifier']
port = secret['port']

# Web Site Locations
ccs_url = 'https://shop.ccs.com/collections/shoes'
tactics_url = 'https://www.tactics.com/mens-shoes'

# Set Webdriver settings for Selenium to extract CCS data
options = Options()
options.add_argument("--headless")
options.add_argument("window-size=1400,1500")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("start-maximized")
options.add_argument("enable-automation")
options.add_argument("--disable-infobars")
options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=options)
driver.get(ccs_url)
page_source = driver.page_source
driver.quit()

# Get Data from Tactics Web Site
tactics_request = requests.get(tactics_url)
tactics_soup = BeautifulSoup(tactics_request.content, 'html.parser')

# Initialize Empty Dictionaries for CCS data
ccs_item_name_dict = {}
ccs_item_price_dict = {}

item_name = re.compile(r'item_name:\s*"([^"]+)"')
item_name_match = item_name.findall(page_source)

for i, match in enumerate(item_name_match):
    ccs_item_name_dict[f"item_name_{i+1}"] = match

item_price = re.compile(r'price:\s*([\d.]+)')
item_price_match = item_price.findall(page_source)

for i, match in enumerate(item_price_match):
    ccs_item_price_dict[f"item_price_{i+1}"] = match

# Create CCS dataframe from Dict Values
ccs_df = pd.DataFrame({
    "models": list(ccs_item_name_dict.values()),
    "prices": list(ccs_item_price_dict.values())
})


ccs_df['brands'] = ccs_df['models'].str.split().str[0]
ccs_df['models'] = ccs_df['models'].str.split(n=1).str[1].str.split(pat="-").str[0].str.split(pat="Shoes").str[0]
ccs_df['prices'] = "$"+ccs_df['prices']
ccs_df['source'] = "CCS.com"


tactics_shoes = tactics_soup.find(id='browse-grid')

# Initialize empty Lists for Tactics Data
brands = []
prices = []
models = []

# Core logic for Navigating HTML Parse Tree
for shoes in tactics_shoes.children:
    if isinstance(shoes, Tag):
        brand = shoes.find("span", class_="browse-grid-item-brand")
        price = shoes.find("span", class_="browse-grid-item-price")
        
        anchor_tag = shoes.find("a")
        if anchor_tag:
            text_under_a = anchor_tag.stripped_strings
            complete_text = " ".join(text_under_a)
            
            if brand and brand.string:
                complete_text = complete_text.replace(brand.string.strip(), "")
            color = shoes.find("span", class_="browse-grid-item-color")
            if color and color.string:
                complete_text = complete_text.replace(color.string.strip(), "")
            
            complete_text = complete_text.strip()  # Remove leading and trailing whitespaces

            tactics_brand = f"{brand.string if brand else 'N/A'}"
            tactics_price = f"{price.string if price else 'N/A'}"
            tactics_model = f"{complete_text if anchor_tag else 'N/A'}"
            
            # Populate empty Lists
            brands.append(tactics_brand)
            prices.append(tactics_price)
            models.append(tactics_model)

    
brand_series = pd.Series(brands)
prices_series = pd.Series(prices)
models_series = pd.Series(models)

tactics_dict = {
    'brands' : brand_series,
    'models' : models_series,
    'prices' : prices_series
    }

tactics_df = pd.DataFrame(tactics_dict)
tactics_df['models'] = tactics_df["models"].str.split(pat="Skate Shoes").str[0]
tactics_df['source'] = "Tactics.com"

# Combine Dataframes
combined_df = pd.concat([tactics_df, ccs_df], ignore_index=True)
combined_df['dt'] = dt.datetime.now()
combined_df['prices'] = combined_df['prices'].replace('N/A', '0')
combined_df['prices'] = combined_df['prices'].replace('[\$,]', '', regex=True).astype(float)

# Connect to RDS Instance and append to Table
engine = create_engine(f'postgresql://{user_name}:{password}@{endpoint}:{port}/{db_name}')
combined_df.to_sql('shoes', engine, if_exists='append', index=False)
