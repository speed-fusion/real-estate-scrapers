import scrapy

import pymongo
from datetime import datetime
import os

user = "root"
password = "C5E01228B178C925CD0A0D6C6889BB029072"
host = "localhost:27017"
database = "scrapers-2022"

class Database:
    def __init__(self):
        db_name = database
        connection_uri = f'mongodb://{user}:{password}@{host}/?authSource=admin'
        client = pymongo.MongoClient(connection_uri)
        self.db = client[db_name]
        self.data_collection = self.db["data-07"]
        self.responses = self.db["responses"]

        


class PropertyDataPipeline:
    
    def __init__(self):
        self.db = Database()
    
    def process_item(self, item, spider):
        
        if item["event"] == "update":
            self.db.data_collection.update_one(
                {"_id":item["_id"]},
                {"$set":item["data"]}
            )
            
            if "response" in item:
                temp = {}
                temp["_id"] = item["_id"]
                temp["response"] = item["response"]
                
                self.db.responses.insert_one(temp)
            
        return item
    
    
    
    
        
class PropertyDataSpider(scrapy.Spider):
    name = 'property_data'
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":64,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
        #  "LOG_LEVEL":"INFO",
         "AUTOTHROTTLE_MAX_DELAY":True,
         "AUTOTHROTTLE_START_DELAY":5,
         "AUTOTHROTTLE_MAX_DELAY":10,
         "AUTOTHROTTLE_TARGET_CONCURRENCY":1,
         "AUTOTHROTTLE_DEBUG":True,
         "ITEM_PIPELINES" : {
            PropertyDataPipeline: 300,
        }
    }
    
    db = Database()
    
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")
    
    proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    
    headers = {
    'Cache-Control': 'public, max-age=7200',
    'Mapi-Bucket': 'search_reranking_app_test:v2',
    'Host': 'mapi-ng.rdc.moveaws.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'User-Agent': 'okhttp/3.12.12'
    }
    
    def get_last_sold_date(self,listing):
        last_sold_date = None
        last_sold_price = None
        try:
            history = listing.get("property_history",None)
            if history != None:
                for hist in history:
                    if hist["event_name"] == "Sold":
                        last_sold_date = hist["date"]
                        last_sold_price = hist["price"]
                        break
        except:
            pass
        return last_sold_date,last_sold_price
    
    def start_requests(self):
        for item in self.db.data_collection.find({"error_count":{"$lt":3},"status":3}).limit(5000):
            url = f'https://mapi-ng.rdc.moveaws.com/api/v1/properties/{item["property_id"]}?schema=legacy&tag_version=v2&client_id=rdc_mobile_native%2C10.41.1%2Candroid'
            yield scrapy.Request(url,headers=self.headers,callback=self.parse_property_data,meta={"proxy":self.proxy,"item":item})
    
    
    def parse_property_data(self, response):
        item = response.meta["item"]
        
        try:
            json_data = response.json()
            temp = {}

            listing = json_data.get("listing",None)

            if listing != None:
                temp["BedCount"] = listing.get("beds",None) 
                temp["BathCount"] = listing.get("baths",None)
                temp["Sqft"] = listing.get("sqft",None)
                temp["Acreslot"] = listing.get("lot_sqft",None)
                temp["EstValue"] = listing.get("price",None)
                temp["Url"] = listing.get("web_url",None)
                temp["Propertytype"] = listing.get("raw_prop_type",None)
                temp["Status"] = listing.get("prop_status",None)
                temp["YearBuilt"] = listing.get("year_built",None)
                LastSoldDate,LastSoldPrice = self.get_last_sold_date(listing)
                temp["LastSoldDate"] = LastSoldDate
                temp["LastSoldPrice"] = LastSoldPrice
                temp["status"] = 4
                temp["ScrapeDate"] = datetime.now()
                
            else:
                temp["error_count"] = item["error_count"] + 1
            # temp["response"] = json_data
            
            yield {
                "event":"update",
                "data":temp,
                "_id":item["_id"],
                "response" : json_data
            }
                           
        except Exception as e:
            message = str(e)
            yield {
                "event":"update",
                "data":{
                    "message":message,
                    "error_count" : item["error_count"] + 1
                },
                "_id":item["_id"]
            }
