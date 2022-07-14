import scrapy

import pymongo

user = "root"
password = "C5E01228B178C925CD0A0D6C6889BB029072"
host = "localhost:27017"
database = "scrapers-2022"

import os

class Database:
    def __init__(self):
        db_name = database
        connection_uri = f'mongodb://{user}:{password}@{host}/?authSource=admin'
        client = pymongo.MongoClient(connection_uri)
        self.db = client[db_name]
        self.data_collection = self.db["data-07"]

class PropertyIdPipeline:
    
    def __init__(self):
        self.db = Database()
    
    def process_item(self, item, spider):
        
        if item["event"] == "update":
            self.db.data_collection.update_one(
                {"_id":item["_id"]},
                {"$set":item["data"]}
            )
            
        return item
    
    
       
class PropertyIdSpider(scrapy.Spider):
    name = 'property_id'

    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":256,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "AUTOTHROTTLE_MAX_DELAY":True,
         "AUTOTHROTTLE_START_DELAY":5,
         "AUTOTHROTTLE_MAX_DELAY":10,
         "AUTOTHROTTLE_TARGET_CONCURRENCY":1,
         "AUTOTHROTTLE_DEBUG":True,
         "ITEM_PIPELINES" : {
            PropertyIdPipeline: 300,
        }
    }
    
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")
    
    max_records = 6000
    
    db = Database()
    
    proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    
    headers = {
    'Cache-Control': 'public, max-age=900',
    'Mapi-Bucket': 'search_reranking_app_test:v2',
    'Host': 'mapi-ng.rdc.moveaws.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'User-Agent': 'okhttp/3.12.12'
    }
    
    def start_requests(self):
        
        for item in list(self.db.data_collection.find({"error_count":{"$lt":3},"status":2}).limit(self.max_records)):
            query = item["query"]
            url = f'https://mapi-ng.rdc.moveaws.com/api/v1/location/autocomplete?input={query}'
            yield scrapy.Request(url,headers=self.headers,callback=self.parse_property_id,meta={"proxy":self.proxy,"item":item})            
        
    
    def parse_property_id(self, response):
        item = response.meta["item"]
        try:
        
            json_data = response.json()
            property_id = json_data["autocomplete"][0]["mpr_id"]
            
            try:
                address = json_data["autocomplete"][0]["full_address"][0]
            except:
                address = None
            
            try:
                centroid = json_data["autocomplete"][0]["centroid"]
            except:
                centroid = None

            yield {
                "event":"update",
                "data":{
                    "property_id":property_id,
                    "status":1,
                    "FullAddress":address,
                    "centroid":centroid
                },
                "_id":item["_id"]
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