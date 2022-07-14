import scrapy
import pymongo
import json



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
        
        
class EstimatesPipeline:
    
    def __init__(self):
        self.db = Database()
    
    def process_item(self, item, spider):
        
        if item["event"] == "update":
            self.db.data_collection.update_one(
                {"_id":item["_id"]},
                {"$set":item["data"]}
            )
            
        return item
    
    

class EstimatesSpider(scrapy.Spider):
    name = 'estimates'
    
    scrapy_id = "1"
    
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":500,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "LOG_LEVEL":"DEBUG",
         "ITEM_PIPELINES" : {
            EstimatesPipeline: 300,
        }
    }
 
    headers = {
      'authority': 'www.realtor.com',
      'pragma': 'no-cache',
      'cache-control': 'no-cache',
      'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
      'sec-ch-ua-mobile': '?0',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
      'content-type': 'application/json',
      'accept': 'application/json',
      'sec-ch-ua-platform': '"Windows"',
      'origin': 'https://www.realtor.com',
      'sec-fetch-site': 'same-origin',
      'sec-fetch-mode': 'cors',
      'sec-fetch-dest': 'empty',
      'accept-language': 'en-US,en;q=0.9'
    }
 
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")
    
    max_records = 5000
    
    db = Database()
    
    proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    
    def start_requests(self):
        for item in list(self.db.data_collection.find({"property_id":{"$exists":True},"status":1}).limit(self.max_records)):
            if "property_id" in item:
                property_id = item["property_id"]
                url = "https://www.realtor.com/api/v1/hulk?client_id=rdc-x&schema=vesta"

                payload = json.dumps({
                    "query": None,
                    "queryLoader": {
                        "appType": "FOR_SALE",
                        "pageType": "LDP",
                        "serviceType": "HOME_ESTIMATES"
                    },
                    "propertyId":property_id,
                    "callfrom": "LDP",
                    "nrQueryType": "HOME_ESTIMATES",
                    "variables": {
                        "propertyId": property_id,
                        "historicalMin": "2016-11-23",
                        "historicalMax": "2021-11-23",
                        "forecastMax": "2022-09-23"
                    },
                    "isClient": True
                    })
                
                yield scrapy.Request(url,method="POST",headers=self.headers,body=payload,callback=self.parse_property_estimates,meta={"item":item})
            # else:
            #     message = "property id missing"
            #     yield {
            #         "event":"update",
            #         "data":{
            #             "message":message,
            #             "status":5
            #         },
            #         "_id":item["_id"]
            #     }
    def parse_property_estimates(self, response):
        item = response.meta["item"]
        
        if not "error_count" in item:
            item["error_count"]= 0
        try:
            temp = {}
            json_data =  response.json()
            for source in json_data["data"]["home"]["estimates"]["current_values"]:
               temp[source["source"]["type"]] = source["estimate"]
               
            temp["status"] = 3
            
            yield {
                "event":"update",
                "data":temp,
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