import scrapy

import json
import pymongo
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

class PropertySurroundingPipeline:
    
    def __init__(self):
        self.db = Database()
    
    def process_item(self, item, spider):
        
        if item["event"] == "update":
            self.db.data_collection.update_one(
                {"_id":item["_id"]},
                {"$set":item["data"]}
            )
            
        return item
    
    
    
    


class PropertySurroundingSpider(scrapy.Spider):
    name = 'property_surrounding'
    

    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":128,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
         "ITEM_PIPELINES" : {
            PropertySurroundingPipeline: 300,
        }
    }
    # Bearer eyJraWQiOiI3MzNhMjk3YWFkNmVmODEwNjJhOWZkOGYwZWEzZGY2ODE5MjNjYzhiMzRlYjczZTk0NGYwNGJjY2Q0NDdmODNhIiwiYWxnIjoiUlM1MTIifQ.eyJpc3MiOiJhY2NvdW50cy5yZWFsdG9yLmNvbSIsImlhdCI6MTY0NTA3MjI2MiwiZXhwIjoxNjQ1MTU4NjYyLCJhdWQiOiJyZWFsdG9yLmNvbSIsInN1YiI6ImNvbnN1bWVyfDYxYThiNGNhYTNmYWY1MDE1YTMxYzkyYyIsImF1dGhvcml6YXRpb24iOiJ1bnByb3ZlbiIsInJkY190dHIiOjE2NDUwNzU4NjJ9.AEYpH-VYV2do3NwwNYRepYaPpTego-KV6516JHNGhV1_6rCMO86KTvCkmjw7z4tb8fxzt2xCBzJUpzJBznBXO-ZpiyrP6V-reZCozMn9_MhiA8lQcN6-Dpu3EEsYk-6O3813BIS9fD5e_C8-jEH9tLo_XiDjPLv7LYJyl-9YJ4g33ZoXn2OkIjtk7fSe7J0BsZa_olIhNAJQx8CYydxwGmMTH77jMIBy0Yf51-JeGJpQiUMYgayvmcwKyePTkb_HIA3lpxbKduan-wSqVE5d2SMO303a3WoDvpya-usinAVkdsGzMgJG0YZWPWytKaPZDut6SKMhnJpzCEuuR1sHWA
    auth_token = "eyJraWQiOiI3MzNhMjk3YWFkNmVmODEwNjJhOWZkOGYwZWEzZGY2ODE5MjNjYzhiMzRlYjczZTk0NGYwNGJjY2Q0NDdmODNhIiwiYWxnIjoiUlM1MTIifQ.eyJpc3MiOiJhY2NvdW50cy5yZWFsdG9yLmNvbSIsImlhdCI6MTY0NTA3MjI2MiwiZXhwIjoxNjQ1MTU4NjYyLCJhdWQiOiJyZWFsdG9yLmNvbSIsInN1YiI6ImNvbnN1bWVyfDYxYThiNGNhYTNmYWY1MDE1YTMxYzkyYyIsImF1dGhvcml6YXRpb24iOiJ1bnByb3ZlbiIsInJkY190dHIiOjE2NDUwNzU4NjJ9.AEYpH-VYV2do3NwwNYRepYaPpTego-KV6516JHNGhV1_6rCMO86KTvCkmjw7z4tb8fxzt2xCBzJUpzJBznBXO-ZpiyrP6V-reZCozMn9_MhiA8lQcN6-Dpu3EEsYk-6O3813BIS9fD5e_C8-jEH9tLo_XiDjPLv7LYJyl-9YJ4g33ZoXn2OkIjtk7fSe7J0BsZa_olIhNAJQx8CYydxwGmMTH77jMIBy0Yf51-JeGJpQiUMYgayvmcwKyePTkb_HIA3lpxbKduan-wSqVE5d2SMO303a3WoDvpya-usinAVkdsGzMgJG0YZWPWytKaPZDut6SKMhnJpzCEuuR1sHWA"
 
    headers = {
    'Accept': 'application/json',
    'Auth': f'Bearer {auth_token}',
    'Content-Type': 'application/json; charset=utf-8',
    'Host': 'graph.realtor.com',
    'Connection': 'Keep-Alive',
    'Accept-Encoding': 'gzip',
    'User-Agent': 'okhttp/3.12.12'
    }
 
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")
    
    db = Database()
    
    proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    
    def start_requests(self):
        
        for item in list(self.db.data_collection.find({"error_count":{"$lt":6},"status":100},{"_id":1,"property_id":1,"error_count":1}).limit(10000)):
            property_id = item["property_id"]
            url = "https://graph.realtor.com/graphql?client_id=rdc_mobile_native%2C10.41.1%2Candroid"

            payload = {"operationName":"GetSurroundingsCardData","variables":{"query":property_id,"enableFlood":True},"query":"query GetSurroundingsCardData($query: ID!, $enableFlood: Boolean!) { home(property_id: $query) { __typename local { __typename flood @include(if: $enableFlood) { __typename flood_factor_score fema_zone } noise { __typename noise_categories { __typename type text } } } } }"}
            
            yield scrapy.Request(url,method="POST",headers=self.headers,body=json.dumps(payload),callback=self.parse_property_surrounding,meta={"proxy":self.proxy,"item":item})
            
            
    
    def parse_property_surrounding(self, response):
        
        item = response.meta["item"]
        error_count = item["error_count"]
        try:
            temp = {}
            
            json_data = response.json()
            try:
                feema_zone = ",".join(json_data["data"]["home"]["local"]["flood"]["fema_zone"]).strip(",")
                temp["FemaFloodZone"] = feema_zone
            except:
                error_count += 1
            
            try:
                noise_categories = {}
                for noise_category in json_data["data"]["home"]["local"]["noise"]["noise_categories"]:
                    noise_categories[noise_category["type"]] = noise_category["text"]
                
                temp.update(noise_categories)
                
                if "score" in temp:
                    temp["overall"] = temp["score"]
                    del temp["score"]
                  
            except:
                error_count += 1
            
            if error_count == item["error_count"]:
                temp["status"] =  5
            
            temp["error_count"] = error_count
            
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
