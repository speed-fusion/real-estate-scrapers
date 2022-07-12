
import scrapy
import pymongo
from scrapy.exceptions import DropItem
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

class ZillowPipeline:
    
    def __init__(self):
        self.db = Database()
    
    def process_item(self, item, spider):
        # print(item)
        if item["event"] == "update":
            self.db.data_collection.update_one(
                {"_id":item["_id"]},
                {"$set":item["data"]}
            )
            
            if "error_count" in item["data"]:
                raise DropItem(f"Failed in {item}")
        
            
        return item
    
        


class RentzestSpider(scrapy.Spider):
    name = 'rentzest'
    handle_httpstatus_list = [400]
    suggestion_url = "https://www.zillowstatic.com/autocomplete/v3/suggestions?q={}"
    custom_settings = {
         "ROBOTSTXT_OBEY":False,
         "COOKIES_ENABLED":False,
         "RETRY_ENABLED":True,
         "CONCURRENT_REQUESTS":10,
         "COOKIES_ENABLED":False,
         "RETRY_TIMES":3,
        #  "AUTOTHROTTLE_ENABLED":True,
        #  "AUTOTHROTTLE_MAX_DELAY":True,
        #  "AUTOTHROTTLE_START_DELAY":5,
        #  "AUTOTHROTTLE_MAX_DELAY":0.8,
        #  "AUTOTHROTTLE_TARGET_CONCURRENCY":10,
        #  "AUTOTHROTTLE_DEBUG":True,
         "ITEM_PIPELINES" : {
            ZillowPipeline: 300,
        }
    }
    
    username = os.environ.get("PROXY_USERNAME")
    password = os.environ.get("PROXY_PASSWORD")

    max_records = 5
    
    db = Database()
    
    # proxy = f'http://{username}:{password}@gate.smartproxy.com:10000'
    proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    # proxy = f'http://{username}:{password}@gate.dc.smartproxy.com:20000'
    
    suggestion_headers = {
          'authority': 'www.zillowstatic.com',
          'pragma': 'no-cache',
          'cache-control': 'no-cache',
          'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
          'sec-ch-ua-platform': '"Windows"',
          'accept': '*/*',
          'origin': 'https://www.zillow.com',
          'sec-fetch-site': 'cross-site',
          'sec-fetch-mode': 'cors',
          'sec-fetch-dest': 'empty',
          'accept-language': 'en-US,en;q=0.9'
        }
    info_headers = {
          'authority': 'www.zillow.com',
          'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
          'sec-ch-ua-platform': '"Windows"',
          'content-type': 'text/plain',
          'accept': '*/*',
          'origin': 'https://www.zillow.com',
          'sec-fetch-site': 'same-origin',
          'sec-fetch-mode': 'cors',
          'sec-fetch-dest': 'empty',
          'referer': 'https://www.zillow.com',
          'accept-language': 'en-US,en;q=0.9'
        }

    
    def start_requests(self):
        pending_records = list(self.db.data_collection.find({"status":0,"error_count":{"$lt":6}}).limit(self.max_records))
        # pending_records = [{"query":"1590 smithson dr lithonia ga 30058"}]
        
        for record in pending_records:
            yield scrapy.Request(
                self.suggestion_url.format(record["query"]),
                headers=self.suggestion_headers,
                meta= {"record":record,"proxy":self.proxy},
                callback=self.parse_suggestion
            )

    def parse_suggestion(self, response):
        record = response.meta["record"]
        try:
            json_data =  response.json()
            results = json_data["results"]
            if len(results) > 0:
                zpid = str(results[0]["metaData"]["zpid"])
                url = 'https://www.zillow.com/graphql/?zpid='+zpid+'&contactFormRenderParameter=&queryId=d64cbbc1458567321829e9cf1283438c&operationName=ForRentDoubleScrollFullRenderQuery'
                payload = '{"operationName":"ForRentDoubleScrollFullRenderQuery","variables":{"zpid":'+zpid+',"contactFormRenderParameter":{"zpid":'+zpid+',"platform":"desktop","isDoubleScroll":true}},"clientVersion":"home-details/6.0.11.6224.master.d2a245b","queryId":"d64cbbc1458567321829e9cf1283438c"}'
                record["zpid"] = zpid
                yield scrapy.Request(
                    url=url,
                    body=payload,
                    method="POST",
                    headers=self.info_headers,
                    callback=self.parse_info,
                    meta={"record":record,"proxy":self.proxy,'dont_redirect': True}
                )
                
            else:
                message = "address not found on zillow."
                yield {
                "event":"update",
                "data":{
                    "error_count":record["error_count"] + 1,
                    "status":1,
                    "message":message
                },
                "_id":record["_id"]
            }
        except Exception as e:
            message = str(e)
            yield {
                "event":"update",
                "data":{
                    "error_count":record["error_count"] + 1,
                    "status":1,
                    "message":message
                },
                "_id":record["_id"]
            }
    
    def parse_info(self,response):
        record = response.meta["record"]
        
        try:
            property_info =  response.json()
            try:
                zestimate = property_info["data"]["property"]["zestimate"]
            except:
                zestimate = None
            try:
                rentZestimate = property_info["data"]["property"]["rentZestimate"]
            except:
                rentZestimate = None
            
            yield {
                "event":"update",
                "data":{
                    "zestimate":zestimate,
                    "rentZestimate":rentZestimate,
                    "status":2,
                    "zpid":record["zpid"]
                },
                "_id":record["_id"]
            }
            
        except Exception as e:
            message = str(e)
            yield {
                "event":"update",
                "data":{
                    "error_count":record["error_count"] + 1,
                    "message":message
                },
                "_id":record["_id"]
            }
            
        
