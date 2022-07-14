import os

while True:
    try:
        os.system("scrapy crawl property_id")
    except Exception as e:
        print(f'error : {str(e)}')
        