import os

while True:
    try:
        os.system("scrapy crawl property_data")
    except Exception as e:
        print(f'error : {str(e)}')
        