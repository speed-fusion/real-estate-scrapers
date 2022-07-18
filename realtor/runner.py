import os

while True:
    try:
        os.system("scrapy crawl property_surrounding")
    except Exception as e:
        print(f'error : {str(e)}')
        