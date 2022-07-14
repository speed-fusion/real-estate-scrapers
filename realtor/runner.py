import os

while True:
    try:
        os.system("scrapy crawl estimates")
    except Exception as e:
        print(f'error : {str(e)}')
        