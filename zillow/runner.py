import os

while True:
    try:
        os.system("scrapy crawl rentzest")
    except Exception as e:
        print(f'error : {str(e)}')
        