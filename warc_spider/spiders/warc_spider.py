import scrapy
from scrapy.http import Request

class WarcSpider(scrapy.Spider):
    name = 'warc_spider'
    allowed_domains = ['*']

    def start_requests(self):
        with open('input.csv', 'r') as f:
            for line in f.readlines():
                url = line.strip()
                yield Request(url, self.parse, meta={
                    'download_slot': url,
                    "zyte_api": {
                        "browserHtml": True
                    }
                })

    def parse(self, response):
        headers = {}
        for k, v in response.headers.items():
            headers[k.decode()] = ', '.join(map(bytes.decode, v))
        
        yield {
            'url': response.url,
            'content': response.text,
            'headers': headers,
        }
