import scrapy


class InformacionPersonalSpider(scrapy.Spider):
    name = 'informacion_personal'
    allowed_domains = ['transparencia.com']
    start_urls = ['http://transparencia.com/']

    def parse(self, response):
        pass
