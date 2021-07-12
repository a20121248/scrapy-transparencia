from transparencia_v2.items import EntidadItem
import scrapy

class EntidadesSpider(scrapy.Spider):
    name = 'entidades'

    def __init__(self):
        self.init_URL = 'https://www.transparencia.gob.pe'

    def start_requests(self):
        meta = {
            'cookiejar': 1
        }
        yield scrapy.Request(url=self.init_URL, meta=meta)

    def parse(self, response):
        tipos = response.selector.xpath("//p[contains(@class, 'list-link')]/a")
        for con,tipo in enumerate(tipos):
            tipo_poder_URL = tipo.xpath('./@href').extract_first()
            lst = tipo_poder_URL.split('=')
            tipo_poder_id = lst[-1] if len(lst) > 1 else -1
            tipo_poder_nombre = tipo.xpath('./text()').extract_first().replace('\n','').strip()
            meta={
                'tipo_poder_id': tipo_poder_id,
                'tipo_poder_nombre': tipo_poder_nombre,
                'cookiejar': con+1
            }
            yield scrapy.Request(url=response.urljoin(tipo_poder_URL), callback=self.parse_entidades, meta=meta)        

    def parse_entidades(self, response):        
        bloques = response.selector.xpath("//div[@class='row bloque-cont']/div/div")
        for bloque in bloques:
            entidades = bloque.xpath("./div[2]/ul/li/a")
            for entidad in entidades:
                item = EntidadItem()
                item['tipo_poder_id'] = response.meta.get('tipo_poder_id')
                item['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
                item['categoria'] = bloque.xpath("./div[1]/h4/text()").extract_first().replace('\n','').strip()
                item['entidad_id'] = entidad.xpath('./@href').extract_first().split('=')[-1]
                item['entidad_nombre'] = entidad.xpath('./text()').extract_first()
                yield item