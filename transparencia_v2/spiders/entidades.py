import scrapy
from datetime import datetime
from transparencia_v2.items import EntidadItem

class EntidadesSpider(scrapy.Spider):
    name = 'entidades'
    YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self) -> None:
        self.out_path = './2_OUTPUT/'
        self.init_URL = 'https://www.transparencia.gob.pe'
        self.personal_URL = 'https://transparencia.gob.pe/enlaces/pte_transparencia_enlaces.aspx?id_tema=32&ver=&'

        filepath = self.out_path + f'entidades_items_{self.YYYYMMDD_HHMMSS}.txt'
        with open(filepath, 'w') as file:
            file.write('tipo_poder_id\ttipo_poder_nombre\tcategoria\tentidad_id\tentidad_nombre\tpersonal_url\testado\n')

    def start_requests(self):
        meta = {
            'cookiejar': 1
        }
        yield scrapy.Request(url=self.init_URL, meta=meta)

    def parse(self, response):
        tipos = response.selector.xpath("//p[contains(@class, 'list-link')]/a")
        for i,tipo in enumerate(tipos):
            tipo_poder_URL = tipo.xpath('./@href').extract_first()
            lst = tipo_poder_URL.split('=')
            tipo_poder_id = lst[-1] if len(lst) > 1 else -1
            tipo_poder_nombre = tipo.xpath('./text()').extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip()
            meta = {
                'tipo_poder_id': tipo_poder_id,
                'tipo_poder_nombre': tipo_poder_nombre,
                'cookiejar': i + 1
            }
            yield scrapy.Request(url=response.urljoin(tipo_poder_URL), callback=self.parse_entidades, meta=meta)

    def parse_entidades(self, response):        
        bloques = response.selector.xpath("//div[@class='row bloque-cont']/div/div")
        for bloque in bloques:
            entidades = bloque.xpath("./div[2]/ul/li/a")
            for i,entidad in enumerate(entidades):
                entidad_id = entidad.xpath('./@href').extract_first().split('=')[-1]
                meta = {
                    'tipo_poder_id': response.meta.get('tipo_poder_id'),
                    'tipo_poder_nombre': response.meta.get('tipo_poder_nombre'),
                    'categoria': bloque.xpath("./div[1]/h4/text()").extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip(),
                    'entidad_id': entidad_id,
                    'entidad_nombre': entidad.xpath('./text()').extract_first().replace('\n','').replace('\r',' ').replace('\t',' ').strip(),
                    'cookiejar': response.meta.get('cookiejar') + i + 1
                }
                yield scrapy.Request(url=f'{self.personal_URL}id_entidad={entidad_id}', callback=self.parse_entidad, meta=meta)
        
    def parse_entidad(self, response):            
        mensaje = response.selector.xpath("//table//tr[2]/td/text()").extract_first()
        tipo_poder_id = response.meta.get('tipo_poder_id')
        tipo_poder_nombre = response.meta.get('tipo_poder_nombre')
        categoria = response.meta.get('categoria')
        entidad_id = response.meta.get('entidad_id')
        entidad_nombre = response.meta.get('entidad_nombre')
        url = response.request.url
        line = f'{tipo_poder_id}\t{tipo_poder_nombre}\t{categoria}\t{entidad_id}\t{entidad_nombre}\t{url}\t'
        if mensaje == 'No se han encontrado coincidencias con la palabras registradas':
            line += 'SUCCESS\n'
            item = EntidadItem()
            item['tipo_poder_id'] = response.meta.get('tipo_poder_id')
            item['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
            item['categoria'] = response.meta.get('categoria')
            item['entidad_id'] = response.meta.get('entidad_id')
            item['entidad_nombre'] = response.meta.get('entidad_nombre')
            yield item
        else:
            line += 'ERROR\n'
        
        filepath = self.out_path + f'entidades_items_{self.YYYYMMDD_HHMMSS}.txt'
        with open(filepath, 'a') as file:
            file.write(line)