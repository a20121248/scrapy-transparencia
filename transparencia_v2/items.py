# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class EntidadItem(scrapy.Item):
    tipo_poder_id = scrapy.Field()
    tipo_poder_nombre = scrapy.Field()
    categoria = scrapy.Field()
    entidad_id = scrapy.Field()
    entidad_nombre = scrapy.Field()
    pass

class InformacionPersonaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
