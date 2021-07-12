import scrapy
import pandas as pd
from transparencia_v2.items import InformacionEntidadItem, PersonaItem
from datetime import datetime

YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

class InformacionPersonalSpider(scrapy.Spider):
    name = 'informacion_personal'

    def __init__(self):
        self.in_path = './1_INPUT/'
        self.out_path = './2_OUTPUT/'
        self.personal_URL = 'http://www.transparencia.gob.pe/personal/pte_transparencia_personal_genera.aspx?ch_tipo_regimen=0&vc_dni_funcionario=&vc_nombre_funcionario=&ch_tipo_descarga=1&'

        col = 'entidad_id'
        cols = ['tipo_poder_id','tipo_poder_nombre','categoria',col,'entidad_nombre']

        # INPUT: Entidades
        entidades_filepath = self.in_path + 'entidades.txt'
        entidades_df = pd.read_csv(entidades_filepath, sep='\t', usecols=cols, encoding='utf8')
        # INPUT: Filtrar
        entidades_filtrar_filepath = self.in_path + 'entidades_filtrar.txt'
        entidades_filtrar_lst = pd.read_csv(entidades_filtrar_filepath, sep='\t', usecols=[col])[col].to_list()

        # Entidades filtradas
        self.entidades_df = entidades_df[~entidades_df[col].isin(entidades_filtrar_lst)]

        # Periodos a consultar
        periodos_filepath = self.in_path + 'periodos.txt'
        cols = ['anho','mes']
        types = {'anho': str, 'mes': str}
        self.periodos_df = pd.read_csv(periodos_filepath, sep='\t', usecols=cols, dtype=types)

    def start_requests(self):
        for periodo_idx, periodo in self.periodos_df.iterrows():
            for entidad_idx, entidad in self.entidades_df.iterrows():    
                meta = {
                    'tipo_poder_id': entidad['tipo_poder_id'],
                    'tipo_poder_nombre': entidad['tipo_poder_nombre'],
                    'categoria': entidad['categoria'],
                    'entidad_id': entidad['entidad_id'],
                    'entidad_nombre': entidad['entidad_nombre'],
                    'cookiejar': periodo_idx + entidad_idx + 1
                }
                file_url = f"{self.personal_URL}id_entidad={entidad['entidad_id']}&in_anno_consulta={periodo['anho']}&ch_mes_consulta={periodo['mes']}"
                yield scrapy.Request(url=file_url, meta=meta, callback=self.parse)

    def parse(self, response):
        if response.text != '' :
            rows = response.selector.xpath("//tr")[1:]
            personas = []
            for row in rows:
                columns = row.xpath("./td/text()").extract()
                persona = PersonaItem()
                persona['pk_id_personal'] = columns[0].replace('\n','').strip()
                persona['vc_personal_ruc_entidad'] = columns[1].replace('\n','').strip()
                persona['in_personal_anno'] = columns[2].replace('\n','').strip()
                persona['in_personal_mes'] = columns[3].replace('\n','').strip()
                persona['vc_personal_regimen_laboral'] = columns[4].replace('\n','').strip()
                persona['vc_personal_paterno'] = columns[5].replace('\n','').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_materno'] = columns[6].replace('\n','').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_nombres'] = columns[7].replace('\n','').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_cargo'] = columns[8].replace('\n','').strip()
                persona['vc_personal_dependencia'] = columns[9].replace('\n','').strip()
                persona['mo_personal_remuneraciones'] = columns[10].replace('\n','').strip()
                persona['mo_personal_honorarios'] = columns[11].replace('\n','').strip()
                persona['mo_personal_incentivo'] = columns[12].replace('\n','').strip()
                persona['mo_personal_gratificacion'] = columns[13].replace('\n','').strip()
                persona['mo_personal_otros_beneficios'] = columns[14].replace('\n','').strip()
                persona['mo_personal_total'] = columns[15].replace('\n','').strip()
                persona['vc_personal_observaciones'] = columns[16].replace('\n','').strip()
                persona['fec_reg'] = columns[17].replace('\n','').strip()
                personas.append(dict(persona))
            
            entidad = InformacionEntidadItem()
            entidad['tipo_poder_id'] = response.meta.get('tipo_poder_id')
            entidad['tipo_poder_nombre'] = response.meta.get('tipo_poder_nombre')
            entidad['categoria'] = response.meta.get('categoria')
            entidad['entidad_id'] = response.meta.get('entidad_id')
            entidad['entidad_nombre'] = response.meta.get('entidad_nombre')
            entidad['personas'] = personas

            yield(entidad)