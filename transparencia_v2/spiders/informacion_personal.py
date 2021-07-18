import scrapy
import pandas as pd
from transparencia_v2.items import InformacionEntidadItem, PersonaItem
from datetime import datetime

class InformacionPersonalSpider(scrapy.Spider):
    name = 'informacion_personal'
    YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, *args, **kwargs):
        super(InformacionPersonalSpider, self).__init__(*args, **kwargs)
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

        # Periodo a procesar
        self.anho = self.codmes[:4]
        self.mes = self.codmes[-2:]

        self.filepath_log = self.out_path + f'{self.anho}{self.mes}_informacion_personal_items_{self.YYYYMMDD_HHMMSS}.txt'
        with open(self.filepath_log, 'w') as file:
            file.write('tipo_poder_id\ttipo_poder_nombre\tcategoria\tentidad_id\tentidad_nombre\tpersonal_url\testado\n')

    def start_requests(self):
        for entidad_idx, entidad in self.entidades_df.iterrows():    
            meta = {
                'tipo_poder_id': entidad['tipo_poder_id'],
                'tipo_poder_nombre': entidad['tipo_poder_nombre'],
                'categoria': entidad['categoria'],
                'entidad_id': entidad['entidad_id'],
                'entidad_nombre': entidad['entidad_nombre'],
                'cookiejar': entidad_idx + 1
            }
            file_url = f"{self.personal_URL}id_entidad={entidad['entidad_id']}&in_anno_consulta={self.anho}&ch_mes_consulta={self.mes}"
            yield scrapy.Request(url=file_url, meta=meta, callback=self.parse)

    def parse(self, response):
        tipo_poder_id = response.meta.get('tipo_poder_id')
        tipo_poder_nombre = response.meta.get('tipo_poder_nombre')
        categoria = response.meta.get('categoria')
        entidad_id = response.meta.get('entidad_id')
        entidad_nombre = response.meta.get('entidad_nombre')
        url = response.request.url
        line = f'{tipo_poder_id}\t{tipo_poder_nombre}\t{categoria}\t{entidad_id}\t{entidad_nombre}\t{url}\t'
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
                persona['vc_personal_paterno'] = columns[5].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_materno'] = columns[6].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_nombres'] = columns[7].replace('\n','').replace('\r',' ').replace('\t',' ').replace('Ð','Ñ').replace('¥','Ñ').replace('+','E').replace(',','').replace('É','E').replace('Á','A').replace('Í','I').replace('Ó','O').replace('Ú','U').strip()
                persona['vc_personal_cargo'] = columns[8].replace('\n','').replace('\r',' ').replace('\t',' ').replace('\r',' ').replace('\t',' ').strip()
                persona['vc_personal_dependencia'] = columns[9].replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                persona['mo_personal_remuneraciones'] = columns[10].replace('\n','').strip()
                persona['mo_personal_honorarios'] = columns[11].replace('\n','').strip()
                persona['mo_personal_incentivo'] = columns[12].replace('\n','').strip()
                persona['mo_personal_gratificacion'] = columns[13].replace('\n','').strip()
                persona['mo_personal_otros_beneficios'] = columns[14].replace('\n','').strip()
                persona['mo_personal_total'] = columns[15].replace('\n','').strip()
                persona['vc_personal_observaciones'] = columns[16].replace('\n','').replace('\r',' ').replace('\t',' ').strip()
                persona['fec_reg'] = columns[17].replace('\n','').strip()
                personas.append(dict(persona))
            
            entidad = InformacionEntidadItem()
            entidad['tipo_poder_id'] = tipo_poder_id
            entidad['tipo_poder_nombre'] = tipo_poder_nombre
            entidad['categoria'] = categoria
            entidad['entidad_id'] = entidad_id
            entidad['entidad_nombre'] = entidad_nombre
            entidad['personas'] = personas

            line += 'SUCESS\n'
            yield(entidad)
        else:
            line += 'ERROR\n'
        
        with open(self.filepath_log, 'a') as file:
            file.write(line)