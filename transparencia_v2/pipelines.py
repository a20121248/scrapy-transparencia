import pandas as pd
from transparencia_v2.items import EntidadItem, InformacionEntidadItem

class csvWriterPipeline(object):
    out_path = './2_OUTPUT/'
    items_written_infogeneral = 0

    def open_spider(self, spider):
        spider_name = type(spider).__name__
        if spider_name == 'InformacionPersonalSpider':
            self.ctd_save_infogeneral = 1
            self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','pk_id_personal','vc_personal_ruc_entidad','in_personal_anno','in_personal_mes','vc_personal_regimen_laboral','vc_personal_paterno','vc_personal_materno','vc_personal_nombres','vc_personal_cargo','vc_personal_dependencia','mo_personal_remuneraciones','mo_personal_honorarios','mo_personal_incentivo','mo_personal_gratificacion','mo_personal_otros_beneficios','mo_personal_total','vc_personal_observaciones','fec_reg']
            self.prename_infogeneral = f"{self.out_path}{getattr(spider, 'codmes')}_informacion_personal_{getattr(spider, 'YYYYMMDD_HHMMSS')}.txt"
        elif spider_name == 'EntidadesSpider':
            self.ctd_save_infogeneral = 5
            self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre']
            self.prename_infogeneral = f"{self.out_path}entidades_{getattr(spider, 'YYYYMMDD_HHMMSS')}.txt"
        elif spider_name == 'EntidadesTotalSpider':
            self.ctd_save_infogeneral = 5
            self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre']
            self.prename_infogeneral = f"{self.out_path}entidades_total_{getattr(spider, 'YYYYMMDD_HHMMSS')}.txt"
        self.data_encontrada_infogeneral = []

    def process_item(self, item, spider):
        if isinstance(item, InformacionEntidadItem):
            item_df = pd.json_normalize(item['personas'])
            item_df['tipo_poder_id'] = item['tipo_poder_id']
            item_df['tipo_poder_nombre'] = item['tipo_poder_nombre']
            item_df['categoria'] = item['categoria']
            item_df['entidad_id'] = item['entidad_id']
            item_df['entidad_nombre'] = item['entidad_nombre']
        elif isinstance(item, EntidadItem):
            item_df = pd.DataFrame([item], columns=item.keys())
        
        self.items_written_infogeneral += 1
        self.data_encontrada_infogeneral.append(item_df)
        if self.items_written_infogeneral % self.ctd_save_infogeneral == 0:
            self.data_encontrada_infogeneral = self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        return item

    def guarda_data(self, lista_df, ctd_items=0):
        pd.concat(lista_df).to_csv(self.prename_infogeneral, sep='\t', header=ctd_items<=self.ctd_save_infogeneral, index=False, encoding="utf-8", columns=self.columnsPrincipal, mode='a')
        return [pd.DataFrame(columns = self.columnsPrincipal)]

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral)
        print(25*'=','THE END',25*'=')