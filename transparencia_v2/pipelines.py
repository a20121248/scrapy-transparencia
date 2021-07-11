import pandas as pd
from transparencia_v2.items import EntidadItem, InformacionPersonaItem
from datetime import datetime

YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

class TransparenciaV2Pipeline:
    def process_item(self, item, spider):
        return item

class csvWriterPipeline(object):
    out_path = './2_OUTPUT/'

    def __init__(self) -> None:
        self.items_written_infogeneral = 0
        self.data_encontrada_infogeneral = []
        self.ctd_save_infogeneral = 1000
        self.prename_infogeneral = f'{self.out_path}{YYYYMMDD_HHMMSS}_entidades'
        pass

    def process_item(self, item, spider):
        if isinstance(item, EntidadItem) or isinstance(item, InformacionPersonaItem):
            if spider.name == 'entidades':
                self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre']
            else:
                self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre']
            self.items_written_infogeneral += 1
            self.data_encontrada_infogeneral.append(item)
            if self.items_written_infogeneral % self.ctd_save_infogeneral == 0 :
                self.data_encontrada_infogeneral = self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral, self.prename_infogeneral)        
            return item
    
    def guarda_data(self, lista_datos, ctd_items=0, prename=''):
        df_gral = pd.DataFrame(lista_datos, columns=self.columnsPrincipal)
        fname_gral = prename + '.txt'
        print('ctd_items=',ctd_items)
        print('ctd_save_infogeneral=',self.ctd_save_infogeneral)
        df_gral.to_csv(fname_gral, sep='\t',header=ctd_items<=self.ctd_save_infogeneral,index=False, encoding="utf-8",mode='a')
        return []

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral, self.prename_infogeneral)
        print(25*'=','THE END',25*'=')
