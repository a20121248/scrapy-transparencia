import pandas as pd
from transparencia_v2.items import EntidadItem, InformacionEntidadItem
from datetime import datetime

YYYYMMDD_HHMMSS = datetime.now().strftime("%Y%m%d_%H%M%S")

class TransparenciaV2Pipeline:
    def process_item(self, item, spider):
        return item

class csvWriterPipeline(object):
    out_path = './2_OUTPUT/'

    def __init__(self) -> None:
        self.items_written_infogeneral = 0
        self.columnsPrincipal = ['tipo_poder_id','tipo_poder_nombre','categoria','entidad_id','entidad_nombre','pk_id_personal','vc_personal_ruc_entidad','in_personal_anno','in_personal_mes','vc_personal_regimen_laboral','vc_personal_paterno','vc_personal_materno','vc_personal_nombres','vc_personal_cargo','vc_personal_dependencia','mo_personal_remuneraciones','mo_personal_honorarios','mo_personal_incentivo','mo_personal_gratificacion','mo_personal_otros_beneficios','mo_personal_total','vc_personal_observaciones','fec_reg']
        self.data_encontrada_infogeneral = pd.DataFrame(columns = self.columnsPrincipal)
        self.ctd_save_infogeneral = 2
        self.prename_infogeneral = ''
        pass

    def process_item(self, item, spider):
        if isinstance(item, InformacionEntidadItem):
            self.prename_infogeneral = f'{self.out_path}{YYYYMMDD_HHMMSS}_informacion_personal.txt'
            
            self.data_encontrada_infogeneral = pd.json_normalize(item['personas'])
            self.data_encontrada_infogeneral['tipo_poder_id'] = item['tipo_poder_id']
            self.data_encontrada_infogeneral['tipo_poder_nombre'] = item['tipo_poder_nombre']
            self.data_encontrada_infogeneral['categoria'] = item['categoria']
            self.data_encontrada_infogeneral['entidad_id'] = item['entidad_id']
            self.data_encontrada_infogeneral['entidad_nombre'] = item['entidad_nombre']

            self.items_written_infogeneral += 1
            if self.items_written_infogeneral % self.ctd_save_infogeneral == 0 :
                self.data_encontrada_infogeneral = self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral, self.prename_infogeneral)
            return item

    def guarda_data(self, df_gral, ctd_items=0, prename=''):
        df_gral.to_csv(self.prename_infogeneral, sep='\t', header=ctd_items<=self.ctd_save_infogeneral, index=False, encoding="utf-8", columns=self.columnsPrincipal, mode='a')
        return None

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral, self.prename_infogeneral)
        print(25*'=','THE END',25*'=')
    
    """
    def guarda_data(self, lista_datos, ctd_items=0, prename=''):
        df_gral = pd.DataFrame(lista_datos, columns=self.columnsPrincipal)
        print('ctd_items=',ctd_items)
        print('ctd_save_infogeneral=',self.ctd_save_infogeneral)
        df_gral.to_csv(self.prename_infogeneral, sep='\t',header=ctd_items<=self.ctd_save_infogeneral,index=False, encoding="utf-8",mode='a')
        return []

    def __del__(self):
        self.guarda_data(self.data_encontrada_infogeneral, self.items_written_infogeneral, self.prename_infogeneral)
        print(25*'=','THE END',25*'=')
    """
