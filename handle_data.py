from contextlib import nullcontext
import pandas as pd
import numpy as np

class Handle_data():

    def __init__(self):
        pass

    def load_estrazione_biglietti(data_path, sep):
        return pd.read_csv(
        data_path,
        sep=sep,
        header=0,
        names=["Num Biglietto","Cod Linea","Cod Tratta","Data Prima Emissione","Anno Prima Emissione","Settimana Prima Emissione","Num Viaggio Pax","Flg Viaggio Annullato","Flg Viaggio Sostituito","Data Viaggio","Ora Viaggio","Settimana Viaggio","Mese Viaggio","Anno Viaggio","Cluster Emissione","Gruppi/individuali","Des Stagione","Con BUS","Con moto","Flg Tar Res","Tipo Biglietto","Flg Sistemazione","Con veicoli","Con Auto","Con ML_senza BUS","Id Campagna","Tripletta troncata","Flg Var/Sost","Data Variazione","Nucleo Viaggiante","Composizione Nucleo","Cod Nave","Bgt sostituzione_NEW","Nro Passeggeri","Importo incasso","Nro Auto","Nro Veicoli","Nro Moto","Tot Mt Lin","Imp fruito  Claim"]
    ) 

    def load_rilevazione_prezzi(data_path, sep):
        return pd.read_csv(
        data_path,
        sep=sep,
        header=0,
        names=["DataRil","IdVettore","IdLinea","IdTrattaCn","VggMese","VggDataCn","VggPartCn","PrzPax","PrzPol","PrzCab","PrzAut","Prom","PacPol01Cn","PacPol02Cn","PacPol03Cn","PacPol04Cn","PacPol05Cn","PacPol06Cn","PacCab01Cn","PacCab02Cn","PacCab03Cn","PacCab04Cn","PacCab05Cn","PacCab06Cn","PacLet01Cn","PacFrg01Cn","PacFrg02Cn", "Settimana Viaggio"]
    )

    def filter_data_estrazione_biglietti(data):
        data = data[data['Flg Var/Sost']!='S']
        data = data[data['Gruppi/individuali']!='Gruppo']
        data = data[data['Nro Passeggeri']!=0]
        return data

    def get_market_segments_from_estrazione_biglietti(data):
        dataC = data[data['Flg Sistemazione'] == 'P']
        dataC_noAuto = dataC[dataC['Nro Auto'] == 0]
        dataC_Auto = dataC[dataC['Nro Auto'] == 1]

        dataC0 = dataC_noAuto[dataC_noAuto['Composizione Nucleo'] == '1A + 0B']

        dataC1 = dataC_Auto[dataC_Auto['Composizione Nucleo'] == '1A + 0B']
        dataC2 = dataC_Auto[dataC_Auto['Composizione Nucleo'] == '2A + 0B']
        dataC3 = dataC_Auto[dataC_Auto['Composizione Nucleo'] == '2A + 1B']
        dataC4 = dataC_Auto[dataC_Auto['Composizione Nucleo'] == '2A + 2B']
        dataC5 = dataC_Auto[dataC_Auto['Composizione Nucleo'] == '4A + 0B']
        return dataC0, dataC1, dataC2, dataC3, dataC4, dataC5

    def convert_data_rilevazione_prezzi(data):
        for i in range(len(data)):
            #print(data.loc[i, 'VggDataCn'])
            date = data.loc[i, 'VggDataCn']
            if date[3:6] == 'giu':
                data.loc[i, 'MeseViaggo']='06'
            elif date[3:6] == 'lug':
                data.loc[i, 'MeseViaggo']='07'
            elif date[3:6] == 'ago':
                data.loc[i, 'MeseViaggo']='08'
            elif date[3:6] == 'set':
                data.loc[i, 'MeseViaggo']='09'

        data['Data']=data['VggDataCn'].str[:3]+data['MeseViaggo']+data['VggDataCn'].str[6:]
        from datetime import datetime
        #data['Data']=datetime.strptime(data['Data'].to_string(), '%d-%m-%y')
        data['DataViaggioDateFormat']=pd.to_datetime(data['Data'], format='%d-%m-%y' )

    def get_line_data_rilevazione_prezzi(line, data):
        data_line = data[data['IdLinea']==line]
        return data_line

    def get_only_GNV_from_rilevazione_prezzi(data):
        data_GNV = data[data['IdVettore']=='GN'] 
        return data_GNV
    
    def find_and_get_competitors_rilevazione_prezzi(data, competitor):
        data_competitor = data[data['IdVettore']==competitor]
        if data_competitor.empty:
            print("GNV does not have this competitor for this line")
            return
        else:
            return data_competitor
    
    