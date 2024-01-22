import requests
import pandas as pd
import numpy as np
import json
from datetime import date
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import Day, BDay
from datetime import datetime
from premailer import transform
from tqdm import tqdm
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys
import time
import io
import math

def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
    percent = ('{0:.' + str(decimals) + 'f}').format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix))
    sys.stdout.flush()
    

def create_dataframe_Quantum():
    session = requests.Session()

    url = "https://www.quantumaxis.com.br/webaxis/webaxis2/notAuthorised/login/logar/realizaLogin"

    payload = json.dumps({
      "username": "[Username]",
      "senha": "[Password]",
      "autenticador": None,
      "isNavegadorChrome": True,
      "paginaRedirecionar": None
    })
    headers = {
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID=3E249314BE6B9D280F7FCD4187337D56; logo=Axis'
    }

    test = session.post(url, headers=headers, data=payload)
    cookies = str(test.cookies)
    sessionID = cookies[cookies.find('=')+1:cookies.find(' for')]
    print('JSessionID = ' +sessionID)


    #Getting bearer token

    url = "https://www.quantumaxis.com.br/webaxis/"

    payload = "{\"id\": 1, \"method\": \"system.listMethods\", \"params\": []}"
    headers = {
      'Content-Type': 'text/plain',
      'Cookie': 'JSESSIONID='+sessionID
    }

    test = session.get(url, headers=headers, data=payload)
    cookies = str(test.cookies)
    start_cookie = cookies.find('api-authentication=')+19
    end_cookie = cookies.find(' for www.quantumaxis.com.br', start_cookie)
    auth_code = cookies[start_cookie:end_cookie]
    print('API authentication code =' +auth_code)

    #BUILD PAYLOAD

    #Identify chosen data

    url = "https://www.quantumaxis.com.br/webaxis/webaxis2/selecao/adicionar"

    payload = json.dumps({
      "label": "ranking diário concorrência",
      "identificador": "ranking diário concorrência",
      "tipoItemSelecionavel": "GRUPO_ATIVOS"
    })
    headers = {
      'Content-Type': 'application/json',
      'Cookie': 'JSESSIONID='+sessionID
    }

    test = session.post(url, headers=headers, data=payload)

    #Raw selection data (with id number for each fund)
    url = "https://www.quantumaxis.com.br/webaxis/webaxis2/selecaoGlobal/ajax/obterItensSelecao?"

    payload = {}
    headers = {
        'Cookie': 'JSESSIONID='+sessionID
    }

    test = session.get(url, headers=headers, data=payload)
    raw_data = str(test.text)


    #How many funds are in the request?

    pattern = "identificador"
    count = 0
    flag = True
    start = 0

    while flag:
        a = raw_data.find(pattern, start)
        if a == -1:
            flag = False
        else:
            count += 1        
            start = a + 1
    print("Numero de fundos/indices no QuantumAxis: "+ str(count))

    #Extract all id numbers
    start = 0
    data_list = []
    for i in range(0,count):
        a = raw_data.find('identificador',start)
        b = raw_data.find('}', a)
        x = raw_data[a+18:b]
        start = a+1
        data_list.append(x)

    id_list = []
    tipo_list = []
    for item in data_list:
        data_id = item[0:item.find('\",')-1]
        data_tipo = item[item.find('Selecionavel')+17:-2]
        id_list.append(data_id)
        tipo_list.append(data_tipo)

    #Payload formatting

    final_list = []
    for i in range (0, len(data_list)):
        num = id_list[i]
        tipo = tipo_list[i]
        payload_format = "identificador: '{x}', label: '', tipoItemSelecionavel: {y}".format(x = num, y = tipo)
        payload_format = '{' + payload_format + '}'
        final_list.append(payload_format)

    output = '[' + ', '.join(final_list) + ']'


    keys = ["medida", "modoRelatorio", "selecao", "configuracao", "exibirAtivos", "exibirValoresFundosEuropa"]
    config = "[{medida: Nome, alias: QA},{medida: Cota/Preço de Fechamento, alias: QB, ativos: [], casasDecimais: 2, diasAjuste: "+ "Banco" +", periodicidade: '0', periodo: no dia}]"
    values = ["RELATORIO", "NORMAL", output, config, True, True]
    data_dict = dict(zip(keys,values))

    #Gerar relatorio

    url = "https://www.quantumaxis.com.br/api/acessoDados/v2/relatorio"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + auth_code
    }

    #Here, can directly use json parameter instead of payload and json.dumps
    response1 = session.post(url, headers=headers, json=data_dict)
    try:
        #Extracting data from json
        json_data1 = json.loads(response1.text)
    except Exception as e:
        print("=====================================================")
        print("ERROR ", response1, ". Likely cause: API key extraction from cookie")
        print("=====================================================")
        print("Diagnostics...")
        print("Cookie: ", cookies)
        print("=====================================================")
        print("CODE> start_cookie = cookies.find('api-authentication=')+19")
        print("CODE> end_cookie = cookies.find(' for www.quantumaxis.com.br', start_cookie)")
        print("start_cookie -->", start_cookie, "end_cookie -->", end_cookie)
        print("CODE> auth_code = cookies[start_cookie:end_cookie]")
        print("API key: ", auth_code)
        print("\n ↑↑↑ SEE ABOVE FOR DIAGNOSTICS ↑↑↑ \n")
    
    #VOLATILIDADE
    
    config_VOLA = "[{medida: Nome, alias: QA},{medida: Volatilidade, alias: QB, ativos: [], casasDecimais: 2, diasAjuste: "+ "Banco" +", periodicidade: '0', periodo: nos últimos 30 dias úteis}]"
    values_VOLA = ["RELATORIO", "NORMAL", output, config_VOLA, True, True]
    data_dict_VOLA = dict(zip(keys,values_VOLA))
    response1_VOLA = session.post(url, headers=headers, json=data_dict_VOLA)

    # Volatilidade dados
    json_data1_VOLA = json.loads(response1_VOLA.text)

    # Obtaining 'Banco' date
    header_row1 = str(json_data1['valores'][0][1])
    date1 = header_row1[header_row1.find('('):]

    #Getting exact days, months, and years
    d = int(date1[1:3])
    m = int(date1[4:6])
    y = int(date1[7:-1])
    
    banco_date = date(y,m,d)
    
    #CHECK PARA VER SE PRECISA RODAR
    with open("C:\Users\Thiago\json_master.json", 'r') as file:
        info = json.load(file)
    date_LO = date.fromisoformat(info['LO']['date'])
    status_LO = info['LO']['status'] 
    date_LB = date.fromisoformat(info['LB']['date'])
    status_LB = info['LB']['status'] 
    date_RF = date.fromisoformat(info['RF']['date'])
    status_RF = info['RF']['status']
    date_INST = date.fromisoformat(info['INST']['date'])
    status_INST = info['INST']['status'] 
    date_QUANT = date.fromisoformat(info['QUANT']['date'])
    status_QUANT = info['QUANT']['status'] 
    date_LS = date.fromisoformat(info['LS']['date'])
    status_LS = info['LS']['status'] 
    
    flagLO = False
    flagLB = False
    flagRF = False
    flagINST = False
    flagQUANT = False
    flagLS = False
    
    # Long Only
    if  banco_date > date_LO:
        print("Temos novas cotas para Long Only! Pegando Dados...")
        flagLO=True
        
    elif banco_date == date_LO and status_LO == 'P':
        print("Relatório Completo para LO ainda tem que ser mandado. Checando cotas presentes...")
        flagLO=True
    
    else:
        print("E-mail para Long Only já foi enviado! Relatório nao será gerado")
    
    # Long Bias
    if  banco_date > date_LB:
        print("Temos novas cotas para Long Bias! Pegando Dados...")
        flagLB=True
    
    elif banco_date == date_LB and status_LB == 'P':
        print("Relatório Completo para LB ainda tem que ser mandado. Checando cotas presentes...")
        flagLB=True
    
    else:
        print("E-mail para Long Bias já foi enviado! Relatório nao será gerado")
    
    # Renda Fixa
    if  banco_date > date_RF:
        print("Temos novas cotas para Renda Fixa! Pegando Dados...")
        flagRF=True
        
    elif banco_date == date_RF and status_RF == 'P':
        print("Relatório Completo para RF ainda tem que ser mandado. Checando cotas presentes...")
        flagRF=True
        
    else:
        print("E-mail para Renda Fixa já foi enviado! Relatório nao será gerado")
    
    # Institucional
    if  banco_date > date_INST:
        print("Temos novas cotas para Institucional! Pegando Dados...")
        flagINST=True
        
    elif banco_date == date_INST and status_INST == 'P':
        print("Relatório Completo para INST ainda tem que ser mandado. Checando cotas presentes...")
        flagINST=True
        
    else:
        print("E-mail para Institucional já foi enviado! Relatório nao será gerado")
    
    # Quant
    if  banco_date > date_QUANT:
        print("Temos novas cotas para Quant! Pegando Dados...")
        flagQUANT=True
        
    elif banco_date == date_QUANT and status_QUANT == 'P':
        print("Relatório Completo para QUANT ainda tem que ser mandado. Checando cotas presentes...")
        flagQUANT=True
        
    else:
        print("E-mail para Quant já foi enviado! Relatório nao será gerado")
    
    # Long Short
    if  banco_date > date_LS:
        print("Temos novas cotas para Long Short! Pegando Dados...")
        flagLS=True
        
    elif banco_date == date_LS and status_LS == 'P':
        print("Relatório Completo para LS ainda tem que ser mandado. Checando cotas presentes...")
        flagLS=True
        
    else:
        print("E-mail para Long Short já foi enviado! Relatório nao será gerado")
    
    
    # Check geral
    if flagLO == False and flagLB == False and flagRF == False and flagINST == False and flagQUANT == False and flagLS == False:
        print("\nE-mail ja foi mandado para todas as categorias de relatório. Abandonando funçaõ...")
        return
    
    #DATA READ (Excel) >> Planilha com feriados nacionais
    df = pd.read_excel("C:\Users\Thiago\feriados_nacionais.xlsx")

    #Create list of holidays
    holiday_list = list(df['Data'])

    #Find today's date
    td = pd.Timestamp(y,m,d)
    #1 Dia
    dia_date = td - pd.offsets.CustomBusinessDay(1, holidays=holiday_list)
    #5 Dias Uteis
    dia5_date = td - pd.offsets.CustomBusinessDay(5, holidays=holiday_list)
    #10 Dias Uteis 
    dia10_date = td - pd.offsets.CustomBusinessDay(10, holidays=holiday_list)
    #44 Dias Uteis
    dia44_date = td - pd.offsets.CustomBusinessDay(44, holidays=holiday_list)
    #No mes
    mes_date = td - relativedelta(days=td.day-1)
    #No ano
    ano_date = td - relativedelta(months=td.month-1, days=td.day-1)
    #12 meses
    mes_12_date = td - relativedelta(months=12)
    #24 meses
    mes_24_date = td - relativedelta(months=24)
    #36 meses
    mes_36_date = td - relativedelta(months=36)
    #48 meses
    mes_48_date = td - relativedelta(months=48)
    #60 meses
    mes_60_date = td - relativedelta(months=60)
    #Começo Fund1 LB
    start_lb_date = pd.Timestamp(2022,3,31)
    #Dia especifico do mercado
    special_date = pd.Timestamp(2022,2,11)
    #Semestre
    semestre_date = pd.Timestamp(y,6,30)
    #Começo Fund1 Institucional
    start_inst_date = pd.Timestamp(2009,11,25)
    #Comeco Fund1 FIRF
    start_rf_date = pd.Timestamp(2016,8,23)

    #OFFSETS   

    bday=BDay()
    dia_offset = 1
    dia5_offset = 5
    dia10_offset = 10
    dia44_offset = 44
    mes_offset = len(pd.bdate_range(start=mes_date, end=td, holidays=holiday_list, freq='C'))
    ano_offset = len(pd.bdate_range(start=ano_date, end=td, holidays=holiday_list, freq='C'))
    mes_12_offset = len(pd.bdate_range(start=mes_12_date, end=td, holidays=holiday_list, freq='C'))
    mes_24_offset = len(pd.bdate_range(start=mes_24_date, end=td, holidays=holiday_list, freq='C'))
    mes_36_offset = len(pd.bdate_range(start=mes_36_date, end=td, holidays=holiday_list, freq='C'))
    mes_48_offset = len(pd.bdate_range(start=mes_48_date, end=td, holidays=holiday_list, freq='C'))
    mes_60_offset = len(pd.bdate_range(start=mes_60_date, end=td, holidays=holiday_list, freq='C'))
    start_lb_offset = len(pd.bdate_range(start=start_lb_date, end=td, holidays=holiday_list, freq='C'))-1
    special_offset = len(pd.bdate_range(start=special_date, end=td, holidays=holiday_list, freq='C'))-1
    semestre_offset = len(pd.bdate_range(start=semestre_date, end=td, holidays=holiday_list, freq='C'))-1
    start_inst_offset = len(pd.bdate_range(start=start_inst_date, end=td, holidays=holiday_list, freq='C'))-1
    start_rf_offset = len(pd.bdate_range(start=start_rf_date, end=td, holidays=holiday_list, freq='C'))-1

    contas = [dia_offset, mes_offset, semestre_offset, ano_offset, dia5_offset, dia10_offset, dia44_offset, mes_12_offset, mes_24_offset, mes_36_offset, mes_48_offset, mes_60_offset, start_inst_offset, start_lb_offset, special_offset, start_rf_offset]
    col_new = ["Dia", "Mês", "Semestre", "Ano", "5D", "10D", "44D", "12 Meses", "24 Meses", "36 Meses", "48 Meses", "60 Meses", "ITD Institucional", "ITD Long Bias", "11/02/2022", "ITD FIRF"]
    
    col_drop_LO = ["11/02/2022", "ITD FIRF", "Vol 30D"]
    col_drop_LB = ["ITD Institucional", "ITD FIRF", "Vol 30D"]
    col_drop_RF = ["5D", "10D", "44D", "ITD Institucional", "ITD Long Bias", "11/02/2022", "Vol 30D"]
    col_drop_INST = ["11/02/2022", "ITD FIRF", "ITD Long Bias", "Vol 30D"]
    col_drop_QUANT = ["11/02/2022", "ITD FIRF", "ITD Long Bias", "ITD Institucional", "Vol 30D"]
    col_drop_LS = ["11/02/2022", "ITD FIRF", "ITD Long Bias", "ITD Institucional"]
    
    if m < 8:
        contas.remove(semestre_offset)
        col_new.remove("Semestre")
        
    if dia_offset == mes_offset:
        contas.remove(mes_offset)
        col_new.remove("Mês")
    
    fund1_ref = [td - pd.offsets.CustomBusinessDay(t, holidays=holiday_list) for t in contas]
    dict_offset_colName = dict(zip(contas, col_new))

    #DATA READ (Excel) >> Fund1 database 'Cotas'
    new_df = pd.DataFrame()

    #GET COTAS FROM DATABASE
    path = r'Carteiras.xlsm'
    with open(path, "rb") as f:
        file_io_obj = io.BytesIO(f.read())
    fund1_dia = pd.read_excel(file_io_obj, engine='openpyxl', sheet_name='Hist Dia')
    fund1_dia.set_index(fund1_dia.columns[0], inplace=True)
    fund1_nomes = list(fund1_dia.iloc[:0])
    #Clean fund1_nomes by removing columns with no name
    fund1_nomes = [k for k in fund1_nomes if "unnamed" not in k.casefold()]

    iteration_count = 0
    total_iterations = len(contas)
    
    list_df = []
    
    for j, offset in enumerate(contas):
        config2 = "[{medida: Nome, alias: QA},{medida: Cota/Preço de Fechamento, alias: QB, ativos: [], casasDecimais: 2, diasAjuste: -"+ str(offset) +" dias, periodicidade: '0', periodo: no dia}]"
        values2 = ["RELATORIO", "NORMAL", output, config2, True, True]
        data_dict2 = dict(zip(keys,values2))

        response2 = session.post(url, headers=headers, json=data_dict2)
        json_data2 = json.loads(response2.text)

        # Extract relevant data from JSON
        header_row2 = str(json_data2['valores'][0][1])
        date2 = header_row2[header_row2.find('('):]

        # Create a DataFrame from the data
        df = pd.DataFrame(columns=["Fundo",date1,date2,"Retorno"])

        for i in range(1, len(json_data1['valores'])):
            data_rows = json_data1['valores'][i]
            data_rows2 = json_data2['valores'][i]
            name = data_rows[0]
            if data_rows[1] == '': value1 = np.nan
            else: value1 = float(data_rows[1])
            if data_rows2[1] == '': value2 = np.nan
            else: value2 = float(data_rows2[1])
            mat = ((value1 / value2) - 1)*100
            mat = round(mat,2)
            #str("{:.2f}".format()) + "%"
            df.loc[i-1] = [name, value1, value2, mat]
        
        #Adicionando dados FUND1
        #off by (len(json_data1['valores'])-1)
        for k, txt in enumerate(fund1_nomes):
            name = txt
            if type(fund1_dia.loc[td, txt]) != float and type(fund1_dia.loc[td, txt]) != int or fund1_dia.loc[td, txt] == 0: value1 = np.nan
            else: value1 = float(fund1_dia.loc[td, txt])
            if type(fund1_dia.loc[fund1_ref[j], txt]) != float and type(fund1_dia.loc[fund1_ref[j], txt]) != int or fund1_dia.loc[fund1_ref[j], txt] == 0: value2 = np.nan
            else: value2 = float(fund1_dia.loc[fund1_ref[j], txt])
            mat = ((value1 / value2) - 1)*100
            mat = round(mat,2)
            df.loc[k+(len(json_data1['valores'])-1)] = [name, value1, value2, mat]
            #printrow = [name, value1, value2, mat]
            #print(printrow)


        if j == 0: new_df = df[["Fundo"]].copy()
        
        list_df.append(df)
        new_df[col_new[j]] = df[["Retorno"]].copy()
        
        iteration_count+=1
        print_progress_bar(iteration_count, total_iterations, prefix='Creating dataframe:', suffix='Complete', length=50)
    
    
    # Dataframe para Volatilidade
    df_VOLA = pd.DataFrame(columns=["Fundo","Vol 30D"])

    for i in range(1, len(json_data1_VOLA['valores'])):
                data_rows_VOLA = json_data1_VOLA['valores'][i]
                name = data_rows_VOLA[0]
                if data_rows_VOLA[1] == '': value1 = np.nan
                else: value1 = float(data_rows_VOLA[1])
                mat = value1*100
                mat = round(mat,2)
                df_VOLA.loc[i-1] = [name, mat]
                
        
    new_df["Vol 30D"] = df_VOLA["Vol 30D"].copy()
    
    fund1_vol = [td - pd.offsets.CustomBusinessDay(t, holidays=holiday_list) for t in range(0,32)]

    df_VOLA = pd.DataFrame(columns=["Fundo","Vol 30D"])
    for k, txt in enumerate(fund1_nomes):
        std=0
        temp_list = []
        df_temp = pd.DataFrame(columns=[txt, "Valor"])
        for d in fund1_vol:
            if type(fund1_dia.loc[d, txt]) != float and type(fund1_dia.loc[d, txt]) != int or fund1_dia.loc[d, txt] == 0: value1 = np.nan
            else: value1 = float(fund1_dia.loc[d, txt])
            temp_list.append(value1)
        df_temp['Valor'] = temp_list
        df_temp['PCT'] = df_temp['Valor'].pct_change()
        df_temp.drop(index=0, inplace=True)
        try:
            std = np.std(list(df_temp['PCT'].values))
        except Exception as e:
            std=0
        std = round(std * math.sqrt(252),4)*100
        df_VOLA.loc[k] = [txt, std]

    new_df.loc[len(json_data1['valores'])-1:, 'Vol 30D'] = df_VOLA["Vol 30D"].values
    
    print("\nDados extraidos do Quantum Axis para dia: ",date1)
    
    #Pretty cool bar tool: .bar(subset=col_list, color=['lightsalmon','lightgreen'], align='zero')\ (POOR support from email html styling)

    def style_df(df_copy, i):
        col_list = list(df_copy.columns[1:])
        sort = col_list.pop(i)

        # Calculate the positions of the rows where to add the border
        separation_rows=[]
        num_rows = len(df_copy.index)
        separation_rows = [num_rows // 4, num_rows // 2, (3 * num_rows) // 4]
        
        grey_rows = ['ibovespa', 'ibx', 'ima-g', 'cdi']

        x = df_copy.style\
        .hide(axis="index")\
        .format({i:'{0:,.2f}%' for i in list(new_df.columns[1:])}, na_rep='')\
        .apply(lambda x: ['background:lightgoldenrodyellow' if 'fund1' in str(x.casefold()) else 'background: aliceblue' for x in df_copy['Fundo']], axis=0)\
        .apply(lambda x: ['background:gainsboro' if any(i in str(x.casefold()) for i in grey_rows) else None  for x in df_copy['Fundo']], axis=0)\
        .apply(lambda x: ['border-bottom: 1px solid black' if x in separation_rows else '' for x in range(0, num_rows)], axis=0)\
        .set_properties(subset=[sort], **{'font-weight': 'bold'})\
        .set_properties(subset=df_copy.columns[1:], **{'width': '80px'})\
        .set_properties(subset=df_copy.columns[0], **{'width': '200px'})\
        .set_table_styles({
               'Fundo': [{
                   'selector': 'th',
                   'props': [
                       ('background-color', '#dac174'),
                       ('color', 'white'),
                       ('text-align', 'left')]
               },
                   {
                   'selector': 'td',
                   'props': [
                       ('text-align', 'left')]
               }]},overwrite=False)\
        .set_table_styles(
               [{
                   'selector': 'th',
                   'props': [
                       ('background-color', '#dac174'),
                       ('color', 'white'),
                       ('text-align', 'center')]
               },
                {
                   'selector': 'td',
                   'props': [
                       ('text-align', 'center')]
               }],overwrite=False)
        #.set_table_styles(
            #[{"selector": "", "props": [("border", "1px solid grey")]},
              #{"selector": "tbody td", "props": [("border", "1px solid grey")]},
             #{"selector": "th", "props": [("border", "1px solid grey")]}
            #], overwrite=False
        #)
        return x

    full = pd.ExcelFile("C:\Users\Thiago\dict_nomes_quantum.xlsx")
    LO = pd.read_excel(full, 'Planilha2').astype(str)

    ddf = pd.read_excel(full, 'Planilha1')
    dict_ddf = ddf.set_index('Nome Quantum').to_dict()['Nome Relatório']
    new_df.replace({'Fundo':dict_ddf},inplace=True)
    
    relatorios = ["Long Only","Long Bias","Renda Fixa","Institucional","Quant","Long Short"]
    
    formatted_date = td.strftime("%d/%m/%Y")
    
    for k in relatorios:
        new_df1 = pd.DataFrame()
        #Long Only Filtro
        if k == "Long Only":
            if flagLO==False: continue
            mask = new_df['Fundo'].isin(LO.Long_Only)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_LO)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            
            if cotas < 50:
                print("Cotas presentes LO: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório LO...")
                continue
            elif status_LO == 'P' and banco_date == date_LO:
                if cotas < 80:
                    print("Cotas presentes LO: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LO"]["status"] = "C"
                    
            elif banco_date > date_LO:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LO"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["LO"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["LO"]["status"])
            print("------ Gerando Relatório Long Only ------ Cotas presentes: "+str(cotas)+"%")

        #Long Bias Filtro
        if k == "Long Bias":
            if flagLB==False: continue
            mask = new_df['Fundo'].isin(LO.Long_Bias)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_LB)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            if cotas < 50:
                print("Cotas presentes LB: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório LB...")
                continue
            elif status_LB == 'P' and banco_date == date_LB:
                if cotas < 80:
                    print("Cotas presentes LB: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LB"]["status"] = "C"
                    
            elif banco_date > date_LB:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LB"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["LB"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["LB"]["status"])
            print("------ Gerando Relatório Long Bias ------ Cotas presentes: "+str(cotas)+"%")
            
        #Renda Fixa Filtro
        if k == "Renda Fixa":
            if flagRF==False: continue
            mask = new_df['Fundo'].isin(LO.Renda_Fixa)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_RF)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            if cotas < 50:
                print("Cotas presentes RF: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório RF...")
                continue
            elif status_RF == 'P' and banco_date == date_RF:
                if cotas < 80:
                    print("Cotas presentes RF: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["RF"]["status"] = "C"
                    
            elif banco_date > date_RF:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["RF"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["RF"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["RF"]["status"])
            print("------ Gerando Relatório Renda Fixa ------ Cotas presentes: "+str(cotas)+"%")
            
        #Institucional Filtro
        if k == "Institucional":
            if flagINST==False: continue
            mask = new_df['Fundo'].isin(LO.Institucional)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_INST)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            
            if cotas < 50:
                print("Cotas presentes INST: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório INST...")
                continue
            elif status_INST == 'P' and banco_date == date_INST:
                if cotas < 80:
                    print("Cotas presentes INST: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["INST"]["status"] = "C"
                    
            elif banco_date > date_INST:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["INST"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["INST"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["INST"]["status"])
            print("------ Gerando Relatório Institucional ------ Cotas presentes: "+str(cotas)+"%")
            
        #Quant Filtro
        if k == "Quant":
            if flagQUANT==False: continue
            mask = new_df['Fundo'].isin(LO.Quant)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_QUANT)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            if cotas < 50:
                print("Cotas presentes QUANT: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório QUANT...")
                continue
            elif status_QUANT == 'P' and banco_date == date_QUANT:
                if cotas < 80:
                    print("Cotas presentes QUANT: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["QUANT"]["status"] = "C"
                    
            elif banco_date > date_QUANT:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["QUANT"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["QUANT"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["QUANT"]["status"])
            print("------ Gerando Relatório Quant ------ Cotas presentes: "+str(cotas)+"%")
            
        #Long Short Filtro
        if k == "Long Short":
            if flagLS==False: continue
            mask = new_df['Fundo'].isin(LO.Long_Short)
            new_df1 = new_df[mask]
            new_df1 = new_df1.drop(columns=col_drop_LS)
            #Check para volume de cotas
            num1= new_df1.iloc[:,1].isna().sum()
            denom1= len(new_df1.iloc[:,1])
            cotas = (100 - round(100* (num1/denom1),2))
            if cotas < 50:
                print("Cotas presentes LS: ", cotas, " Dados insuficientes para mandar o Relatório")
                print("Abandonando relatório LS...")
                continue
            elif status_LS == 'P' and banco_date == date_LS:
                if cotas < 80:
                    print("Cotas presentes LS: ", cotas, " Dados insuficientes para mandar o Relatório Completo")
                    continue
                else:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LS"]["status"] = "C"
                    
            elif banco_date > date_LS:
                if cotas > 80:
                    SUBJECT = 'Ranking {} - {}'.format(k,formatted_date)
                    info["LS"]["status"] = "C"
                else:
                    SUBJECT = '[Parcial] Ranking {} - {}'.format(k,formatted_date)
                    info["LS"]["status"] = "P"
                    
            print("P = Parcial, C = Completo. Relatório estilo:", info["LS"]["status"])
            print("------ Gerando Relatório Long Short ------ Cotas presentes: "+str(cotas)+"%")

            
        list_sorted = [new_df1.sort_values(by=col, ascending=False).dropna(subset=new_df1.columns[i+1]) for i, col in enumerate(list(new_df1.columns)[1:])]

        # Generate HTML tables for each sorted dataframe
        html_tables = []


        for j, sorted_df in enumerate(list_sorted):

            # Create a title for the table
            title = f"Por {sorted_df.columns[j+1]}"

            # Apply styling to the sorted dataframe
            styled_df = style_df(sorted_df,j)

            # Generate the HTML table from the styled dataframe
            html_table = styled_df.to_html()

            # Add the title and HTML table to the list
            html_tables.append((title, html_table))
        
        print('Styled & Converted Table to HTML')

        # Create the HTML code
        html_code = ""
        for title, table in html_tables:
            html_code += f"<h2>{title}</h2>\n{table}\n<br>\n"

        print("Inclining CSS styles... (Isso pode demorar um pouco)")
        # Inline the CSS styles in the HTML code
        html_code_inline_css = transform(html_code)
        print("Finished inclining")

        # Save the HTML code to a file
        #with open('tables.html', 'w') as f:
            #f.write(html_code_inline_css)

        #print("tables.html created")

        SENDER_EMAIL = '[username]@gmail.com'
        SENDER_PASSWORD = '[password]'
        SERVER = 'smtp.gmail.com:587'
        RECEIVER_EMAIL = 'reciever@gmail.com'

        HTML = """\
        <html>
          <head></head>
          <body>
            {0}
          </body>
        </html>
        """.format(html_code_inline_css)


        def _generate_message() -> MIMEMultipart:
            message = MIMEMultipart("alternative")
            message['Subject'] = SUBJECT
            message['From'] = SENDER_EMAIL
            message['To'] = RECEIVER_EMAIL
            html_part = MIMEText(HTML, 'html')
            message.attach(html_part)
            return message


        def send_message():
            message = _generate_message()
            server = smtplib.SMTP(SERVER)
            server.ehlo()
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, message.as_string())
            server.quit()


        if __name__ == '__main__':
            send_message()
            print("Enviando email para:",RECEIVER_EMAIL, "---> Tipo:",k,"\n")
        #Long Only Update
        if k == "Long Only":
            info["LO"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)

        #Long Bias Update
        if k == "Long Bias":
            info["LB"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)
            
        #Renda Fixa Update
        if k == "Renda Fixa":
            info["RF"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)
                
        #Renda Fixa Update
        if k == "Institucional":
            info["INST"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)
                
        #Renda Fixa Update
        if k == "Quant":
            info["QUANT"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)
                
        #Renda Fixa Update
        if k == "Long Short":
            info["LS"]["date"] = str(banco_date)
            with open("C:\Users\Thiago\json_master.json", 'w') as file:
                json.dump(info, file)

    
    print("Dates used: ")
    print("==========================")
    date_list = [i.columns[2] for i in list_df]
    for i in range(0, len(date_list)):
        print(col_new[i], "-->", date_list[i])
    
    print("-> Volatilidade")
    print("--------------")
    print(json_data1_VOLA["valores"][0][1])
    print("Fund1 --> ", fund1_vol[0].strftime('%Y-%m-%d'), " : ", fund1_vol[-1].strftime('%Y-%m-%d'))
    
    
    return new_df

def output_to_excel(new_df):
    out_path = f"C:\\Users\\Thiago\\fundo_ranking_{formatted_date}.xlsx"
    new_df.to_excel(out_path, index=False)
    print("File created & saved at: " + out_path)
    

new_df = create_dataframe_Quantum()
