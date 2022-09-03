import csv 
import json 
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions 
pipeline_options = PipelineOptions(argv=None)

pipeline = beam.Pipeline(options=pipeline_options)


colunas_covid = [
                'regiao',
                'estado',
                'municipio',
                'coduf',
                'codmun',
                'codRegiaoSaude',
                'nomeRegiaoSaude',
                'data',
                'semanaEpi',
                'populacaoTCU2019',
                'casosAcumulado',
                'casosNovos',
                'obitosAcumulado',
                'obitosNovos',
                'Recuperadosnovos',
                'emAcompanhamentoNovos',
                'interior/metropolitana'
                ]

def lista_para_dicionario(elemento, colunas):
    """
    Recebe duas listas
    Retorna um dicionário 
    """
    d = dict(zip(colunas, elemento))
    d.pop('municipio'), d.pop('codmun'), d.pop('codRegiaoSaude'), d.pop('nomeRegiaoSaude'), d.pop('data'), d.pop('semanaEpi'), d.pop('populacaoTCU2019'), d.pop('casosAcumulado'), d.pop('obitosAcumulado'), d.pop('Recuperadosnovos'), d.pop('emAcompanhamentoNovos'), d.pop('interior/metropolitana')
    return d 

def texto_para_lista(elemento, delimitador=';'):
    """
    Recebe um texto e um delimitador e retorna uma lista de elementos
    pelo delimitador 
    """
    return elemento.split(delimitador)

def chave_coduf(elemento): 
    """
    Receber um dicionário 
    Retornar uma tupla contendo coduf e um dicionário
    """  
    coduf = elemento['coduf']
    return (coduf, elemento)

def casos_covid(elemento):
    """
    Recebe uma tupla (coduf, [{}, {}, {}...])
    Retornar uma tupla contendo o coduf e um dicionário contendo regiao, estado, coduf, totalCasos, totalObitos
    """
    coduf, registros = elemento
    totalCasos = 0
    totalObitos = 0
    primeiro_registro = registros[0]
    regiao = primeiro_registro['regiao']
    estado = primeiro_registro['estado']  

    for registro in registros:
        totalCasos += int(registro['casosNovos'])
        totalObitos += int(registro['obitosNovos'])       
    dicionario = {
        'UF': estado,
        'regiao': regiao,
        'totalCasos': totalCasos,
        'totalObitos': totalObitos
    }
    return (coduf, dicionario)

def lista_para_tupla(elemento):
    """
    Receber lista 
    Retornar tupla com coduf e um dicionário contendo nome do estado e do governador
    """
    nome_estado = elemento[0]
    coduf = elemento[1]
    governador = elemento[3]
    dicionario = {
        'nome_estado': nome_estado,
        'governador': governador
    }
    return (coduf, dicionario)

def remove_regiao(elemento):
    """
    Recebe tupla 
    Remove a região denominada Brasil pelo código UF
    """
    coduf, lista_dicionarios = elemento
    if(coduf == '76'):
        return False  
    return True

def organiza_dados(elemento):
    """
    Recebe uma tupla 
    Retorna uma tupla com colunas organizadas conforme solicitado no projeto
    """
    coduf, lista_dicionarios = elemento
    dic1 = lista_dicionarios[0]
    dic2 = lista_dicionarios[1]
    regiao = dic2['regiao']
    estado = dic1['nome_estado']
    uf = dic2['UF']
    governador = dic1['governador']
    totalCasos = dic2['totalCasos']
    totalObitos = dic2['totalObitos']
    return (regiao, estado, uf, governador, str(totalCasos), str(totalObitos))

def preparar_csv(elemento, delimitador=";"):
    """
    Receber uma tupla do tipo 
    Retornar uma string delimitada por ';'
    """
    return f"{delimitador}".join(elemento)    


def csv_para_json(csvArquivo, jsonArquivo):
    """
    Recebe dois arquivos, um csv e outro json
    Lê o arquivo csv e carrega seus dados usando o leitor de dicionário da biblioteca csv
    Converte cada linha do csv em um dicionário
    Adiciona o dicionário em um array 
    Converte o array em uma string JSON e escreve no arquivo
    """
    json_array = [] 
    with open(csvArquivo, encoding='utf-8') as csvf: 
        reader = csv.DictReader(csvf, delimiter=';')  
        for row in reader: 
            json_array.append(row)

    with open(jsonArquivo, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(json_array, indent=4)
        jsonf.write(jsonString)
    

#pcollection gerado a partir do pipeline que trata os dados do arquivo HIST_PAINEL_COVIDBR_28set2020.csv
covid = (
    pipeline
    | "Leitura do dataset de casos de covid" >> 
        ReadFromText('HIST_PAINEL_COVIDBR_28set2020.csv', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_covid)
    | "Criar chave pelo código da UF" >> beam.Map(chave_coduf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de covid" >> beam.Map(casos_covid)
   # | "Mostrar resultados" >> beam.Map(print)   
)

#pcollection gerado a partir do pipeline que trata os dados do arquivo EstadosIBGE.csv
estados = (
    pipeline
    | "Leitura do dataset de informações dos estados" >> 
        ReadFromText('EstadosIBGE.csv', skip_header_lines=1)
    | "Transforma texto para lista" >> beam.Map(texto_para_lista)
    | "Lista para tupla" >> beam.Map(lista_para_tupla)
   # | "Exibe resultados" >> beam.Map(print)   
)

resultado = (
    (covid, estados)
    | "Empilha as pcollection" >> beam.Flatten()
    | "Agrupa as pcollection" >> beam.GroupByKey()
    | "Remove região denominada Brasil" >> beam.Filter(remove_regiao) 
    | "Organiza os dados" >> beam.Map(organiza_dados) 
    | "Preparar csv" >> beam.Map(preparar_csv)
    #| "Mostrar resultados da união dos pcollection" >> beam.Map(print)
)

header = 'Regiao;Estado;UF;Governador;TotalCasos;TotalObitos'

resultado | "Criar arquivo CSV" >> WriteToText('arquivo1', file_name_suffix='.csv', shard_name_template='',header=header)

pipeline.run()

csvArquivo = r'arquivo1.csv'
jsonArquivo = r'arquivo1.json'
csv_para_json(csvArquivo, jsonArquivo)

