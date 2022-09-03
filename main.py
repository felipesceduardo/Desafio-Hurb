"""Pipeline de dados utilzando Apache Beam

Este script permite ao usuário obter um arquivo csv e um json a partir
de dois arquivos de dados csv.
Cada um desses dois arquivos csv é passado por um pipeline de dados 
onde os mesmos são tratados individualmente. Ao final, os dois são agrupados 
e passam por um novo pipepline, sendo tratados para gerar os arquivos finais
de acordo com a formatação solicitada.

O script requer instalação do framework Apache Beam
"""
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
    """Recebe duas listas e retorna um dicionário 

    Parametros
    ----------
    elemento : list
        Lista contendo os elementos de cada coluna do arquivo csv
    colunas : list
        Lista que contém o header do arquivo csv 

    Retorna
    -------
    dict 
        Dicionário onde cada key é o header da coluna e cada value é o valor associado a tal coluna
    """

    d = dict(zip(colunas, elemento))
    d.pop('municipio'), d.pop('codmun'), d.pop('codRegiaoSaude'), d.pop('nomeRegiaoSaude'), d.pop('data'), d.pop('semanaEpi'), d.pop('populacaoTCU2019'), d.pop('casosAcumulado'), d.pop('obitosAcumulado'), d.pop('Recuperadosnovos'), d.pop('emAcompanhamentoNovos'), d.pop('interior/metropolitana')
    return d 

def texto_para_lista(elemento, delimitador=';'):
    """Recebe uma string e um delimitador retorna uma lista 

    Parametros
    ----------
    elemento : str
        String contendo linha do arquivo csv
    delimitador : str, opcional
        Delimitador usado para fazer o split na string

    Retorna
    -------
    Lista
        Lista contendo os elementos de cada coluna do arquivo csv
    """

    return elemento.split(delimitador)

def chave_coduf(elemento): 
    """Recebe um dicionário e retorna uma tupla contendo o códido da UF e o dicionário

    Parametros
    ----------
    elemento : dict
        Dicionário onde cada key é o header da coluna e cada value é o valor associado a tal coluna

    Retorna
    -------
    Tupla
        Tupla contendo o código da UF e o dicionário
    """
    coduf = elemento['coduf']
    return (coduf, elemento)

def casos_covid(elemento):
    """Recebe uma tupla e retorna uma tupla contendo o códido da UF e um dicionário

    Parametros
    ----------
    elemento : tupla
        Tupla contendo código "coduf" e uma lista de dicionários onde tem em comum este código 

    Retorna
    -------
    Tupla
        Tupla contendo o código "coduf" e um dicionário contendo informações da região, estado, código da UF, total 
        de casos e total de óbitos
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
    """Recebe uma lista e retorna uma tupla contendo o códido da UF e um dicionário

    Parametros
    ----------
    elemento : lista
        Lista onde cada elemento corresponde a uma célula de uma linha do arquivo "EstadosIBGE.csv" 

    Retorna
    -------
    Tupla
        Tupla contendo o código "coduf" e um dicionário contendo informações do nome do estado e do governador
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
    """Recebe uma tupla e a retorna caso satisfa a condicional 

    Parametros
    ----------
    elemento : tupla
        tupla contendo código "coduf" e uma lista de dicionários

    Retorna
    -------
    Tupla
        tupla contendo código "coduf" e uma lista de dicionários
    """     
    
    coduf, lista_dicionarios = elemento
    if(coduf != '76'):
        return True  
    return False

def organiza_dados(elemento):
    """Recebe uma tupla e a retorna com elementos organizadas conforme desejado pelo projeto

    Parametros
    ----------
    elemento : tupla
        tupla contendo código "coduf" e uma lista de dicionários

    Retorna
    -------
    Tupla
        tupla contendo informações do nome da região, estado, governador, total de casos e de óbitos
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

def prepara_csv(elemento, delimitador=";"):
    """Recebe uma tupla e retorna uma string com os elementos delimitados por ";"

    Parametros
    ----------
    elemento : tupla
        tupla contendo os elementos desejados pelo projeto

    Retorna
    -------
    String
        string formada pela concatenação de cada elemento da tupla delimitados pelo caractere ';'
    """
    
    return f"{delimitador}".join(elemento)    


def csv_para_json(arquivo_csv, arquivo_json):
    """Recebe um arquivo csv e um json vazio e escreve neste último o conteúdo do arquivo csv

    Parametros
    ----------
    arquivo_csv : csv
        arquivo csv contendo o conteúdo do arquivo1 gerado pelo pipeline

    Retorna
    -------
    json
        arquivo json 
    """

    json_array = [] 
    with open(arquivo_csv, encoding='utf-8') as csvf: 
        reader = csv.DictReader(csvf, delimiter=';')  
        for row in reader: 
            json_array.append(row)

    with open(arquivo_json, 'w', encoding='utf-8') as jsonf: 
        json_string = json.dumps(json_array, indent=4)
        jsonf.write(json_string)
    

covid = (
    pipeline
    | "Leitura do dataset de casos de covid" >> 
        ReadFromText('HIST_PAINEL_COVIDBR_28set2020.csv', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_covid)
    | "Criar chave pelo código da UF" >> beam.Map(chave_coduf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de covid" >> beam.Map(casos_covid)
    #| "Mostrar resultados" >> beam.Map(print)   
)


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
    | "Preparar csv" >> beam.Map(prepara_csv)
    #| "Mostrar resultados da união dos pcollection" >> beam.Map(print)
)

header = 'Regiao;Estado;UF;Governador;TotalCasos;TotalObitos'

resultado | "Criar arquivo CSV" >> WriteToText('arquivo1', file_name_suffix='.csv', shard_name_template='',header=header)

pipeline.run()

csvArquivo = r'arquivo1.csv'
jsonArquivo = r'arquivo2.json'
csv_para_json(csvArquivo, jsonArquivo)

