import apache_beam as beam
from apache_beam.io import ReadFromText
#importando opções de pipeline para o projeto 
from apache_beam.options.pipeline_options import PipelineOptions 
#criação de instância de objeto de opção de pipeline 
pipeline_options = PipelineOptions(argv=None)
#criação de instância do pipeline recebendo como parâmetro as opções de pipeline
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

'''def organizando_dados_resultado(elemento):
    """
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
    return (regiao, estado, uf, governador, totalCasos, totalObitos)'''

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
    #| "Tratando e organizando dados para saída final" >> beam.Map(organizando_dados_resultado) 
    | "Mostrar resultados da união dos pcollection" >> beam.Map(print)
)

pipeline.run()

