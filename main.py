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
    Retornar uma tupla contendo todos os valores para cada chave
    """  
    coduf = elemento['coduf']
    return (coduf, elemento)

def casos_covid(elemento):
    """
    Recebe uma tupla (coduf, [{}, {}, {}...])
    Retornar uma tupla contendo regiao, estado, coduf, totalCasos, totalObitos
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
    return (regiao, estado, coduf, totalCasos, totalObitos)

def lista_para_tupla(elemento):
    """
    Receber lista 
    Retornar tupla com colunas filtradas
    """
    return (elemento[0], elemento[1], elemento[3])

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
    | "Mostrar resultados" >> beam.Map(print)   
)

#pcollection gerado a partir do pipeline que trata os dados do arquivo EstadosIBGE.csv
estados = (
    pipeline
    | "Leitura do dataset de informações dos estados" >> 
        ReadFromText('EstadosIBGE.csv', skip_header_lines=1)
    | "Transforma texto para lista" >> beam.Map(texto_para_lista)
    | "Lista para tupla" >> beam.Map(lista_para_tupla)
    | "Exibe resultados" >> beam.Map(print)   
)

pipeline.run()

