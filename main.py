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
    d.pop('municipio'), d.pop('codmun'), d.pop('codRegiaoSaude'), d.pop('nomeRegiaoSaude'), d.pop('data'), d.pop('semanaEpi'), d.pop('populacaoTCU2019'), d.pop('casosNovos'), d.pop('obitosNovos'), d.pop('Recuperadosnovos'), d.pop('emAcompanhamentoNovos'), d.pop('interior/metropolitana')
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
    regiao = elemento['regiao']
    estado = elemento['estado']
    coduf = elemento['coduf']
    casosAcumulado = elemento['casosAcumulado']
    obitosAcumulado = elemento['obitosAcumulado']
    return (regiao, estado, coduf, casosAcumulado, obitosAcumulado)

'''def casos_covid(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retornar uma tupla ('RS-2014-12', 8.0)
    """
    coduf, registros = elemento
    for registro in registros:
        yield (f"{coduf}", registro['casos'])
'''

covid = (
    pipeline
    | "Leitura do dataset de casos de covid" >> 
        ReadFromText('HIST_PAINEL_COVIDBR_28set2020.csv', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_covid)
    #| "Criar campo ano_mes" >>beam.Map(trata_data) 
    | "Criar chave pelo código da UF" >> beam.Map(chave_coduf)
    #| "Agrupar pelo estado" >> beam.GroupByKey()
    #| "Descompactar casos de covid" >> beam.FlatMap(casos_covid)
    #| "Soma dos casos de infecão e óbito pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
    
)

pipeline.run()

