# Desafio-Hurb
## Descrição do Projeto
<p align="left">Script de um pipeline de dados desenvolvido em Python utilizando Apache Beam. Este script permite ao usuário obter um arquivo csv e um json a partir de dois arquivos de dados csv. Cada um desses dois arquivos csv é passado por um pipeline de dados onde os mesmos são tratados individualmente. Ao final, os dois são agrupados e passam por um novo pipeline, sendo tratados para gerar os arquivos finais de acordo com a formatação solicitada.</p>
 
## Como Rodar o Script
<p align="left">É necessário a instalação do Python e do framework Apache Beam. O projeto foi desenvolvido utilizando a versão 3.8.6 do Python e a versão 2.41.0 do Apache Beam. Para executar o script deve-se executar "python main.py" no terminal da sua IDE ou localizar no terminal da sua máquina o path onde o projeto foi salvo e executar "python main.py" </p>
 
## ETAPAS

 
 <h3 align="left"><li><b>
	Pipeline para tratamento dos dados do arquivo "HIST_PAINEL_COVIDBR_28set2020.csv"</li></b></h3>

<p align="left">
 A variável "covid" é uma pcollection que armazena o resultado do pipeline onde os dados da tabela 
 "HIST_PAINEL_COVIDBR_28set2020.csv" são carregados e tratados.

 Primeiramente cada linha do arquivo csv 
 é passado como string para a função "texto_para_lista". Essa função retorna uma lista onde cada elemento é uma substring que foi separada pelo delimitador ";". 
 
 Posteriormente, a função lista_para_dicionario recebe esta lista e outra lista contendo o cabeçalho do arquivo csv e converte as duas em um dicionário onde as chaves são os elementos da lista de cabeçalho e os valores são os elementos da outra. 
 
 A função "chave_coduf" recebe este dicionário, descompacta o valor da chave "coduf" e retorna uma tupla contendo este valor e o dicionário. 
 
 Em seguida, o método "GroupByKey" do Apache Beam recebe esta coleção de elementos com a chave "coduf" e gera uma coleção onde cada elemento consiste de uma chave e uma lista de dicionários associados a esta chave.

 A função "casos_covid" recebe esta coleção, descompacta a chave "coduf" e a lista de dicionários. Percorre através de um "for" cada dicionário e calcula o somatório do número de casos de covid e de óbitos. A saída é uma tupla contendo como primeiro elemento o código "coduf" e como segundo elemento um novo dicionário contendo UF, regiao, total de casos e total de óbitos.</p>

<h3 align="left"><li><b>
	Pipeline para tratamento dos dados do arquivo "EstadosIBGE.csv"</li></b></h3>

<p align="left">
 A variável "estados" é uma pcollection que armazena o resultado do pipeline onde os dados da tabela "EstadosIBGE.cvs" serão carregados e tratados. 
 
 Primeiramente cada linha do arquivo csv será passado como string para a função "texto_para_lista". Essa função retornará uma lista onde cada elemento é uma substring que foi separada pelo delimitador ";".
 
  A função lista_para_tupla
 recebe esta lista e retorna um tupla contendo o código "coduf" e um dicionário de informações do nome do estado e do governador.
 </p>

<h3 align="left"><li><b>
	Pipeline para tratamento da pcollection resultado"</li></b></h3>
<p align="left">    
 A variável "resultado" é uma pcollection que recebe as pcollections "covid" e "estados". Estas passam por um pipeline onde são mescladas e tratadas para estarem em conformidade com o padrão do arquivo final desejado.
 
  O método "Flatten" do Apache Beam é chamado para fazer o merge das duas pcollections. O resultado disso é o empilhamento das mesmas. 
  
  Em seguinda, o método "GroupByKey" é chamado para agrupar todos elementos que possuem em comum a chave "coduf". Este método então vai agrupar os dicionários dos elementos que possuem em comum o código "coduf" como primeiro elemento da tupla, em uma lista. O retorno será uma tupla contendo o código "coduf" e a lista do agrupamento dos dicionários. 
  
  Após isso, o método "Filter" do apache Beam é chamado para filtrar os elementos que deverão permanecer ou não na pcollection. Para isso foi usado a função "remove_regiao" para remover o elemento condizente a região denominada "Brasil", uma vez que este não é de interesse do projeto. 
  
  Posteriormente, a função "organiza_dados" é chamada para descompatar as informações de região, estado, UF, governador, total de casos de covid e de óbitos e retornar uma tupla com estes dados.
  
   O método "prepara_csv" é chamado para fazer a conversão da tupla em uma string onde os elementos são concatenados e delimitados por ";".
  </p> 

<h3 align="left"><li><b>
	Geração do arquivo1 em formato csv"</li></b></h3>
<p align="left">  
 O resultado da pcollection anterior está pronto para gerar o arquivo1.csv. Este resultado será passado por um pipeline onde um objeto da classe "WriteToText" será instanciado passando como argumentos o nome do aquivo a ser gerado, o sufixo ".csv" e a string contendo os nomes das colunas que formarão o header do arquivo. O arquivo então é gerado quando o objeto "pipeline" é rodado.
 </p>

<h3 align="left"><li><b>
	Geração do arquivo2 em formato json"</li></b></h3>
<p align="left">  
 A função "csv_para_json" é chamada passando como argumentos o arquivo csv gerado e um arquivo json vazio. Essa função sobrescreve no arquivo json onde cada coluna do arquivo csv é uma key dentro deste json.

## Importância da utilização do Apache Beam
<p align="left">O Apache Beam é um modelo de programação unificado de código aberto para definir e executar pipelines de processamento de dados, como por exemplo, o ETL, que é a extração, a transformação e o carregamento de dados.

A SDK do Apache Beam permite realizar processamento tanto em lote, conhecido como batch, ou seja, grande quantidade de arquivos brutos que nós vamos processar linha a linha, em partes, como também os famosos streams, ou então processamentos em fluxo. Mas durante o nosso processo, foi utilizado apenas o processamento em batch. 

Essa unificação de processamento em batch ou stream, além de características como execução de pipelines em múltiplos ambientes de desenvolvimento (portabilidade) e suporte de ferramentas desenvolvidas pelo usuário e comunidade (extensível) são os principais motivos para o uso desse framework.
</p>



