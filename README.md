# PySpark IT Incident ETL
Projeto de engenharia de dados desenvolvido localmente com PySpark e Docker, a partir de dados acadêmicos do Enterprise Challenge da FIAP no contexto do projeto com a Locaweb.
O objetivo é construir um pipeline ETL reproduzível para tratamento e análise de dados de incidentes de TI, aplicando práticas comuns de Engenharia de Dados com Apache Spark.
> Status: em desenvolvimento. Neste momento, o ambiente Docker/PySpark já está configurado e validado. As camadas do ETL serão implementadas nas próximas etapas.
## Objetivos do projeto
Executar Apache Spark localmente sem depender da instalação de Java ou PySpark no Windows.
Construir as etapas de extração, transformação e carga utilizando PySpark.
Trabalhar com schema explícito, tratamento de dados e validações de qualidade.
Organizar os dados em uma arquitetura de processamento em camadas.
Gerar arquivos otimizados para análise nas etapas finais do pipeline.
Adicionar testes automatizados às principais transformações.
## Arquitetura planejada
```text
Dataset acadêmico (.xlsx)
        |
        v
      Raw
        |
        v
     Bronze
        |
        v
     Silver
        |
        v
      Gold
```
As responsabilidades de cada camada serão definidas durante a implementação do ETL. A arquitetura acima representa o fluxo planejado e não significa que todas as camadas já estejam implementadas.
## Tecnologias
Apache Spark 4.1.2
PySpark
OpenJDK 21 (dentro da imagem Docker)
Docker
Docker Compose
Python
Git e GitHub
## Estrutura atual
```text
pyspark-it-incident-etl/
|-- data/
|   `-- sample/
|-- src/
|   `-- test_spark.py
|-- tests/
|-- .gitignore
|-- Dockerfile
|-- docker-compose.yml
|-- README.md
`-- requirements.txt
```
## Pré-requisitos
Para executar o projeto localmente é necessário ter:
Git
Docker Desktop
Docker Compose
Não é necessário instalar Java, Spark ou PySpark diretamente no Windows. Essas dependências são executadas dentro do container Docker.
## Como executar
### 1. Iniciar o Docker Desktop
Antes de executar os comandos, confirme que o Docker Desktop está em execução.
### 2. Construir a imagem
Na raiz do projeto:
```bash
docker compose build
```
O build normalmente só precisa ser repetido quando o `Dockerfile` ou alguma dependência da imagem for alterada.
### 3. Validar o ambiente PySpark
No Git Bash do Windows:
```bash
MSYS_NO_PATHCONV=1 docker compose run --rm spark /opt/spark/bin/spark-submit src/test_spark.py
```
O teste cria uma `SparkSession`, executa um pequeno DataFrame e exibe a versão do Spark. A execução esperada utiliza Spark 4.1.2.
## Como interromper uma execução
Se um job Spark estiver rodando no terminal e você quiser interrompê-lo, pressione:
```text
Ctrl + C
```
Os jobs deste projeto são executados com `docker compose run --rm`. Quando o processo termina, o container temporário é removido automaticamente.
Se em algum momento um serviço tiver sido iniciado com `docker compose up`, ele pode ser encerrado com:
```bash
docker compose down
```
Esse comando encerra os containers e a rede do Compose. Ele não apaga os arquivos do código-fonte armazenados na pasta do projeto.
## Como continuar o projeto depois
Não é necessário deixar o Spark ou o container executando entre uma sessão de desenvolvimento e outra.
Para continuar em outro momento:
Abra o Docker Desktop.
Abra o terminal na pasta `pyspark-it-incident-etl`.
Atualize o repositório, se necessário:
```bash
git pull
```
Execute novamente o job desejado com `docker compose run --rm`.
Para repetir o teste atual:
```bash
MSYS_NO_PATHCONV=1 docker compose run --rm spark /opt/spark/bin/spark-submit src/test_spark.py
```
O código e os arquivos do projeto permanecem no computador porque a pasta local é montada no container em `/app`. Por isso, encerrar o container não apaga o desenvolvimento realizado.
## Comandos úteis
Verificar a configuração do Compose:
```bash
docker compose config
```
Reconstruir a imagem:
```bash
docker compose build
```
Verificar containers do projeto:
```bash
docker compose ps
```
Encerrar serviços iniciados pelo Compose:
```bash
docker compose down
```
## Próximas etapas
Analisar e preparar o dataset de origem.
Implementar a ingestão dos dados.
Criar a camada Bronze.
Implementar limpeza, tipagem, deduplicação e regras de qualidade na Silver.
Criar agregações e indicadores na Gold.
Persistir resultados em formato Parquet.
Adicionar particionamento onde fizer sentido.
Criar testes automatizados.
Documentar as decisões técnicas e resultados do pipeline.
## Contexto
Este repositório é um projeto de portfólio focado na prática de Engenharia de Dados com PySpark. Ele reaproveita o contexto acadêmico do Enterprise Challenge da FIAP como fonte para a construção de um pipeline ETL executável localmente.