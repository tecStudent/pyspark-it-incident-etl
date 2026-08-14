# PySpark IT Incident ETL

[![PySpark Tests](https://github.com/tecStudent/pyspark-it-incident-etl/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/tecStudent/pyspark-it-incident-etl/actions/workflows/ci.yml)

Pipeline local de Engenharia de Dados desenvolvido com **PySpark**, **Apache Spark 4.1.2** e **Docker** para transformar dados de incidentes de TI em indicadores operacionais auditáveis.

O projeto utiliza um dataset acadêmico do Enterprise Challenge da FIAP, no contexto do desafio com a Locaweb, e foi evoluído como projeto de portfólio. A solução demonstra:

- carga completa em camadas Raw, Bronze, Silver e Gold;
- carga incremental idempotente por lotes;
- reconciliação automática entre controles e arquivos físicos;
- schema explícito, deduplicação, particionamento e Parquet;
- Data Quality com quarentena;
- controle e auditoria das execuções;
- regras de KPI e OLA calculadas separadamente dos indicadores da origem;
- agregações operacionais e tendências diárias;
- ranking de risco explicável;
- previsão de volume por baseline estatístico;
- recomendações determinísticas com evidências;
- contrato JSON para integração com o dashboard;
- dashboard operacional com cinco áreas analíticas e carregamento sob demanda;
- testes automatizados, cobertura, smoke test end-to-end e CI com GitHub Actions.

A carga completa processa **122.543 incidentes**. Os dados analíticos são publicados em um dashboard web estático, sem expor o Excel ou os arquivos Parquet.

[Acessar o dashboard publicado no GitHub Pages](https://tecstudent.github.io/pyspark-it-incident-etl/)

## Visão geral da arquitetura

~~~mermaid
flowchart TD
    A["Excel ou lotes CSV"] --> B["Raw e Landing"]
    B --> C["Bronze: schema e metadados"]
    C --> D["Silver: limpeza, regras e qualidade"]
    D --> E["Gold: KPIs, risco e previsão"]
    D --> Q["Quarentena"]
    E --> J["Contratos JSON"]
    J --> W["Dashboard Web"]
    C --> O["Controles e auditoria"]
    D --> O
    E --> O
~~~

Existem dois modos de execução que reutilizam as mesmas transformações e regras de negócio:

| Modo | Entrada | Comportamento |
| --- | --- | --- |
| Carga completa | Excel convertido para CSV | Reconstrói as camadas usando escrita overwrite |
| Carga incremental | Arquivos CSV na Landing | Processa apenas lotes ainda não aplicados |

## Camadas de dados

### Raw

- Origem em arquivo XLSX.
- Leitura em modo read-only para reduzir o consumo de memória.
- Conversão para CSV preservando os dados de origem.
- Dataset completo mantido apenas localmente e ignorado pelo Git.

### Bronze

- Leitura distribuída com PySpark.
- Schema explícito com os 19 campos de origem.
- Preservação inicial dos valores como StringType.
- Metadados de arquivo e horário de ingestão.
- Validação da quantidade de registros entre origem e destino.
- Persistência em Apache Parquet.

### Silver

- Padronização dos nomes das colunas.
- Tratamento de strings vazias e valores nulos.
- Conversão de timestamps, números e booleanos.
- Separação do código e da descrição da prioridade.
- Regras estruturais de Data Quality.
- Deduplicação utilizando Window e row_number.
- Particionamento físico por ano e mês de abertura.
- Colunas calculadas para auditoria de KPI e OLA.
- Persistência em Apache Parquet.

### Gold

A Gold mantém as agregações históricas do dashboard e acrescenta produtos analíticos operacionais.

| Tabela | Finalidade |
| --- | --- |
| monthly_kpis | Volume mensal, origem, KPI, duração média e P95 |
| priority_summary | Indicadores agregados por prioridade |
| team_summary | Volume e KPI por equipe |
| dashboard_summary | Visão multidimensional por período, prioridade e equipe |
| daily_trends | Tendências diárias por prioridade, produto, categoria e equipe |
| operational_kpi_summary | Comparação entre os indicadores da origem e as regras calculadas |
| annual_ola_summary | Compliance e atingimento anual por prioridade |
| risk_summary | Ranking de risco por prioridade, produto, categoria e equipe |
| forecast_history | Janela histórica utilizada pela previsão |
| forecast_summary | Previsão explicável para D+1 até D+7 |
| recommendations | Recomendações determinísticas com severidade e evidências |

As agregações utilizam groupBy, agg, agregações condicionais, avg, percentile_approx e funções Window.

## Regras auditáveis de KPI e OLA

Os campos fornecidos pela origem são preservados:

- Entrou para KPI?;
- KPI Violado?.

O pipeline também calcula indicadores independentes a partir das regras documentadas. Isso permite comparar a classificação recebida com a classificação reproduzida pelo processamento, sem sobrescrever a fonte.

A Silver registra, entre outras colunas:

- elegibilidade calculada;
- violação calculada;
- justificativa da regra aplicada;
- correspondência entre origem e cálculo;
- versão das regras.

As regras e suas premissas estão documentadas em [docs/kpi-business-rules.md](docs/kpi-business-rules.md).

## Indicadores operacionais

A camada operacional mede:

- volume tratado;
- incidentes elegíveis para KPI;
- incidentes efetivamente avaliáveis;
- violações e incidentes conformes;
- compliance calculado;
- divergências entre origem e regra;
- duração média e P95;
- atingimento anual das metas de volume e violações.

Os indicadores recebidos e calculados permanecem separados para manter a rastreabilidade.

## Ranking de risco operacional

O ranking consolida quatro dimensões: prioridade, produto, categoria e equipe responsável.

O score varia de 0 a 100 e utiliza pesos versionados:

| Componente | Peso |
| --- | ---: |
| Volume normalizado | 45% |
| Taxa de violação normalizada | 35% |
| Duração média normalizada | 20% |

Cada resultado contém metodologia, versão, componentes normalizados, score e posição no ranking. Valores não informados são identificados por is_unknown e não recebem posição operacional.

O score é uma priorização explicável baseada nas métricas do projeto. Ele não representa probabilidade estatística de ocorrência.

## Previsão explicável de volume

A previsão considera os incidentes válidos de prioridade P1, P2 e P3.

A metodologia utiliza uma baseline ponderada:

- 60% da média histórica do mesmo dia da semana;
- 40% da média dos sete dias recentes;
- janela histórica de 28 dias;
- horizonte de sete dias;
- intervalo inferior e superior baseado na variabilidade histórica.

São gerados previsão D+1, total previsto para D+7, projeção diária, intervalo de risco e o histórico usado no cálculo.

Essa previsão é deliberadamente simples e auditável. Ela não deve ser apresentada como um modelo de inteligência artificial ou machine learning validado.

## Recomendações operacionais

As recomendações são derivadas de regras determinísticas e versionadas. Cada item possui identificador estável, regra, versão, dimensão, alvo, severidade, ação sugerida, evidência e métrica de origem.

| Regra | Gatilho |
| --- | --- |
| Risco elevado | Score de risco a partir de 50 |
| Violação elevada | Taxa a partir de 20%, com amostra mínima |
| Concentração de volume | Maior volume por dimensão acima do limite |
| Compliance anual baixo | Compliance calculado abaixo de 90% |
| Crescimento previsto | D+7 pelo menos 10% acima dos sete dias recentes |

As recomendações apoiam a priorização e não executam ações automáticas.

## Arquitetura incremental

~~~mermaid
flowchart TD
    A["CSV na Landing"] --> B["Bronze incremental"]
    B --> C["Silver: merge e Data Quality"]
    C --> D["Gold: snapshot completo"]
    C --> Q["Quarentena"]
    B --> R["Controles por lote"]
    C --> R
    D --> R
    R --> U["Auditoria da execução"]
~~~

### Bronze incremental

- Calcula o hash SHA-256 de cada arquivo.
- Gera um batch_id determinístico.
- Registra arquivo, hash, quantidade e status.
- Ignora arquivos já processados com sucesso.
- Mantém os lotes Bronze em diretórios Parquet independentes.

### Silver incremental

- Reutiliza as transformações da Silver completa.
- Combina novos lotes com o estado existente.
- Deduplica pelo identificador do incidente e mantém a versão mais recente.
- Separa registros válidos e inválidos.
- Preserva a quarentena existente.
- Substitui as saídas com staging e backup.

### Gold incremental

- Executa apenas quando existem lotes Silver pendentes.
- Recalcula os snapshots a partir do estado Silver atual.
- Produz as mesmas famílias de indicadores operacionais da Gold completa.
- Registra os batches já refletidos na camada.

### Controles e idempotência

Os controles locais ficam em data/control:

| Controle | Finalidade |
| --- | --- |
| processed_batches.json | Lotes processados na Bronze |
| silver_batches.json | Lotes incorporados à Silver |
| gold_batches.json | Lotes refletidos nos snapshots Gold |
| pipeline_runs.json | Histórico das execuções do runner |

Uma reexecução sem novos arquivos não duplica registros nem reaplica lotes concluídos.

### Auditoria

Cada execução incremental registra identificador, início, término, duração, status, resultado por etapa, possível falha e snapshot dos controles Bronze, Silver, Gold e quarentena.

## Data Quality e quarentena

A Silver verifica:

- presença do identificador do incidente;
- faixa válida de prioridade;
- presença da equipe responsável;
- timestamps de abertura e encerramento;
- duração nula ou negativa;
- encerramento anterior à abertura.

Registros inválidos do fluxo incremental são preservados em data/quarantine/incidents com os motivos encontrados em dq_issues.

## Contrato de dados do dashboard

O dashboard não lê Parquet, o Excel bruto ou regras PySpark. A integração ocorre por JSONs gerados automaticamente em docs/data.

### Arquivos preservados para compatibilidade

| Arquivo | Conteúdo |
| --- | --- |
| dashboard_summary.json | Visão geral usada pelo dashboard atual |
| monthly_kpis.json | Série mensal |
| priority_summary.json | Resumo por prioridade |
| team_summary.json | Resumo por equipe |

### Contratos operacionais versão 1.0

| Arquivo | Conteúdo |
| --- | --- |
| filter_options.json | Anos, meses, prioridades, produtos, categorias e equipes |
| daily_trends_index.json | Catálogo, integridade e recorte padrão das tendências |
| daily_trends/AAAA/MM.json | Indicadores diários multidimensionais particionados |
| risk_summary.json | Metodologia e ranking de risco |
| forecast_summary.json | Histórico, escopo e previsão D+1/D+7 |
| recommendations.json | Recomendações com evidências |
| manifest.json | Integridade, atualização, volume e hash dos payloads |

Os contratos contêm schema_version, generated_at em UTC, mock: false, datas ISO 8601 e valores JSON válidos sem NaN ou Infinity. JSON Schemas Draft 2020-12 e o manifesto versionado protegem os campos, tipos e a integridade dos payloads.

A especificação completa está em [docs/dashboard-data-contract.md](docs/dashboard-data-contract.md). Os exemplos para o front-end permanecem em docs/data/samples com mock: true.

A exportação operacional é consumida pela interface sem reproduzir regras de negócio em JavaScript.

Para migrar uma publicação antiga que ainda contém o arquivo único `daily_trends.json`:

~~~bash
docker compose run --rm spark \
  python3 src/dashboard_trend_partitions.py
~~~

O comando cria as partições mensais, publica `daily_trends_index.json`, remove o arquivo monolítico e regenera o manifesto. As próximas execuções de `src/export_dashboard.py` já produzem diretamente o formato particionado.

## Dashboard

O dashboard estático foi desenvolvido com HTML, CSS, JavaScript e Chart.js e está publicado gratuitamente no GitHub Pages.

A interface preserva o estilo visual do projeto e organiza a análise em cinco áreas:

| Área | Conteúdo |
| --- | --- |
| Visão geral | Volume, KPI, violações, compliance, evolução mensal, prioridades e equipes |
| Tendências | Evolução diária e semanal, duração média e ranking por dimensão |
| Previsão | Baseline explicável D+1/D+7, faixa de risco e projeção diária |
| Risco operacional | Ranking por prioridade, produto, categoria ou equipe, com metodologia e evidências |
| Recomendações | Ações determinísticas filtráveis por severidade e acompanhadas da evidência de origem |

Os filtros usam as opções publicadas pelo pipeline. Ano, mês, prioridade e equipe afetam a Visão geral; os seis filtros afetam as Tendências. Previsão, risco e recomendações são snapshots com escopo declarado nos respectivos contratos e não são recalculados no navegador.

Antes de renderizar os indicadores, a página verifica se o manifesto está `HEALTHY`, se todos os contratos estão válidos e se a publicação não contém dados simulados. As tendências são particionadas por ano e mês. A aba Tendências abre no recorte mais recente e baixa apenas as partições necessárias, mantendo em memória as partições já consultadas.

### Executar localmente

~~~bash
python -m http.server 8000 --directory docs
~~~

Acesse http://localhost:8000 e encerre com Ctrl + C.

## Resultados de referência

### Carga completa

| Métrica | Resultado |
| --- | ---: |
| Registros processados | 122.543 |
| Agregações mensais | 36 |
| Prioridades | 5 |
| Equipes | 17 |
| Agregações do dashboard | 683 |
| Tendências diárias | 21.064 |
| Itens no ranking de risco | 216 |
| Dias previstos | 7 |
| Recomendações operacionais | 18 |
| Testes automatizados | 148 passed |

Os números representam uma execução local de referência e podem mudar quando as regras ou o dataset forem atualizados.

### Validação incremental

No snapshot mais recente utilizado durante o desenvolvimento:

| Métrica | Resultado |
| --- | ---: |
| Registros válidos na Silver incremental | 55 |
| Registros em quarentena | 1 |
| Recomendações no snapshot incremental | 21 |
| Reexecução sem novos lotes | Nenhum batch reprocessado |

## Tecnologias

- Apache Spark 4.1.2 e PySpark
- Python e OpenJDK 21
- Apache Parquet
- Docker e Docker Compose
- Pytest, pytest-cov e OpenPyXL
- HTML5, CSS3, JavaScript e Chart.js
- Git, GitHub, GitHub Actions e GitHub Pages

## Estrutura principal

~~~text
pyspark-it-incident-etl/
|-- .github/workflows/
|   `-- ci.yml
|-- data/
|   |-- raw/
|   |-- landing/
|   |-- control/
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|   |-- quarantine/
|   `-- sample/
|-- src/
|   |-- extract_xlsx.py
|   |-- bronze.py
|   |-- silver.py
|   |-- kpi_rules.py
|   |-- gold.py
|   |-- operational_gold.py
|   |-- risk_gold.py
|   |-- forecast_gold.py
|   |-- recommendation_gold.py
|   |-- export_dashboard.py
|   |-- dashboard_trend_partitions.py
|   |-- validate_dashboard_contracts.py
|   |-- dashboard_manifest.py
|   |-- e2e_smoke_test.py
|   |-- coverage_report.py
|   |-- pipeline.py
|   |-- create_incremental_batches.py
|   |-- incremental_bronze.py
|   |-- incremental_silver.py
|   |-- incremental_gold.py
|   |-- incremental_pipeline.py
|   |-- pipeline_audit.py
|   `-- pipeline_reconciliation.py
|-- tests/
|   |-- conftest.py
|   |-- test_silver.py
|   |-- test_kpi_rules.py
|   |-- test_operational_gold.py
|   |-- test_risk_gold.py
|   |-- test_forecast_gold.py
|   |-- test_recommendation_gold.py
|   |-- test_dashboard_export.py
|   |-- test_dashboard_json_schemas.py
|   |-- test_dashboard_manifest.py
|   |-- test_dashboard_manifest_quality_gate.py
|   |-- test_e2e_smoke_test.py
|   |-- test_coverage_report.py
|   |-- test_ingestion_helpers.py
|   |-- test_pipeline_audit.py
|   |-- test_pipeline_reconciliation.py
|   `-- test_dashboard_ui.py
|-- docs/
|   |-- dashboard-data-contract.md
|   |-- kpi-business-rules.md
|   |-- index.html
|   |-- css/
|   |-- js/
|   `-- data/
|-- Dockerfile
|-- docker-compose.yml
|-- .coveragerc
|-- requirements.txt
`-- README.md
~~~

Os dados brutos, controles e saídas Parquet permanecem fora do versionamento. Apenas amostras e JSONs agregados destinados ao GitHub Pages são publicados.

## Pré-requisitos

- Git;
- Docker Desktop;
- Docker Compose.

Java, Spark e PySpark não precisam ser instalados no Windows. O container fornece o ambiente completo.

## Preparar o dataset

Coloque LW-DATASET.xlsx em:

~~~text
data/raw/LW-DATASET.xlsx
~~~

O pipeline utiliza a aba Dataset Geral.

## Construir o ambiente

~~~bash
docker compose build
~~~

## Executar a carga completa

~~~bash
docker compose run --rm spark python3 src/pipeline.py
~~~

~~~text
Extract -> Bronze -> Silver -> Gold -> Dashboard
~~~

### Retomar por etapa

~~~bash
# Bronze até Dashboard
docker compose run --rm spark python3 src/pipeline.py --from-stage bronze

# Silver até Dashboard
docker compose run --rm spark python3 src/pipeline.py --from-stage silver

# Gold e exportação JSON
docker compose run --rm spark python3 src/pipeline.py --from-stage gold

# Somente exportação JSON
docker compose run --rm spark python3 src/pipeline.py --from-stage dashboard
~~~

## Executar o pipeline incremental

~~~bash
# Gerar lotes mensais após a extração
docker compose run --rm spark python3 src/create_incremental_batches.py

# Adicionar um lote
cp data/raw/batches/incidents_2023_01.csv data/landing/

# Executar Bronze, Silver, Gold e reconciliação
docker compose run --rm spark python3 src/incremental_pipeline.py
~~~

### Retomar por etapa

~~~bash
docker compose run --rm spark python3 src/incremental_pipeline.py --from-stage silver
docker compose run --rm spark python3 src/incremental_pipeline.py --from-stage gold
docker compose run --rm spark python3 src/incremental_pipeline.py --from-stage reconciliation
~~~

### Conferir a última auditoria

~~~bash
docker compose run --rm spark python3 -c \
'import json; data=json.load(open("data/control/pipeline_runs.json", encoding="utf-8")); print(json.dumps(data["runs"][-1], indent=2, ensure_ascii=False))'
~~~

### Reconciliação automática entre camadas

Depois da Gold, o runner executa uma etapa de reconciliação que compara os controles JSON com as contagens físicas dos Parquets. Ela valida os batches, arquivos de origem, hashes, volumes recebidos, divisão entre válidos e inválidos, duplicidades, snapshot Gold e existência dos produtos analíticos.

Para executar somente essa verificação:

~~~bash
MSYS_NO_PATHCONV=1 docker compose run --rm spark \
  /opt/spark/bin/spark-submit \
  --master "local[2]" \
  src/pipeline_reconciliation.py
~~~

O resultado é registrado em `data/control/reconciliation_runs.json`. Uma divergência retorna exit code diferente de zero e interrompe o pipeline, preservando no relatório quais checks falharam e os valores esperados e encontrados.

## Executar camadas no Git Bash

~~~bash
MSYS_NO_PATHCONV=1 docker compose run --rm spark /opt/spark/bin/spark-submit --master "local[4]" src/bronze.py
MSYS_NO_PATHCONV=1 docker compose run --rm spark /opt/spark/bin/spark-submit --master "local[4]" src/silver.py
MSYS_NO_PATHCONV=1 docker compose run --rm spark /opt/spark/bin/spark-submit --master "local[4]" src/gold.py
~~~

MSYS_NO_PATHCONV=1 evita a conversão automática de caminhos Linux pelo Git Bash.

## Testes automatizados

~~~bash
docker compose run --rm spark python3 -m pytest -q
~~~

Resultado atual:

~~~text
................................................................................. [100%]
148 passed
~~~

Os testes cobrem limpeza, tipagem, Data Quality, deduplicação, KPI, OLA, agregações, risco, previsão, recomendações, contratos JSON, manifesto, particionamento das tendências, controles incrementais, auditoria, reconciliação, integração estática da interface, relatório de cobertura e as validações auxiliares do smoke test.

### Cobertura de testes

O projeto mede linhas e branches do diretório `src`. O quality gate começa em 50% para registrar uma baseline reproduzível e impedir regressões enquanto a cobertura é ampliada progressivamente.

~~~bash
docker compose run --rm spark python3 -m pytest \
  -q \
  -p no:cacheprovider \
  --cov=src \
  --cov-branch \
  --cov-config=.coveragerc \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-report=json:coverage.json \
  --cov-report=html:htmlcov \
  --cov-fail-under=50
~~~

Os arquivos `coverage.json`, `coverage.xml`, `.coverage` e o diretório `htmlcov` são resultados locais e permanecem fora do versionamento.

Para gerar um resumo Markdown a partir do relatório JSON:

~~~bash
docker compose run --rm spark python3 src/coverage_report.py \
  coverage.json \
  --minimum 50 \
  --output coverage-summary.md \
  --check
~~~

No GitHub Actions, o mesmo resumo aparece na página da execução. O runner prepara o diretório gravável `coverage-artifacts` para que o usuário não privilegiado do container possa salvar o banco temporário e os relatórios no volume Linux. O relatório HTML completo fica disponível por 14 dias no artefato `coverage-report`.

### Smoke test end-to-end

O smoke test utiliza a pequena amostra versionada em data/sample, grava todas as saídas em um diretório temporário e valida o fluxo integrado CSV -> Bronze -> Silver -> Gold.

~~~bash
MSYS_NO_PATHCONV=1 docker compose run --rm spark \
  /opt/spark/bin/spark-submit \
  --master "local[2]" \
  src/e2e_smoke_test.py
~~~

Ele verifica as contagens entre camadas, Data Quality com quarentena, deduplicação pela versão mais recente, agregações Gold e idempotência por meio de duas execuções consecutivas. As saídas temporárias são removidas ao final; use --keep-output para preservá-las durante uma inspeção local.

## Integração contínua

O GitHub Actions executa em pushes para main, Pull Requests direcionados à main e acionamentos manuais.

O CI:

- utiliza actions/checkout@v5;
- constrói a imagem Docker;
- executa os 148 testes com cobertura de linhas e branches;
- reprova quando a cobertura total fica abaixo de 50%;
- publica o resumo da cobertura na página da execução;
- disponibiliza JSON, XML e HTML no artefato coverage-report por 14 dias;
- prepara um diretório isolado e gravável para os relatórios no runner Linux;
- desabilita o cache do Pytest;
- possui permissão somente de leitura;
- cancela execuções anteriores do mesmo contexto;
- aplica timeout de 20 minutos;
- verifica se hashes, tamanhos, contagens e metadados do manifesto correspondem aos payloads versionados.
- executa o smoke test integrado com uma amostra reduzida, sem depender do dataset acadêmico completo.

## Como interromper

Durante uma execução, pressione Ctrl + C. Containers iniciados com docker compose run --rm são removidos após o encerramento.

Para serviços iniciados com docker compose up:

~~~bash
docker compose down
~~~

## Decisões técnicas

| Decisão | Motivo |
| --- | --- |
| Docker para Spark e Java | Execução reproduzível sem instalar Java no Windows |
| Parquet entre as camadas | Tipagem, compressão e leitura eficiente |
| Schema explícito | Evitar inferência inconsistente |
| Regras calculadas separadas da origem | Preservar auditabilidade |
| Hash SHA-256 por lote | Garantir idempotência |
| Reconciliação após a Gold | Detectar divergências entre controles e dados físicos |
| JSON agregado no GitHub Pages | Publicação gratuita sem expor dados brutos |
| Manifesto com hash normalizado | Verificação reproduzível no Windows e no Linux |
| Tendências particionadas por mês | Reduzir o download inicial e permitir cache no navegador |
| Cobertura mínima no CI | Impedir regressões de testes com uma baseline mensurável |
| Baseline explicável | Manter a previsão transparente |
| Recomendações determinísticas | Permitir testes, versão e rastreabilidade |

## Limitações conhecidas

- O dataset é acadêmico e não representa um ambiente produtivo em tempo real.
- Os controles JSON não implementam locking distribuído.
- A carga incremental usa snapshots Parquet, não um formato transacional.
- A previsão é uma baseline, não um modelo treinado e validado.
- As recomendações são regras de apoio à decisão.

## Conceitos demonstrados

- ETL em arquitetura medalhão;
- schema explícito e PySpark DataFrame API;
- Parquet, particionamento e funções Window;
- Data Quality, quarentena e deduplicação;
- agregações condicionais e percentil P95;
- carga incremental, idempotência e merge de estado;
- controles por camada e auditoria;
- regras de negócio versionadas;
- data marts operacionais;
- ranking e previsão explicáveis;
- recomendações baseadas em regras;
- contrato de dados, JSON Schema e manifesto de integridade;
- testes automatizados, cobertura, CI/CD e GitHub Pages.

## Possíveis evoluções

- Adicionar métricas Web Vitals ao dashboard publicado.
- Ampliar progressivamente o limite mínimo de cobertura.
- Orquestrar o pipeline com Apache Airflow.
- Evoluir o armazenamento para Iceberg ou Delta Lake.
- Publicar a imagem no GitHub Container Registry.
- Substituir controles locais por uma camada transacional.

## Contexto acadêmico

Este projeto reutiliza o contexto acadêmico do Enterprise Challenge da FIAP para construir uma solução local de Engenharia de Dados voltada ao portfólio técnico.

O dataset completo, os controles e as saídas intermediárias permanecem fora do versionamento. O GitHub Pages publica somente dados analíticos agregados.
