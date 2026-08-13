# Contrato de Dados do Dashboard

Versão do contrato: `1.0`

## Objetivo

Este documento define a interface entre o pipeline PySpark e o dashboard web. O pipeline é responsável por produzir arquivos JSON válidos; o dashboard deve apenas consumir esses arquivos, aplicar os filtros permitidos e apresentar os resultados.

Essa separação permite desenvolver o processamento e a visualização em branches diferentes.

## Responsabilidades

| Componente | Responsabilidade | Diretórios principais |
| --- | --- | --- |
| Pipeline | Calcular métricas, previsões, riscos e recomendações | `src/`, `tests/`, `data/gold/` |
| Exportação | Converter as tabelas Gold para o contrato JSON | `src/export_dashboard.py` |
| Dashboard | Consumir os JSONs sem recalcular regras de negócio | `docs/index.html`, `docs/css/`, `docs/js/` |

O dashboard não deve ler arquivos Parquet, CSV bruto ou reproduzir regras de KPI em JavaScript.

## Arquivos

| Arquivo de produção | Finalidade |
| --- | --- |
| `docs/data/dashboard_summary.json` | Visão geral já utilizada pelo dashboard atual |
| `docs/data/filter_options.json` | Valores disponíveis nos filtros |
| `docs/data/daily_trends.json` | Indicadores diários para tendências |
| `docs/data/forecast_summary.json` | Histórico e baseline de previsão D+1/D+7 |
| `docs/data/risk_summary.json` | Rankings de risco operacional |
| `docs/data/recommendations.json` | Recomendações determinísticas com evidências |
| `docs/data/manifest.json` | Integridade, atualização e volume dos payloads publicados |

Durante o desenvolvimento, os exemplos ficam em `docs/data/samples/`. Todo exemplo possui `"mock": true` e não deve ser apresentado como resultado real.

## Convenções gerais

- Os nomes dos campos usam `snake_case`.
- Datas usam ISO 8601: `YYYY-MM-DD`.
- Timestamps usam ISO 8601 com timezone UTC.
- Durações são armazenadas em segundos.
- Percentuais usam escala de `0` a `100`.
- Scores de risco usam escala de `0` a `100`.
- Valores desconhecidos são representados por `null`.
- Não são permitidos `NaN`, `Infinity` ou strings vazias no lugar de valores nulos.
- Todo novo arquivo contém `schema_version`, `generated_at` e `mock`.
- Alterações incompatíveis exigem uma nova versão principal do contrato.

## Compatibilidade

O formato atual de `dashboard_summary.json` deve ser preservado durante a primeira evolução. Novos campos podem ser acrescentados, mas campos existentes não devem ser removidos ou renomeados sem atualizar o dashboard e a versão do contrato.

## Escopo dos filtros

| Módulo | Filtros da versão 1 |
| --- | --- |
| Visão geral | Ano, mês, prioridade e equipe |
| Tendências | Período, prioridade, equipe, produto e categoria |
| Previsão | Escopo fixo declarado no próprio arquivo |
| Risco | Snapshot consolidado por dimensão |
| Recomendações | Snapshot consolidado com evidências |

A previsão não deve ser recalculada no navegador. Se futuramente houver previsão por produto, prioridade ou equipe, o pipeline deverá gerar explicitamente esses novos escopos.

## `filter_options.json`

Objeto contendo as opções aceitas pelo dashboard:

- `years`: lista de anos inteiros;
- `months`: objetos com número e nome do mês;
- `priorities`: objetos com código e nome;
- `products`: lista de produtos;
- `categories`: lista de categorias;
- `teams`: lista de equipes.

## `daily_trends.json`

Cada item de `records` representa uma agregação diária por prioridade, produto, categoria e equipe.

Campos obrigatórios:

| Campo | Tipo |
| --- | --- |
| `date` | string ISO date |
| `priority_code` | integer |
| `priority_name` | string |
| `product` | string ou null |
| `category` | string ou null |
| `assigned_group` | string ou null |
| `total_incidents` | integer |
| `kpi_incidents` | integer |
| `kpi_violations` | integer |
| `avg_duration_seconds` | number ou null |
| `p95_duration_seconds` | number ou null |

## `forecast_summary.json`

O campo `method` identifica o cálculo como baseline explicável. Ele não deve ser apresentado como modelo de inteligência artificial ou machine learning validado.

Campos principais:

- `scope`: universo de dados usado na previsão;
- `forecast_d1`: volume previsto para o próximo dia;
- `forecast_d7`: soma prevista para os próximos sete dias;
- `risk_range`: margem usada no intervalo inferior e superior;
- `history`: valores reais recentes;
- `forecast`: projeção diária.

## `risk_summary.json`

O campo `methodology` documenta os pesos do score. Cada item contém:

- tipo e valor da dimensão;
- volume;
- taxa de violação;
- duração média;
- score de risco;
- posição no ranking.

Valores nulos ou identificados como não informados devem ser sinalizados com `is_unknown: true`, permitindo que o dashboard os separe dos rankings operacionais.

## `recommendations.json`

Cada recomendação precisa ser rastreável e conter:

- identificador;
- tipo e alvo;
- severidade;
- título;
- recomendação;
- evidência textual;
- métrica, valor e unidade que originaram a recomendação.

## JSON Schemas e validação automática

Os cinco payloads operacionais e o manifesto de publicação possuem definições formais em JSON Schema Draft 2020-12:

| Payload | Schema |
| --- | --- |
| `filter_options.json` | `docs/schemas/filter_options.schema.json` |
| `daily_trends.json` | `docs/schemas/daily_trends.schema.json` |
| `risk_summary.json` | `docs/schemas/risk_summary.schema.json` |
| `forecast_summary.json` | `docs/schemas/forecast_summary.schema.json` |
| `recommendations.json` | `docs/schemas/recommendations.schema.json` |
| `manifest.json` | `docs/schemas/manifest.schema.json` |

Os schemas validam campos obrigatórios, tipos, datas e timestamps ISO 8601, valores nulos permitidos e a ausência de propriedades não documentadas.

Para validar os arquivos reais:

```bash
docker compose run --rm spark python3 src/validate_dashboard_contracts.py
```

Para validar os exemplos usados pelo front-end:

```bash
docker compose run --rm spark python3 src/validate_dashboard_contracts.py \\
  --data-dir docs/data/samples
```

A etapa `src/export_dashboard.py` executa essa validação depois de gerar os cinco arquivos. Uma quebra de contrato encerra a execução com erro antes da publicação. Os mesmos contratos também são verificados pelos testes automatizados no GitHub Actions.

## Manifesto de publicação

O arquivo `docs/data/manifest.json` é gerado somente depois que os cinco payloads passam pelos seus contratos. Ele permite que o dashboard e processos de publicação verifiquem:

- horário de geração do manifesto e de cada payload;
- versão do contrato e origem real ou simulada;
- status de validação;
- quantidade de itens publicada;
- tamanho do arquivo em bytes;
- hash SHA-256 para verificação de integridade;
- quantidade total de arquivos válidos.

O manifesto recebe `status: HEALTHY` somente quando todos os arquivos existem e são válidos. Um arquivo ausente, JSON inválido ou quebra de schema interrompe a geração.

Para regenerar apenas o manifesto, sem executar o Spark:

```bash
docker compose run --rm spark python3 src/dashboard_manifest.py
```

Para verificar se o manifesto versionado ainda corresponde
aos payloads publicados, sem alterar arquivos:

```bash
docker compose run --rm spark python3 \
  src/dashboard_manifest.py --check
```

O quality gate compara versão, status, quantidade de arquivos,
origem real ou simulada e, para cada payload, data dos dados,
contagem de itens, tamanho normalizado e hash SHA-256. O cálculo
normaliza apenas as quebras de linha, garantindo o mesmo resultado
no Windows e no Linux.

O GitHub Actions executa essa verificação após os testes. Se um
payload for modificado sem a regeneração do manifesto, o Pull
Request falha com a lista dos campos desatualizados.

Para gerar um manifesto dos exemplos:

```bash
docker compose run --rm spark python3 src/dashboard_manifest.py \\
  --data-dir docs/data/samples \\
  --output /tmp/sample-manifest.json
```

## Integração da interface

O dashboard publicado em `docs/` consome os contratos sem
reimplementar as regras de KPI, OLA, risco, previsão ou recomendação.
A navegação está organizada em Visão geral, Tendências, Previsão,
Risco operacional e Recomendações.

Na inicialização, a interface lê primeiro o manifesto e interrompe a
renderização quando o status não é `HEALTHY`, quando existe contrato
inválido ou quando a publicação contém dados simulados. Os payloads de
visão geral, filtros, risco, previsão e recomendações são carregados em
seguida. Por ser o maior arquivo, `daily_trends.json` é carregado sob
demanda quando a aba Tendências é aberta.

Ano, mês, prioridade e equipe filtram a Visão geral. Tendências utiliza
também produto e categoria. Os demais módulos respeitam o escopo fixo
documentado nos próprios snapshots, evitando apresentar um filtro como
aplicado quando não existe agregação correspondente no pipeline.

## Critérios de aceite da integração

1. Os cinco payloads e o manifesto são gerados sem edição manual.
2. Todos os arquivos são JSON válidos e seguem `schema_version: 1.0`.
3. O dashboard funciona com os arquivos simulados e com os arquivos reais.
4. Nenhuma regra de negócio é duplicada em JavaScript.
5. O dashboard trata arquivos vazios, valores nulos e erros de carregamento.
6. Os testes automatizados validam pelo menos os campos obrigatórios e os tipos principais.
7. O CI rejeita payloads que não correspondem ao manifesto versionado.
