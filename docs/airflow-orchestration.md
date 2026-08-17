# Orquestração local com Apache Airflow

## Objetivo

O Apache Airflow passa a coordenar o pipeline
incremental existente sem duplicar as regras de
transformação. Os scripts PySpark continuam sendo
a fonte de verdade das camadas; a DAG acrescenta
agendamento, dependências, retries, histórico e
logs por etapa.

O ambiente usa Apache Airflow 3.3.1,
`LocalExecutor` e PostgreSQL. Ele foi projetado
para demonstração local e portfólio;
não para produção distribuída.

## Fluxo

```text
check_landing
    -> bronze
    -> silver
    -> gold
    -> reconciliation
    -> execution_summary
```

### check_landing

Inspeciona `data/landing`, conta os arquivos CSV,
calcula o tamanho total e envia somente esses
metadados ao XCom. A ausência de arquivos não é
uma falha, porque uma reexecução sem lote novo é
um cenário válido.

### bronze

Executa `src/incremental_bronze.py` com
`spark-submit`. O controle por SHA-256 impede
que um arquivo já concluído seja processado
novamente.

### silver

Executa `src/incremental_silver.py`. A etapa
transforma os novos batches, aplica deduplicação,
regras auditáveis de KPI e Data Quality, separando
registros inválidos na quarentena.

### gold

Executa `src/incremental_gold.py`. A etapa
atualiza o snapshot analítico e os produtos
operacionais somente com batches ainda não
aplicados.

### reconciliation

Executa `src/pipeline_reconciliation.py`. Os
controles JSON são confrontados com os Parquets e
uma divergência retorna exit code diferente de
zero, fazendo a tarefa e a DAG falharem.

### execution_summary

Consolida os metadados pequenos retornados pelas
etapas, registra o snapshot dos controles e grava
`data/control/airflow_pipeline_runs.json`.
Falhas permanecem registradas no banco e nos logs
do próprio Airflow.

## Arquivos adicionados

| Arquivo | Responsabilidade |
| --- | --- |
| `Dockerfile.airflow` | Runtime com Airflow, Java 17 e PySpark 4.1.2 |
| `requirements-airflow.txt` | Dependências exclusivas do orquestrador |
| `docker-compose.airflow.yml` | PostgreSQL, API/UI, scheduler, DAG processor e inicialização |
| `.env.airflow.example` | Configurações locais documentadas |
| `dags/it_incident_incremental_pipeline.py` | Dependências, retries e tarefas da DAG |
| `src/airflow_orchestration.py` | Comandos Spark, inspeção da landing e auditoria |
| `tests/test_airflow_orchestration.py` | Testes unitários e checks estáticos da integração |

## Decisões técnicas

### Compose separado

O arquivo `docker-compose.yml` existente continua
dedicado aos comandos Spark. O novo
`docker-compose.airflow.yml` pode ser iniciado e
encerrado sem alterar o fluxo anterior.

### Sem Docker socket

O Airflow executa o PySpark no próprio runtime.
O socket `/var/run/docker.sock` não é montado,
evitando acesso privilegiado ao Docker do host e
problemas de conversão de caminhos no Windows.

### Dados fora do XCom

CSV, Parquet e controles permanecem no diretório
`data`, compartilhado pelos serviços. O XCom
recebe apenas nomes, contagens, status, horários e
durações.

### Execução sequencial

As camadas são dependentes e não executam jobs
Spark em paralelo. `max_active_runs=1` também
impede duas execuções da DAG de alterarem os
controles locais simultaneamente.

### Agendamento manual por padrão

A DAG começa sem agenda e com
`catchup=False`. Isso evita processamento
automático durante a instalação. Uma expressão
cron pode ser definida posteriormente em
`AIRFLOW_PIPELINE_SCHEDULE`.

## Primeiro uso

Copie a configuração local:

```bash
cp .env.airflow.example .env.airflow
```

Construa o runtime:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  build
```

Inicialize o banco e o usuário:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  up airflow-init
```

O serviço `airflow-init` deve terminar com exit
code zero. Depois, suba os serviços permanentes:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  up -d
```

Confira os containers:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  ps
```

## Validar a DAG

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  run --rm airflow-api-server \
  airflow dags list-import-errors
```

O comando deve apresentar `No data found` ou
uma lista vazia.

Também é possível confirmar o cadastro:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  run --rm airflow-api-server \
  airflow dags list
```

## Executar pela interface

1. Acesse <http://localhost:8080>.
2. Entre com `airflow` / `airflow` no ambiente
   local.
3. Localize
   `it_incident_incremental_pipeline`.
4. Ative a DAG.
5. Selecione **Trigger DAG**.
6. Abra a visão de grade ou grafo.
7. Clique em uma tarefa para consultar seus logs.

As credenciais do exemplo são exclusivas do
ambiente local e devem ser alteradas se o serviço
for exposto fora da máquina.

## Retry e retomada

Cada tarefa possui uma tentativa adicional após
cinco minutos. Se uma etapa falhar:

1. abra a execução na interface;
2. selecione a tarefa com falha;
3. consulte o log e corrija a causa;
4. use **Clear task** para executá-la novamente.

As etapas concluídas não precisam ser limpas. Os
controles incrementais tornam uma repetição
segura, mas não implementam locking distribuído.

## Habilitar agenda

Edite `.env.airflow`, por exemplo para executar
às 06:00 diariamente:

```dotenv
AIRFLOW_PIPELINE_SCHEDULE=0 6 * * *
```

Depois recrie os serviços:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  up -d --force-recreate
```

## Encerrar

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  down
```

O banco de metadados permanece no volume
`airflow-postgres-db`.

Para apagar também o histórico local do Airflow:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  down --volumes --remove-orphans
```

O último comando é destrutivo para o histórico do
Airflow, mas não remove o diretório `data` do
projeto.

## Troubleshooting

### Porta 8080 em uso

Encerre o processo que utiliza a porta ou altere
`8080:8080` no Compose.

### API ou scheduler não saudável

Reserve pelo menos 4 GB para o Docker Desktop,
consulte os logs e aguarde a migração inicial:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker-compose.airflow.yml \
  logs airflow-api-server airflow-scheduler
```

### DAG não aparece

Execute `airflow dags list-import-errors` e
confirme que `dags/` está montado em
`/opt/airflow/dags`.

### spark-submit não encontrado

Reconstrua a imagem. O
`requirements-airflow.txt` instala PySpark 4.1.2
e o helper também procura o binário incluído no
pacote Python.

## Limitações

- ambiente local de demonstração;
- um único scheduler com `LocalExecutor`;
- PostgreSQL sem alta disponibilidade;
- credenciais locais de exemplo;
- controles JSON sem locking distribuído;
- ausência de autoscaling e execução remota;
- nenhuma garantia de segurança para exposição
  pública.
