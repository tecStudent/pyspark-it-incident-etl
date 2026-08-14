# Benchmark de desempenho do pipeline

Este relatório compara duas configurações Spark sobre a mesma carga sintética e determinística. O objetivo é medir antes de alterar a configuração de produção.

## Escopo

- Registros de entrada: 50.000
- Execuções medidas por perfil: 3
- Aquecimentos por perfil: 1
- Spark: 4.1.2
- Master: `local[2]`

## Resultado consolidado

| Perfil | Mediana | P95 | Throughput mediano |
| --- | ---: | ---: | ---: |
| Baseline | 106.22s | 106.28s | 447 reg/s |
| Otimizado | 20.86s | 22.60s | 2.277 reg/s |

## Comparação por etapa

| Etapa | Baseline | Otimizado |
| --- | ---: | ---: |
| `core_gold` | 16.12s | 2.19s |
| `operational_gold` | 82.69s | 14.53s |
| `silver_transform_and_dedup` | 5.76s | 4.23s |

## Decisão

- Equivalência dos resultados: **MATCH**
- Variação da mediana: **80.36%**
- Speedup: **5.093x**
- Recomendação: **adotar o perfil otimizado**

## Configurações comparadas

### Baseline

- `spark.sql.adaptive.enabled=false`
- `spark.sql.shuffle.partitions=200`

### Otimizado

- `spark.sql.adaptive.enabled=true`
- `spark.sql.adaptive.coalescePartitions.enabled=true`
- `spark.sql.adaptive.localShuffleReader.enabled=true`
- `spark.sql.shuffle.partitions=8`

## Limitações

A carga é sintética, não contém dados acadêmicos e serve para comparação controlada. Tempos absolutos dependem de CPU, memória, Docker e processos concorrentes.
Uma configuração só deve ser aplicada ao pipeline principal quando mantiver os resultados equivalentes e apresentar ganho repetível no mesmo ambiente.
