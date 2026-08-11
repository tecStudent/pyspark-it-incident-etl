# Regras de Negócio de KPI e OLA

Versão das regras: `1.0`

## Objetivo

Este documento registra as regras que serão utilizadas para auditar os indicadores de KPI do dataset de incidentes. Os campos calculados pelo pipeline serão mantidos separados dos indicadores recebidos na origem.

Fonte de referência: `Dicionário de Dados - v2`, fornecido no Enterprise Challenge FIAP/Locaweb.

## Indicadores existentes na origem

| Campo Silver | Campo de origem | Finalidade |
| --- | --- | --- |
| `entered_kpi` | `Entrou para KPI?` | Indicador informado pela origem sobre a participação do incidente no KPI |
| `kpi_violated` | `KPI Violado?` | Indicador informado pela origem sobre violação do limite |

Esses campos são evidências da fonte e não devem ser sobrescritos pelas regras calculadas.

## Prioridades consideradas no KPI

Somente as prioridades 1, 2 e 3 entram no universo de KPI.

| Código | Prioridade | Limite documentado | Limite em segundos | Entra no KPI |
| ---: | --- | ---: | ---: | --- |
| 1 | Crítica | 4 horas | 14.400 | Sim |
| 2 | Alta | 4 horas | 14.400 | Sim |
| 3 | Média | 12 horas | 43.200 | Sim |
| 4 | Baixa | 24 horas | 86.400 | Não |
| 5 | Muito Baixa | 96 horas | 345.600 | Não |

Os limites de P4 e P5 permanecem documentados para análise operacional, mas essas prioridades não entram no KPI conforme a regra fornecida.

## Regras de exclusão

Um incidente não entra no KPI quando pelo menos uma destas condições for atendida:

1. prioridade diferente de 1, 2 ou 3;
2. campo `parent_incident_id` preenchido;
3. campo `status` igual a `Sem Intervenção`.

A comparação do status poderá normalizar espaços, letras maiúsculas e acentuação, mas o valor original deverá ser preservado.

## Regra de elegibilidade

Representação lógica:

```text
kpi_eligible_by_rule =
    priority_code IN (1, 2, 3)
    AND parent_incident_id IS NULL
    AND normalized_status != "SEM INTERVENCAO"
```

Valores vazios do campo `Incidente Pai` são convertidos para `null` durante o tratamento da Silver.

## Regra de violação

Para um incidente elegível:

```text
kpi_violated_by_rule =
    duration_seconds > kpi_limit_seconds
```

Consequências:

- duração exatamente igual ao limite não representa violação;
- incidente não elegível recebe `null` em `kpi_violated_by_rule`, pois a regra não se aplica;
- duração ausente recebe `null`, pois não é possível determinar a violação;
- o indicador original `kpi_violated` continua preservado.

## Motivos de avaliação

O campo `kpi_rule_reason` utilizará valores controlados:

| Valor | Significado |
| --- | --- |
| `eligible` | Incidente elegível e passível de avaliação |
| `priority_not_eligible` | Prioridade fora do universo P1, P2 e P3 |
| `parent_incident` | Registro relacionado a um incidente pai |
| `status_without_intervention` | Status igual a Sem Intervenção |
| `missing_duration` | Elegível, mas sem duração para avaliar a violação |
| `invalid_priority` | Prioridade ausente ou inválida |

Quando mais de uma exclusão existir, será utilizada esta precedência:

```text
invalid_priority
priority_not_eligible
parent_incident
status_without_intervention
missing_duration
eligible
```

## Campos calculados previstos para a Silver

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `kpi_rule_version` | string | Versão das regras aplicadas |
| `kpi_limit_seconds` | long | Limite correspondente à prioridade |
| `kpi_eligible_by_rule` | boolean | Elegibilidade calculada |
| `kpi_violated_by_rule` | boolean | Violação calculada ou `null` quando não aplicável |
| `kpi_rule_reason` | string | Motivo da decisão |
| `entered_kpi_rule_matches_source` | boolean | Comparação entre a elegibilidade calculada e a origem |
| `kpi_violated_rule_matches_source` | boolean | Comparação entre a violação calculada e a origem |

Os campos de comparação recebem `null` quando algum dos lados não puder ser avaliado.

## Metas anuais — incidentes com OLA quebrado

O documento apresenta metas anuais para P2 e P3, embora o indicador seja medido mensalmente.

### Prioridade 2 — Alta

| Quantidade anual | Atingimento |
| ---: | ---: |
| Menor que 31 | 150% |
| 31 a 35 | 125% |
| 36 a 39 | 100% |
| 40 a 45 | 75% |
| 46 a 53 | 50% |
| Maior que 53 | 0% |

### Prioridade 3 — Média

| Quantidade anual | Atingimento |
| ---: | ---: |
| Menor que 201 | 150% |
| 201 a 230 | 125% |
| 231 a 263 | 100% |
| 264 a 290 | 75% |
| 291 a 320 | 50% |
| Maior que 320 | 0% |

## Metas anuais — volume total tratado

### Prioridade 2 — Alta

| Quantidade anual | Atingimento |
| ---: | ---: |
| Menor que 4.585 | 150% |
| 4.585 a 5.388 | 125% |
| 5.389 a 6.168 | 100% |
| 6.169 a 6.252 | 75% |
| 6.253 a 6.336 | 50% |
| Maior que 6.336 | 0% |

### Prioridade 3 — Média

| Quantidade anual | Atingimento |
| ---: | ---: |
| Menor que 19.489 | 150% |
| 19.489 a 22.116 | 125% |
| 22.117 a 22.524 | 100% |
| 22.525 a 23.892 | 75% |
| 23.893 a 24.276 | 50% |
| Maior que 24.276 | 0% |

As metas anuais serão implementadas em uma agregação Gold posterior. Elas não fazem parte da transformação de elegibilidade da Silver.

## Correção em relação ao protótipo anterior

O protótipo Streamlit utilizava limites padrão diferentes do documento oficial:

| Prioridade | Protótipo anterior | Documento oficial |
| --- | ---: | ---: |
| P2 — Alta | 8 horas | 4 horas |
| P3 — Média | 24 horas | 12 horas |

Os limites do protótipo não devem ser reutilizados. A implementação PySpark deverá seguir os valores oficiais registrados neste documento.

## Critérios de aceite

1. Os indicadores da origem permanecem inalterados.
2. As novas regras são implementadas em função PySpark isolada e testável.
3. A fronteira do limite usa comparação estritamente maior (`>`).
4. Incidentes não elegíveis não são contados como violações calculadas.
5. Registros não avaliáveis recebem `null`, sem conversão silenciosa para `false`.
6. Os testes cobrem todas as prioridades, exclusões, limites e divergências com a origem.
7. As regras completas e incrementais utilizam a mesma função.

