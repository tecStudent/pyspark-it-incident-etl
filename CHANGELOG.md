# Changelog

Todas as mudanças relevantes deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o projeto utiliza [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Adicionado

- Análise estática de segurança do código Python com CodeQL.
- Dependency Review para impedir novas dependências com vulnerabilidades de severidade alta ou crítica.
- Atualizações semanais de pacotes Python e GitHub Actions com Dependabot.
- Política para comunicação privada e tratamento responsável de vulnerabilidades.

### Alterado

- GitHub Actions atualizado para `actions/checkout@v7` nos workflows ativos.

### Planejado

- Ampliação progressiva da cobertura automatizada.
- Avaliação de orquestração com Apache Airflow.
- Avaliação de armazenamento transacional com Iceberg ou Delta Lake.

## [1.0.0] - 2026-08-14

### Adicionado

- Pipeline completo Raw, Bronze, Silver e Gold com PySpark e Docker.
- Pipeline incremental idempotente com controles por lote e retomada por etapa.
- Data Quality, quarentena, deduplicação e reconciliação entre camadas.
- Regras auditáveis de KPI e OLA separadas dos indicadores recebidos.
- Agregações operacionais, ranking de risco, previsão explicável e recomendações determinísticas.
- Contratos JSON, JSON Schemas, manifesto de integridade e quality gate no CI.
- Dashboard operacional publicado no GitHub Pages com cinco áreas analíticas.
- Particionamento mensal das tendências e carregamento sob demanda.
- Diagnóstico local de Web Vitals sem telemetria externa.
- Smoke test end-to-end e benchmark reproduzível de desempenho Spark.
- Guia de contribuição, Código de Conduta, licença MIT e templates do GitHub.

### Alterado

- Perfil Spark otimizado após benchmark com equivalência funcional.
- Dashboard acadêmico inicial evoluído para uma visão operacional com riscos, capacidade, previsão e recomendações.
- Tendências migradas de um payload monolítico para partições mensais cacheáveis.

### Segurança e privacidade

- Dataset acadêmico completo e saídas intermediárias mantidos fora do versionamento.
- Publicação limitada a dados analíticos agregados.
- Manifesto impede o uso acidental de payloads simulados no dashboard publicado.
- Métricas de desempenho do navegador permanecem locais e não utilizam cookies.

### Validação

- 219 testes automatizados.
- Cobertura total acima do quality gate mínimo de 50%.
- Manifesto `HEALTHY` com cinco de cinco contratos válidos.
- Smoke test CSV -> Bronze -> Silver -> Gold aprovado.
- Reexecução incremental idempotente e reconciliação aprovadas.

[Unreleased]: https://github.com/tecStudent/pyspark-it-incident-etl/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/tecStudent/pyspark-it-incident-etl/releases/tag/v1.0.0
