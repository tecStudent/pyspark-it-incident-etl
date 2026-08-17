# Imagem Docker no GitHub Container Registry

O projeto publica uma imagem executável em `ghcr.io/tecstudent/pyspark-it-incident-etl`. Ela empacota o runtime Spark, as dependências Python, o código de `src`, a versão declarada e somente a pequena amostra pública utilizada pelo smoke test.

O dataset acadêmico completo e as saídas Raw, Bronze, Silver, Gold, quarentena e controles locais não entram no contexto de build. Essa separação é aplicada pela allowlist do `.dockerignore`.

## Quando o workflow executa

| Evento | Build | Smoke do runtime | Trivy | Publicação |
| --- | --- | --- | --- | --- |
| Pull Request para `main` | Sim | Sim | Sim | Não |
| Push na `main` | Sim | Sim | Sim | Sim |
| Tag `vX.Y.Z` | Sim | Sim | Sim | Sim |
| Execução manual | Sim | Sim | Sim | Não |

O workflow usa o `GITHUB_TOKEN` fornecido pelo próprio GitHub Actions. Nenhum Personal Access Token ou segredo adicional é necessário.

## Tags publicadas

- `latest`: commit mais recente aprovado na branch padrão;
- `main`: commit publicado a partir da `main`;
- `sha-<commit>`: referência imutável ao código usado no build;
- `1.0.0`, `1.0` e `1`: aliases gerados para uma tag Git como `v1.0.0`.

Para ambientes reproduzíveis, prefira a tag de versão completa ou a tag `sha-*`. Use `latest` apenas para acompanhar a versão mais recente da branch principal.

## Baixar e validar a imagem

~~~bash
docker pull ghcr.io/tecstudent/pyspark-it-incident-etl:latest

docker run --rm \
  --entrypoint python3 \
  ghcr.io/tecstudent/pyspark-it-incident-etl:latest \
  -c "import pyspark; print(pyspark.__version__)"
~~~

O comando padrão da imagem inicia `python3 src/incremental_pipeline.py`. Para persistir entradas, saídas e controles no Git Bash, monte o diretório local `data`:

~~~bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$(pwd)/data:/app/data" \
  ghcr.io/tecstudent/pyspark-it-incident-etl:latest
~~~

O smoke test empacotado pode ser executado sem o dataset acadêmico:

~~~bash
docker run --rm \
  --entrypoint /opt/spark/bin/spark-submit \
  ghcr.io/tecstudent/pyspark-it-incident-etl:latest \
  --master "local[2]" src/e2e_smoke_test.py
~~~

## Segurança e publicação inicial

Antes de publicar, o job `Build and scan container image` valida as bibliotecas, o usuário não privilegiado, os arquivos mínimos do runtime e executa duas verificações com o Trivy:

1. um relatório completo apresenta vulnerabilidades de severidade alta ou crítica de todo o sistema operacional e também dos JARs herdados da distribuição oficial Spark/Hadoop;
2. o gate bloqueante reprova vulnerabilidades corrigíveis de severidade alta ou crítica no sistema operacional e nas dependências controladas pelo projeto.

Os JARs em `/opt/spark/jars` permanecem visíveis no relatório completo, mas não bloqueiam isoladamente a publicação. Eles fazem parte da distribuição Spark fixada por digest e não devem ser substituídos individualmente, pois versões incompatíveis podem alterar o runtime. A correção dessas ocorrências deve acontecer pela atualização testada da imagem oficial do Spark.

Durante o build, os pacotes de segurança do Ubuntu são atualizados antes da instalação das dependências Python. Essa etapa corrige vulnerabilidades do sistema operacional que já possuem atualização no repositório da distribuição.

Na primeira publicação, abra **Packages > Package settings** e confirme que a imagem está com visibilidade **Public**. O pacote pode nascer privado mesmo quando o repositório é público, dependendo das configurações da conta.

Depois que o check aparecer pela primeira vez em um Pull Request, adicione `Build and scan container image` ao ruleset `Protect main`. Não tente adicioná-lo antes da primeira execução, pois o GitHub ainda não conhecerá o nome do check.

## Diagnóstico rápido

- `denied: permission_denied`: confirme `packages: write` no job e Actions habilitado no repositório;
- pacote não aparece: verifique se o evento foi push na `main` ou tag, pois Pull Requests apenas validam;
- imagem privada: altere a visibilidade nas configurações do pacote;
- relatório completo mostrou CVEs nos JARs: registre a evidência e avalie uma nova imagem oficial do Spark, sem trocar JARs isoladamente;
- gate acionável reprovou: atualize o pacote do sistema operacional ou a dependência Python indicada; não remova o gate para liberar a publicação.
