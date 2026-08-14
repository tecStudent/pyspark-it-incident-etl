# Como contribuir

Obrigado pelo interesse em contribuir com o PySpark IT Incident ETL.

Este documento descreve o fluxo utilizado no projeto para manter as mudanças pequenas, auditáveis e validadas pelo GitHub Actions.

## Antes de começar

Você precisa ter instalado:

- Git;
- Docker Desktop;
- Docker Compose.

Java, Spark e PySpark não precisam ser instalados diretamente no computador. O ambiente de execução é fornecido pelo container.

## Escolha uma contribuição

Antes de alterar o código:

1. consulte as Issues existentes para evitar trabalho duplicado;
2. abra uma Issue usando o formulário de bug ou de melhoria;
3. descreva o problema, o resultado esperado e o escopo proposto;
4. não inclua dados acadêmicos brutos, credenciais, tokens ou informações pessoais.

Mudanças pequenas de documentação podem ser enviadas diretamente em um Pull Request, desde que o objetivo esteja explicado.

## Preparar o repositório

~~~bash
git clone https://github.com/tecStudent/pyspark-it-incident-etl.git
cd pyspark-it-incident-etl
docker compose build
docker compose run --rm spark python3 -m pytest -q
~~~

O último comando confirma que o ambiente inicial está saudável antes da alteração.

## Criar uma branch

Sempre parta da `main` atualizada:

~~~bash
git switch main
git pull --ff-only origin main
git switch -c feature/minha-melhoria
~~~

Prefixos adotados:

| Tipo | Uso | Exemplo |
| --- | --- | --- |
| `feature/` | Nova funcionalidade | `feature/dashboard-accessibility` |
| `fix/` | Correção de defeito | `fix/manifest-hash` |
| `docs/` | Documentação | `docs/contribution-guide` |
| `test/` | Testes | `test/incremental-gold` |
| `ci/` | Integração contínua | `ci/cache-docker-layers` |
| `perf/` | Desempenho | `perf/spark-shuffle` |

Use nomes curtos, descritivos, em inglês e separados por hífen.

## Desenvolver e validar

Mantenha a mudança focada em um único objetivo. Reutilize as transformações existentes e adicione testes para todo comportamento novo ou corrigido.

Validação mínima:

~~~bash
docker compose run --rm spark python3 -m pytest -q
git diff --check
git status
~~~

Quando a mudança afetar uma etapa específica, execute também o comando correspondente documentado no README. Alterações no dashboard devem ser verificadas localmente:

~~~bash
python -m http.server 8000 --directory docs
~~~

Acesse `http://localhost:8000` e encerre o servidor com `Ctrl + C`.

## Criar commits

O projeto utiliza Conventional Commits. As mensagens são curtas e escritas em inglês:

~~~text
feat: add operational dashboard filters
fix: preserve manifest hash normalization
test: cover incremental audit failures
docs: document contribution workflow
ci: validate dashboard contracts
perf: reduce Spark shuffle partitions
~~~

Prepare somente os arquivos relacionados à contribuição:

~~~bash
git add caminho/do/arquivo outro/arquivo
git diff --cached --stat
git commit -m "feat: describe the change"
~~~

Evite `git add .` quando houver arquivos não relacionados no working tree.

## Publicar e abrir o Pull Request

~~~bash
git push -u origin nome-da-branch
~~~

Abra um Pull Request para `main` e preencha o template em português. Inclua:

- objetivo e motivação;
- principais alterações;
- impacto para quem utiliza ou mantém o projeto;
- testes e validações executados;
- evidências visuais quando houver alteração no dashboard.

O Pull Request deve permanecer sem merge enquanto o GitHub Actions estiver executando ou com falha.

## Revisão e merge

Antes do merge:

1. confirme que todos os checks estão verdes;
2. responda aos comentários da revisão;
3. aplique ajustes em novos commits na mesma branch;
4. confirme que não existem conflitos com `main`;
5. utilize **Create a merge commit** para preservar o histórico da evolução.

Não utilize **Squash and merge** neste projeto, salvo decisão explícita do mantenedor.

## Sincronizar depois do merge

~~~bash
git switch main
git pull --ff-only origin main
git fetch --prune
git branch -d nome-da-branch
git push origin --delete nome-da-branch
git status
~~~

O resultado esperado é a branch `main` sincronizada com `origin/main` e o working tree limpo.

## Dados e segurança

- Nunca versione o arquivo XLSX acadêmico completo.
- Não versione Parquets, controles locais, relatórios de execução ou dados de quarentena.
- Use apenas amostras sintéticas ou agregações destinadas à publicação.
- Nunca publique senhas, tokens, chaves, URLs privadas ou informações pessoais.
- Antes do commit, revise `git diff --cached` e confirme que cada arquivo pertence ao escopo.

## Conduta

Ao participar do projeto, siga o [Código de Conduta](CODE_OF_CONDUCT.md).

