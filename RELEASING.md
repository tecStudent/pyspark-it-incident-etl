# Processo de release

Este documento descreve como publicar uma versão estável do PySpark IT Incident ETL.

## Convenção de versão

O projeto utiliza Versionamento Semântico:

- `MAJOR`: mudança incompatível;
- `MINOR`: nova funcionalidade compatível;
- `PATCH`: correção compatível.

As tags utilizam o prefixo `v`, enquanto o arquivo `VERSION` contém apenas o número. Exemplo: `VERSION=1.0.0` e tag `v1.0.0`.

## Pré-condições

Uma release só pode ser criada quando:

- a versão está declarada em `VERSION`;
- o `CHANGELOG.md` contém a versão e a data;
- as notas existem em `docs/releases/`;
- o GitHub Actions da `main` está verde;
- o manifesto do dashboard está `HEALTHY`;
- o smoke test end-to-end está aprovado;
- o working tree está limpo;
- a versão ainda não possui tag ou Release no GitHub.

## Validar a preparação

~~~bash
docker compose run --rm spark python3 -m pytest -q

docker compose run --rm spark \
  python3 src/release_readiness.py --check --version 1.0.0

git status
~~~

O quality gate deve apresentar `Release readiness: APROVADO`.

## Sincronizar depois do Pull Request

Depois do merge da preparação da release:

~~~bash
git switch main
git pull --ff-only origin main
git fetch --prune
git status
~~~

Confirme no GitHub que a execução da `main` está verde antes de criar a tag.

## Criar a tag anotada

~~~bash
git tag -a v1.0.0 -m "Release v1.0.0"
git show v1.0.0 --no-patch
git push origin v1.0.0
~~~

A tag deve apontar para o merge commit da preparação da release na `main`, nunca para a branch do Pull Request.

## Publicar no GitHub

1. Abra **Releases** no repositório.
2. Selecione **Draft a new release**.
3. Escolha a tag existente `v1.0.0`.
4. Use o título `v1.0.0 — Operational PySpark Incident ETL`.
5. Copie o conteúdo de `docs/releases/v1.0.0.md` para a descrição.
6. Marque a versão como **Set as the latest release**.
7. Não marque **Set as a pre-release**.
8. Revise os links e publique com **Publish release**.

O GitHub disponibiliza automaticamente os arquivos-fonte `.zip` e `.tar.gz`. O dataset acadêmico não deve ser anexado.

## Verificar a publicação

Confirme:

- Release: `https://github.com/tecStudent/pyspark-it-incident-etl/releases/tag/v1.0.0`;
- Pages: `https://tecstudent.github.io/pyspark-it-incident-etl/`;
- badge `v1.0.0` no README;
- screenshots atuais no README;
- download dos arquivos-fonte;
- tag visível na página de commits.

## Corrigir uma tag antes da publicação

Se a tag estiver incorreta e a Release ainda não tiver sido publicada, confirme o alvo antes de removê-la:

~~~bash
git tag -d v1.0.0
git push origin :refs/tags/v1.0.0
~~~

Não mova ou substitua uma tag de release já publicada. Nesse caso, corrija o problema em uma nova versão PATCH.

