# Proteção da branch principal

Este guia define o ruleset recomendado para a branch `main` do PySpark IT Incident ETL. O objetivo é impedir alterações diretas, merges com checks falhando, exclusões e force pushes, preservando o fluxo de Pull Requests adotado pelo projeto.

O arquivo `.github/CODEOWNERS` mantém a responsabilidade técnica explícita e permite que o GitHub solicite revisões automaticamente quando novos colaboradores forem adicionados.

## Pré-requisitos

- Task 16.0 integrada à `main`;
- workflows `ci.yml`, `codeql.yml`, `dependency-review.yml` e `container-image.yml` executados pelo menos uma vez;
- permissão de administrador no repositório;
- Dependency graph habilitado.

## Criar o ruleset

1. Abra o repositório no GitHub.
2. Acesse **Settings > Rules > Rulesets**.
3. Clique em **New ruleset > New branch ruleset**.
4. Em **Ruleset name**, informe `Protect main`.
5. Em **Enforcement status**, selecione **Active**.
6. Não adicione bypass inicialmente.

## Definir a branch protegida

Em **Target branches**:

1. clique em **Add target**;
2. escolha **Include default branch**;
3. confirme que a regra aponta para `main`.

Não utilize o padrão `*`, pois ele também protegeria branches temporárias de desenvolvimento.

## Regras obrigatórias

Ative as seguintes regras:

- **Restrict deletions**;
- **Block force pushes**;
- **Require a pull request before merging**;
- **Require status checks to pass**;
- **Require branches to be up to date before merging**;
- **Require conversation resolution before merging**.

Na configuração do Pull Request:

- mantenha **Required approvals** em `0` enquanto o repositório tiver apenas um mantenedor ativo;
- habilite **Dismiss stale pull request approvals when new commits are pushed** quando passar a exigir aprovações;
- habilite **Require review from Code Owners** somente depois que existir outro colaborador com permissão de escrita;
- mantenha **Merge** como método permitido para preservar o histórico com **Create a merge commit**.

Não habilite **Require linear history**, pois essa regra impediria o merge commit adotado pelo projeto. Também não exija commits assinados antes de configurar uma chave de assinatura em todas as máquinas utilizadas.

## Status checks obrigatórios

Adicione exatamente estes checks:

| Check | Workflow | Proteção |
| --- | --- | --- |
| `Run PySpark tests` | `ci.yml` | Testes, cobertura, manifesto, release e smoke test |
| `Analyze Python` | `codeql.yml` | Análise estática de segurança com CodeQL |
| `Review dependency changes` | `dependency-review.yml` | Vulnerabilidades e licenças adicionadas pelo PR |
| `Build and scan container image` | `container-image.yml` | Build, runtime mínimo e vulnerabilidades da imagem |

Os nomes devem corresponder aos exibidos em um Pull Request recente. Não selecione checks antigos com nomes semelhantes.

O check da imagem deve ser adicionado somente depois da primeira execução do workflow em um Pull Request. Antes disso, o GitHub ainda não oferece esse nome na busca de status checks.

## Validar a proteção

Depois de salvar o ruleset:

1. confirme que o status aparece como **Active**;
2. abra uma branch de teste com uma alteração pequena;
3. confirme que a `main` só aceita a mudança por Pull Request;
4. confirme que o merge fica bloqueado enquanto algum check está pendente ou falhando;
5. confirme que **Create a merge commit** continua disponível;
6. feche a branch de teste sem alterar regras para contornar os checks.

## Evolução para trabalho em grupo

Quando houver pelo menos dois colaboradores com permissão de escrita:

1. adicione os usuários ou uma equipe ao `CODEOWNERS` conforme a área de responsabilidade;
2. altere **Required approvals** de `0` para `1`;
3. habilite **Require review from Code Owners**;
4. mantenha administradores sujeitos ao mesmo fluxo sempre que possível;
5. documente qualquer bypass com motivo, responsável e prazo para remoção.

O autor de um Pull Request não pode aprovar a própria alteração. Por isso, ativar uma aprovação obrigatória antes da entrada de outro revisor bloquearia o fluxo individual.

## Recuperação

Se uma configuração bloquear todos os merges, não remova o ruleset. Altere temporariamente apenas a regra causadora, registre o motivo no Pull Request e restaure a proteção assim que o ajuste for concluído.
