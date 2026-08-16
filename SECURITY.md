# Política de segurança

## Versões suportadas

Correções de segurança são aplicadas à linha estável mais recente.

| Versão | Suporte |
| --- | --- |
| `1.0.x` | Sim |
| `< 1.0` | Não |

## Relatar uma vulnerabilidade

Não abra uma Issue pública para relatar uma possível vulnerabilidade, credencial exposta ou forma de acessar dados não publicados.

Use a opção **Report a vulnerability** na aba **Security** do repositório para enviar um relatório privado. Se essa opção não estiver disponível, contate o mantenedor pelo perfil do GitHub e solicite um canal privado, sem incluir detalhes sensíveis na mensagem inicial.

Inclua, quando possível:

- componente, arquivo ou versão afetada;
- descrição do comportamento observado;
- passos mínimos para reprodução;
- impacto potencial;
- sugestão de correção ou mitigação;
- evidências sem dados acadêmicos brutos, segredos ou informações pessoais.

## Tratamento do relato

O mantenedor deverá:

1. confirmar o recebimento;
2. reproduzir e classificar o impacto;
3. preparar a correção em ambiente privado quando necessário;
4. validar testes, CodeQL e revisão de dependências;
5. publicar uma versão corretiva e registrar a mudança no changelog;
6. coordenar a divulgação responsável com quem realizou o relato.

Não são oferecidos prazos formais de SLA, pois este é um projeto acadêmico e de portfólio. O status será comunicado pelo canal privado utilizado no relato.

## Escopo

A política cobre o código Python, os workflows do GitHub Actions, a imagem Docker, os contratos JSON e o dashboard publicado. O dataset acadêmico original, controles locais e saídas Parquet não são distribuídos pelo repositório.

## Práticas automatizadas

- CodeQL analisa o código Python em pushes, Pull Requests e execução semanal.
- Dependency Review bloqueia novas dependências com vulnerabilidade alta ou crítica.
- Dependabot verifica semanalmente dependências Python e GitHub Actions.
- Pull Requests continuam exigindo revisão, testes e ausência de credenciais ou dados brutos.
