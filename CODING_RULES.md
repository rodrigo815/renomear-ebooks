# Coding Rules

## Scope

Este projeto é um programa pequeno em Python, organizado como um ou alguns scripts.

O objetivo é manter o código simples, legível, executável localmente e fácil de alterar.

## General principles

- Preferir código direto a arquitetura artificial.
- Não transformar script pequeno em framework.
- Não criar classes sem estado ou sem necessidade concreta.
- Não criar múltiplos arquivos apenas por estética.
- Não adicionar dependências externas sem justificativa forte.
- Preservar compatibilidade com a versão de Python definida no projeto.
- Manter o menor diff possível em refatorações.

## Structure

Scripts devem seguir esta estrutura geral quando fizer sentido:

```python
from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    ...


def main() -> int:
    args = parse_args()
    ...
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Regras:

- Todo script executável deve ter função `main()`.
- A execução direta deve ficar protegida por `if __name__ == "__main__"`.
- Parsing de argumentos deve ficar separado da lógica principal.
- Funções devem ter responsabilidade clara.
- Evitar lógica relevante no escopo global.
- Evitar variáveis globais mutáveis.

## Naming

- Usar nomes específicos do problema.
- Evitar nomes genéricos como `data`, `result`, `item`, `thing`, quando houver nome mais preciso.
- Usar nomes em inglês se o código já estiver em inglês.
- Manter consistência com o estilo existente do script.

## Functions

- Preferir funções pequenas e testáveis.
- Separar leitura de arquivo, transformação de dados e escrita de resultado quando isso reduzir confusão.
- Não criar função para uma linha óbvia se isso piorar a leitura.
- Não criar helper genérico usado uma única vez, salvo se a extração melhorar claramente a legibilidade.

## Types

- Usar type hints em funções públicas ou centrais.
- Não exagerar em tipos complexos quando o script for simples.
- Preferir `Path` em vez de manipulação manual de string para caminhos.
- Usar `dataclass` apenas quando houver estrutura de dados clara, com campos nomeados e uso recorrente.

## Input and output

- Validar existência de arquivos antes de ler.
- Não sobrescrever arquivo de saída sem deixar isso claro no nome do argumento ou na mensagem.
- Quando houver risco de perda de dados, exigir argumento explícito como `--overwrite`.
- Usar encoding explícito para texto, normalmente `utf-8`.
- Separar mensagens de erro da saída principal quando o script for usado em pipeline.

## Error handling

- Não engolir exceções silenciosamente.
- Não usar `except Exception` sem necessidade concreta.
- Mensagens de erro devem indicar o arquivo, argumento ou operação que falhou.
- Para erro esperado de uso, retornar código diferente de zero.
- Para erro de programação, deixar a exceção aparecer durante desenvolvimento.

## Logging and output

- Para script simples, `print()` é aceitável para saída do usuário.
- Usar `logging` quando houver níveis, modo verbose ou execução longa.
- Não espalhar prints de debug permanentes.
- Mensagens devem ser concretas e úteis.

## Dependencies

- Preferir biblioteca padrão quando ela resolver bem o problema.
- Não adicionar pandas, requests, typer, rich, pydantic ou similares sem necessidade real.
- Se adicionar dependência, justificar no resumo da alteração.
- Não trocar biblioteca existente por preferência estética.

## Comments

Comentários são aceitáveis quando explicam:

- formato estranho de arquivo;
- workaround necessário;
- regra externa;
- decisão não óbvia;
- limitação conhecida.

Comentários não devem repetir o código.

Exemplo ruim:

```python
# Open the file
with path.open("r", encoding="utf-8") as file:
    content = file.read()
```

Exemplo aceitável:

```python
# Some exported CSV files include a UTF-8 BOM.
content = path.read_text(encoding="utf-8-sig")
```

## Tests

- Criar testes para funções de transformação, parsing relevante e regras de negócio.
- Evitar testar apenas se função foi chamada.
- Para bugfix, criar teste que falha antes da correção e passa depois.
- Usar arquivos temporários para testar leitura e escrita.
- Não depender de caminhos absolutos da máquina local.

## Refactoring

Antes de refatorar:

- identificar comportamento atual;
- preservar interface de linha de comando;
- preservar formato de entrada e saída;
- criar teste de caracterização se o script não tiver testes.

Durante a refatoração:

- fazer uma mudança por vez;
- evitar reorganização ampla;
- evitar transformar script em pacote complexo;
- manter menor diff possível.

Depois da refatoração:

- rodar testes;
- executar o script com exemplo real ou mínimo;
- verificar se argumentos e saídas continuam iguais.

## Forbidden unless explicitly authorized

Não fazer sem autorização explícita:

- alterar argumentos de linha de comando existentes;
- mudar formato de saída;
- sobrescrever arquivos automaticamente;
- adicionar dependências externas;
- converter script simples em aplicação com várias camadas;
- criar classes artificiais;
- alterar encoding esperado;
- reformatar o arquivo inteiro;
- remover tratamento de erro existente sem substituição melhor.
