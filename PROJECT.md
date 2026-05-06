# Contexto do projeto (estável)

Documento de referência para decisões e convenções que mudam com pouca frequência.

## Linguagem e runtime

| Aspecto | Valor |
|--------|--------|
| Linguagem | Python |
| Versões suportadas (CI) | 3.9, 3.10, 3.11, 3.12 |
| Estilo de tipagem | Anotações onde faz sentido (`from __future__ import annotations`; `typing` / tipos PEP 604) |

O README recomenda Python 3.10+ localmente; o pacote evita depender de APIs que quebrem 3.9 sem necessidade.

## Arquitetura

- **Monólito funcional**: a lógica principal concentra-se em `renomear_ebooks.py` (CLI, I/O, metadados locais/remotos, heurísticas de nome).
- **Sem framework web**: script executável + módulo importável nos testes (`import renomear_ebooks`).
- **Dependências de runtime** (ver `requirements-dev.txt` para o conjunto usado em CI): `requests`, `rapidfuzz`, `pypdf` (PDF opcional mas recomendado).
- **Opcional**: `defusedxml` para parsing de EPUB com endurecimento extra; sem ele usa-se `xml.etree` com limites de tamanho.

## Build e empacotamento

- Não há `pyproject.toml` nem `setup.cfg`: instalação por `pip install -r requirements-dev.txt` (desenvolvimento / CI).
- Artefato principal: o próprio repositório + script na raiz.

## Testes

| Ferramenta | Uso |
|------------|-----|
| **pytest** | `python -m pytest tests/`; config em `pytest.ini` (`pythonpath = .`, `testpaths = tests`) |
| **Asserções** | Funções e classes de teste nomeadas em inglês (`test_*`, `Test*`), docstrings curtas em português quando útil |

CI separa **lint** (uma corrida, Python 3.12) de **testes** (matriz 3.9–3.12).

## Lint e análise estática

| Ferramenta | Escopo |
|------------|--------|
| **flake8** | `renomear_ebooks.py`, `tests/`; regras em `.flake8` (linha 120; `E501` ignorado só no script principal por tamanho) |
| **pylint** | Workflow dedicado; matriz 3.10–3.12 |

## CI / automação

- **GitHub Actions**: `python-package.yml` (flake8 + pytest), `pylint.yml`.
- Ficheiros em `__pycache__/`, saídas locais `renamed/`, caches e venvs estão no `.gitignore`.

## Convenções de código e nomes

- **Módulos e ficheiros**: `snake_case` (`renomear_ebooks.py`, `test_parse_filename.py`).
- **Funções e variáveis**: `snake_case`; prefixo `_` para helpers internos do módulo.
- **Tipos / dados**: `BookMeta` e similares em `PascalCase` (`dataclasses`).
- **Strings voltadas ao utilizador** (logs, mensagens, comentários longos no domínio do script): preferência por **português** (pt), alinhado ao README.
- **Nomes de ficheiros gerados**: padrão histórico documentado no README (`AUTOR - ANO - TÍTULO`), com regras adicionais para padrões customizados (`--filename-pattern`).

## Dados auxiliares na raiz

- `author_overrides.json`: overrides de autor (chave/valor conforme documentado no README).
- Não commitar paths locais específicos de biblioteca; pastas de trabalho ficam fora do controle de versão.

---

Alterações frequentes (flags CLI, heurísticas, fontes remotas) pertencem ao README e às notas de versão, não a este ficheiro.
