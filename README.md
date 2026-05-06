# renomear_ebooks.py

Renomeador de e-books em Python para padronizar nomes de arquivo usando heuristicas locais e, quando necessario, fontes externas.

Formato padrao de saida:
`SOBRENOME, Nome - Ano - Titulo.ext`

## O que o script faz

- Processa bibliotecas de e-books e propõe/aplica novos nomes.
- Extrai metadados de nome do arquivo, PDF/EPUB e fontes remotas.
- Mantem extensao original e gera relatorios CSV.
- Prioriza seguranca operacional: modo simulacao por padrao, revisao manual para casos duvidosos e protecao contra conflitos de nome.

## Funcionalidades reais

- Renomeacao planejada (`rename_plan.csv`) ou aplicada (`rename_log.csv`).
- Leitura local:
  - `EPUB` (container XML/OPF, com limite de tamanho em XML).
  - `PDF` (metadados + texto das primeiras paginas para ISBN/ano).
  - Fallback por nome do arquivo para todos os formatos.
- Consulta remota opcional com merge controlado:
  - Open Library
  - Google Books
  - Wikipedia
  - DuckDuckGo HTML para Skoob, catalogs agregados e fallback web
- Score de confianca local vs final (`match_score`) e bandas de revisao.
- Tolerancia a falhas externas por fonte (timeout, conexao, HTTP, JSON invalido, campo ausente), com registro estruturado em `source_failures`.
- Cache HTTP por pasta em `metadata_cache.json`.
- Revisao interativa (`--review`) para itens nao automaticos.
- Overrides de autor via JSON (`author_overrides.json`).
- Metadado suplementar via `.json`, `.csv` ou `.txt` (TSV).
- Catalogo final opcional (`catalog.json`/`catalog.csv`).
- Rotinas de duplicados:
  - `--find-duplicates` (heuristico por ISBN/autor+titulo/fingerprint parcial)
  - `--dedup` (hash completo MD5/SHA1)

## Formatos suportados

- `.epub`
- `.pdf`
- `.mobi`
- `.azw`
- `.azw3`
- `.djvu`

Observacao: `.html` e ignorado.

## Requisitos

- Python 3.9+ (CI roda 3.9-3.12).
- Dependencias de runtime:
  - `requests`
  - `rapidfuzz`
  - `pypdf` (recomendado para PDFs)
- Opcional de seguranca extra para EPUB XML:
  - `defusedxml` (se ausente, usa `xml.etree` com limite de tamanho)

## Instalacao

```bash
python -m venv .venv
```

Windows PowerShell:

```bash
.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Instale dependencias:

```bash
pip install -r requirements-dev.txt
```

Ou apenas runtime:

```bash
pip install requests rapidfuzz pypdf
```

## Uso basico

Simulacao (padrao; nao renomeia fisicamente):

```bash
python renomear_ebooks.py "E:\Livros"
```

Aplicar renomeacoes:

```bash
python renomear_ebooks.py "E:\Livros" --apply
```

Recursivo:

```bash
python renomear_ebooks.py "E:\Livros" --recursive
```

Filtrar extensoes:

```bash
python renomear_ebooks.py "E:\Livros" --exts "pdf,epub,.mobi"
```

## Flags principais

- Escopo/execucao:
  - `--recursive`
  - `--limit N`
  - `--jobs N`
  - `--quiet`
  - `--omit-console`
- Aplicacao/revisao:
  - `--apply`
  - `--review` (incompativel com `--apply`)
- Fontes remotas:
  - `--source offline|openlibrary|google|skoob|catalogs|wikipedia|web|all`
  - `--sources openlibrary,google,...` (com `--source all`)
  - `--force-remote` (alias: `--fetch-remote-always`)
- Performance remota:
  - `--fast`
  - `--thorough`
  - `--search-speed 1..5`
  - `--sleep SEG`
  - `--max-pdf-pages N`
- Merge de metadados:
  - `--remote-metadata title,authors,year,isbn,publisher`
  - `--keep-local-metadata ...`
  - `--prefer-remote-title`
- Nome final:
  - `--filename-pattern "%AUTHOR% - %DATE% - %TITLE%%FORMAT%"`
  - `--unknown-year sd|omit`
  - `--unknown-year-text TEXTO`
  - `--omit-date-if-missing`
  - `--max-authors N`
- Arquivos auxiliares:
  - `--overrides ARQUIVO.json`
  - `--supplementary-data ARQUIVO.(json|csv|txt)`
  - `--supplementary-mode merge|override`
- Relatorios extras:
  - `--missing-year-log [ARQUIVO.csv]`
  - `--generate-catalog --catalog-format json|csv|both`
- Duplicados:
  - `--find-duplicates [--move-duplicates] [--prefer-format ...]`
  - `--dedup [--dedup-algorithm md5|sha1] [--delete-dups]`

Use `python renomear_ebooks.py --help` para referencia completa e exemplos atualizados da CLI.

## Dry-run, saidas e logs

Para cada pasta raiz `PASTA`, o script grava em `PASTA/renamed/`:

- `rename_plan.csv` (sem `--apply`) ou `rename_log.csv` (com `--apply`)
- `metadata_cache.json`
- opcional: `missing_years.csv` (ou nome fornecido em `--missing-year-log`)
- opcional: `review_needed.csv` (com `--review`)
- opcional: `catalog.json`/`catalog.csv` (com `--generate-catalog`)
- opcional: `duplicates_report.csv` ou `duplicates.csv` (modos de deduplicacao)

Colunas importantes no plano/log incluem:
- `original`
- `novo`
- `status`
- `titulo`
- `autores`
- `ano`
- `fonte`
- `confianca`
- `pontuacao`
- `evidencias`
- `source_failures`
- `notas`

## Comportamento de seguranca

- Preserva extensao original do arquivo.
- Evita sobrescrita com sufixos ` (2)`, ` (3)`, etc. quando necessario.
- Nao sai da pasta de destino ao montar caminho final.
- Escrita atomica para cache JSON (`os.replace`).
- Sanitiza nome de arquivo para caracteres invalidos.
- Escape anti formula-injection em CSV (`=`, `+`, `-`, `@`, TAB, CR).
- Por padrao, roda em simulacao.
- Falhas de fonte externa nao derrubam o lote inteiro.
- Itens com falha externa + pontuacao nao automatica podem ser marcados como `revisao_necessaria`.

## Heuristicas bibliograficas (resumo)

- Parsing de autores com `;`, `&`, e separacao cautelosa de `e`/`and`.
- Bloqueio de falso autor para ano puro, tokens editoriais e lixo de nome de arquivo.
- Tratamento de parenteses editoriais (ex.: `2nd edition`, `book club edition`, `Penguin Classics`) para evitar classificacao como autor.
- Prioriza ano no sufixo `(YYYY)` do nome quando presente.
- Guardrails de merge:
  - bloqueio de autor remoto incompatível com autor local plausivel
  - bloqueio de ano remoto outlier

## Fontes externas e tolerancia a falhas

Fluxo remoto nao usa `retry` com backoff; usa:
- timeout fixo de 20s por requisicao HTTP
- pausa configuravel entre requests (`--sleep` e perfis de velocidade)
- sessao HTTP compartilhada com `User-Agent: ebook-renamer/1.0`
- cache por URL+params
- classificacao e registro de falhas por fonte:
  - `timeout`
  - `connection_error`
  - `rate_limit_reached_http_429`
  - `http_500/502/503/504_temporary_unavailable`
  - `invalid_json`
  - `missing_expected_field_*`
  - `source_error:*` quando a funcao da fonte falha

## Limitacoes conhecidas

- DOI nao e atualmente extraido/validado como campo dedicado.
- Qualidade de snippets DDG varia por regiao, idioma e bloqueios anti-bot.
- Metadados embutidos de PDF/EPUB podem vir poluidos e causar ambiguidades.
- Em acervos heterogeneos, alguns casos devem ficar para revisao manual.

## Testes

Rodar suite:

```bash
python -m pytest -q tests/
```

Lint:

```bash
flake8 renomear_ebooks.py tests/
```

Cobertura atual inclui parsing de nome, heuristicas de autores/titulos e tolerancia a falhas externas com mocks.
