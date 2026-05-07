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

## Referencia de flags (comportamento de cada uma)

- `PASTA` (argumento posicional): uma ou mais pastas de entrada; sem `--recursive` processa so o nivel imediato.
- `--apply`: aplica renomeacao/movimentacao fisica (sem esta flag, fica em simulacao).
- `--review`: revisao interativa para casos nao automaticos; gera `review_needed.csv`; incompativel com `--apply`.
- `--recursive`: inclui subpastas na varredura.
- `--exts EXTS`: filtra extensoes permitidas (CSV; com ou sem ponto), ex.: `pdf,epub,.mobi`.
- `--source`: seleciona estrategia de fonte remota (`offline`, `openlibrary`, `google`, `skoob`, `catalogs`, `wikipedia`, `web`, `all`).
- `--sources LISTA`: restringe fontes quando `--source all` (ex.: `openlibrary,google,wikipedia`); tem precedencia sobre velocidade.
- `--prefer-remote-title`: prefere titulo remoto no merge quando houver.
- `--remote-metadata CAMPOS`: define quais campos podem ser atualizados pelo remoto (`title,authors,year,isbn,publisher`).
- `--keep-local-metadata CAMPOS`: define campos que devem manter valor local quando preenchidos.
- `--max-authors N`: limita autores no nome final; acima do limite vira `et al.`; `0` mostra todos.
- `--unknown-year sd|omit`: com ano ausente, usa placeholder (`sd`) ou remove bloco de data (`omit`).
- `--omit-date-if-missing`: atalho para comportamento de `--unknown-year omit`.
- `--unknown-year-text TEXTO`: texto do placeholder de ano quando `sd` (padrao `s.d.`).
- `--filename-pattern PADRAO`: padrao customizado com `%AUTHOR% %DATE% %TITLE% %PUBLISHER% %FORMAT%`.
- `--year-strategy original|edition`: criterio para escolher ano entre candidatos (mais antigo ou mais recente).
- `--max-pdf-pages N`: numero maximo de paginas lidas no PDF para inferencia local.
- `--sleep SEG`: pausa entre requests HTTP.
- `--fast`: perfil rapido (menos rede, menos leitura de PDF); incompativel com `--thorough` e `--search-speed`.
- `--thorough`: perfil mais completo (mais rede e leitura de PDF); incompativel com `--fast` e `--search-speed`.
- `--search-speed N`: velocidade de busca remota (1 a 5); incompativel com `--fast` e `--thorough`.
- `--overrides ARQUIVO`: caminho do JSON de sobrescrita de autores (padrao `author_overrides.json` na pasta alvo).
- `--supplementary-data ARQUIVO`: metadado adicional por arquivo (`.json`, `.csv` ou `.txt` TSV).
- `--supplementary-mode merge|override`: modo de aplicacao do metadado suplementar.
- `--missing-year-log [ARQUIVO.csv]`: gera CSV so com itens sem ano (nome default se nao informar caminho).
- `--limit N`: limita quantidade de arquivos por pasta (ordem alfabetica).
- `--jobs N`: paralelismo da leitura local (PDF/EPUB).
- `--only-missing-year`: processa so itens sem ano apos etapa local.
- `--force-remote` / `--fetch-remote-always`: forca etapa remota mesmo quando local ja trouxe ano/estrutura suficiente.
- `--quiet`: reduz logs de progresso no console.
- `--omit-console`: silencia logs de console (exceto fatal); incompativel com `--review`.
- `--generate-catalog`: gera catalogo de saida em `renamed/`.
- `--catalog-format json|csv|both`: formato do catalogo quando `--generate-catalog` estiver ativo.
- `--find-duplicates`: detecta duplicados por heuristica (ISBN, autor+titulo, fingerprint parcial).
- `--duplicates-report [ARQUIVO.csv]`: define nome/caminho do relatorio de duplicados heuristico.
- `--move-duplicates`: move duplicados heuristicos para pasta `duplicates/`; exige `--find-duplicates`.
- `--prefer-format LISTA`: ordem de preferencia de formatos para escolher exemplar a manter em `--find-duplicates`.
- `--prefer-larger`: em empate, prefere arquivo maior no modo de duplicados heuristico.
- `--prefer-smaller`: em empate, prefere arquivo menor no modo de duplicados heuristico.
- `--dedup`: detecta duplicados por hash completo (MD5/SHA1), separado de `--find-duplicates`.
- `--dedup-algorithm md5|sha1`: algoritmo de hash para `--dedup`.
- `--delete-dups`: move duplicados detectados por hash para `renamed/duplicates`; exige `--dedup`.

Combinacoes invalidas importantes:
- `--apply` com `--review`
- `--fast` com `--thorough` ou `--search-speed`
- `--find-duplicates` com `--dedup`
- `--generate-catalog` com `--find-duplicates`/`--dedup`

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
