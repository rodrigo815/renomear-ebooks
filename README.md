# renomear_ebooks.py

Renomeador de e-books em Python para padronizar nomes de arquivo usando heurísticas locais e, quando necessário, fontes externas.

Formato padrão de saída:
`SOBRENOME, Nome - Ano - Título.ext`

## O que o script faz

- Processa bibliotecas de e-books e propõe/aplica novos nomes.
- Extrai metadados de nome do arquivo, PDF/EPUB e fontes remotas.
- Mantém extensão original e gera relatórios CSV.
- Prioriza segurança operacional: modo simulação por padrão, revisão manual para casos duvidosos e proteção contra conflitos de nome.

## Funcionalidades reais

- Renomeação planejada (`rename_plan.csv`) ou aplicada (`rename_log.csv`).
- Leitura local:
  - `EPUB` (container XML/OPF, com limite de tamanho em XML).
  - `PDF` (metadados + texto das primeiras páginas para ISBN/ano).
  - Fallback por nome do arquivo para todos os formatos.
- Consulta remota opcional com merge controlado:
  - Open Library
  - Google Books
  - Wikipedia
  - DuckDuckGo HTML para Skoob, catálogos agregados e fallback web
- Score de confiança local vs final (`match_score`) e bandas de revisão.
- Tolerância a falhas externas por fonte (timeout, conexão, HTTP, JSON inválido, campo ausente), com registro estruturado em `source_failures`.
- Cache HTTP por pasta em `metadata_cache.json`.
- Controle de previsibilidade operacional por limite de chamadas/custo/tempo por item.
- Revisão interativa (`--review`) para itens não automáticos.
- Modo de planejamento (`--planning-only`) com saída em Markdown + JSON sem renomear.
- Overrides de autor via JSON (`author_overrides.json`).
- Aliases canônicos opcionais de autor (`--author-aliases`).
- Metadado suplementar via `.json`, `.csv` ou `.txt` (TSV).
- Catálogo final opcional (`catalog.json`/`catalog.csv`).
- Rotinas de duplicados:
  - `--find-duplicates` (heurístico por ISBN/autor+título/fingerprint parcial)
  - `--dedup` (hash completo MD5/SHA1)

## Formatos suportados

- `.epub`
- `.pdf`
- `.mobi`
- `.azw`
- `.azw3`
- `.djvu`

Observação: `.html` é ignorado.

## Requisitos

- Python 3.9+ (CI roda 3.9-3.12).
- Dependências de runtime:
  - `requests`
  - `rapidfuzz`
  - `pypdf` (recomendado para PDFs)
- Opcional de segurança extra para EPUB XML:
  - `defusedxml` (se ausente, usa `xml.etree` com limite de tamanho)

## Instalação

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

Instale dependências:

```bash
pip install -r requirements-dev.txt
```

Ou apenas runtime:

```bash
pip install requests rapidfuzz pypdf
```

## Uso básico

Simulação (padrão; não renomeia fisicamente):

```bash
python renomear_ebooks.py "E:\Livros"
```

Aplicar renomeações:

```bash
python renomear_ebooks.py "E:\Livros" --apply
```

Recursivo:

```bash
python renomear_ebooks.py "E:\Livros" --recursive
```

Filtrar extensões:

```bash
python renomear_ebooks.py "E:\Livros" --exts "pdf,epub,.mobi"
```

## Flags principais

- Escopo/execução:
  - `--execution-profile safe|balanced|aggressive`: perfil pronto de execução.
  - `--recursive`: inclui subpastas na varredura.
  - `--limit N`: limita a quantidade de arquivos por pasta (ordem alfabética).
  - `--jobs N`: define paralelismo da leitura local (PDF/EPUB).
  - `--quiet`: reduz logs de progresso no console.
  - `--omit-console`: silencia logs de console (exceto fatal); incompatível com `--review`.
  - `--planning-only`: só classifica risco e recomenda ação; não gera nome final.
- Aplicação/revisão:
  - `--apply`: aplica renomeação/movimentação física (sem esta flag, permanece em simulação).
  - `--review`: revisão interativa para casos não automáticos; gera `review_needed.csv`; incompatível com `--apply`.
  - `--quarantine`: ativa `originals/`, `failed/`, `converted/` no `renamed/`; backup pré-renomeio e quarentena de falhas.
- Fontes remotas:
  - `--source offline|openlibrary|google|skoob|catalogs|wikipedia|web|all`: seleciona estratégia de consulta remota.
  - `--sources openlibrary,google,...`: restringe fontes quando `--source all`; tem precedência sobre velocidade.
  - `--force-remote` (alias `--fetch-remote-always`): força etapa remota mesmo quando metadado local já parece suficiente.
- Performance remota:
  - `--fast`: perfil rápido (menos rede, menos leitura de PDF); incompatível com `--thorough` e `--search-speed`.
  - `--thorough`: perfil mais completo (mais rede e leitura de PDF); incompatível com `--fast` e `--search-speed`.
  - `--search-speed 1..5`: ajusta intensidade de busca remota; incompatível com `--fast` e `--thorough`.
  - `--sleep SEG`: pausa entre requests HTTP.
  - `--max-pdf-pages N`: número máximo de páginas lidas no PDF para inferência local.
  - `--max-remote-calls-per-file N`: limite de fontes remotas por item (0 = sem limite).
  - `--max-estimated-cost VALOR`: teto de custo estimado por execução (0 = sem limite).
  - `--item-timeout-s SEGUNDOS`: timeout total por item; excedido => decisão conservadora local.
- Merge de metadados:
  - `--remote-metadata title,authors,year,isbn,publisher`: define quais campos podem ser atualizados pelo remoto.
  - `--keep-local-metadata ...`: define campos que devem manter valor local quando já preenchidos.
  - `--prefer-remote-title`: prefere título remoto no merge quando houver.
- Nome final:
  - `--filename-pattern "%AUTHOR% - %DATE% - %TITLE%%FORMAT%"`: padrão customizado para saída.
  - `--unknown-year sd|omit`: com ano ausente, usa placeholder (`sd`) ou remove bloco de data (`omit`).
  - `--unknown-year-text TEXTO`: texto do placeholder de ano quando `sd` (padrão `s.d.`).
  - `--omit-date-if-missing`: atalho para comportamento de `--unknown-year omit`.
  - `--max-authors N`: limita autores no nome final; acima do limite vira `et al.`; `0` mostra todos.
- Arquivos auxiliares:
  - `--overrides ARQUIVO.json`: caminho do JSON de sobrescrita de autores.
  - `--author-aliases ARQUIVO.json`: aliases canônicos de autor (com proteção para não sobrescrever autor local forte).
  - `--supplementary-data ARQUIVO.(json|csv|txt)`: metadado adicional por arquivo.
  - `--supplementary-mode merge|override`: modo de aplicação do metadado suplementar.
- Relatórios extras:
  - `--missing-year-log [ARQUIVO.csv]`: gera CSV só com itens sem ano.
  - `--generate-catalog --catalog-format json|csv|both`: gera catálogo de saída.
- Duplicados:
  - `--find-duplicates [--move-duplicates] [--prefer-format ...]`: detecção heurística.
  - `--duplicates-report [ARQUIVO.csv]`: define nome/caminho do relatório heurístico.
  - `--prefer-larger` / `--prefer-smaller`: critério de desempate em `--find-duplicates`.
  - `--dedup [--dedup-algorithm md5|sha1] [--delete-dups]`: detecção por hash completo e ação opcional.

Use `python renomear_ebooks.py --help` para referência completa e exemplos atualizados da CLI.

## Dry-run, saídas e logs

Para cada pasta raiz `PASTA`, o script grava em `PASTA/renamed/`:

- `rename_plan.csv` (sem `--apply`) ou `rename_log.csv` (com `--apply`)
- `metadata_cache.json`
- opcional: `missing_years.csv` (ou nome fornecido em `--missing-year-log`)
- opcional: `review_needed.csv` (com `--review`)
- `run_summary.md`
- `phase_artifacts.json`
- opcional: `planning_only.md` + `planning_only.json` (com `--planning-only`)
- opcional: `catalog.json`/`catalog.csv` (com `--generate-catalog`)
- opcional: `duplicates_report.csv` ou `duplicates.csv` (modos de deduplicação)

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

## Comportamento de segurança

- Preserva extensão original do arquivo.
- Evita sobrescrita com sufixos ` (2)`, ` (3)`, etc. quando necessário.
- Não sai da pasta de destino ao montar caminho final.
- Escrita atômica para cache JSON (`os.replace`).
- Sanitiza nome de arquivo para caracteres inválidos.
- Escape anti formula-injection em CSV (`=`, `+`, `-`, `@`, TAB, CR).
- Por padrão, roda em simulação.
- Falhas de fonte externa não derrubam o lote inteiro.
- Itens com falha externa + pontuação não automática podem ser marcados como `revisao_necessaria`.
- Quando limites operacionais estouram (custo/chamadas/tempo), o item cai para decisão conservadora local.
- Com `--quarantine`, arquivos originais e falhas ficam auditáveis por execução.

## Heurísticas bibliográficas (resumo)

- Parsing de autores com `;`, `&`, e separação cautelosa de `e`/`and`.
- Bloqueio de falso autor para ano puro, tokens editoriais e lixo de nome de arquivo.
- Tratamento de parênteses editoriais (ex.: `2nd edition`, `book club edition`, `Penguin Classics`) para evitar classificação como autor.
- Prioriza ano no sufixo `(YYYY)` do nome quando presente.
- Guardrails de merge:
  - bloqueio de autor remoto incompatível com autor local plausível
  - bloqueio de ano remoto outlier

## Fontes externas e tolerância a falhas

Fluxo remoto não usa `retry` com backoff; usa:
- timeout fixo de 20s por requisição HTTP
- pausa configurável entre requests (`--sleep` e perfis de velocidade)
- sessão HTTP compartilhada com `User-Agent: ebook-renamer/1.0`
- cache por URL+params
- classificação e registro de falhas por fonte:
  - `timeout`
  - `connection_error`
  - `rate_limit_reached_http_429`
  - `http_500/502/503/504_temporary_unavailable`
  - `invalid_json`
  - `missing_expected_field_*`
  - `source_error:*` quando a função da fonte falha

## Limitações conhecidas

- DOI não é atualmente extraído/validado como campo dedicado.
- Qualidade de snippets DDG varia por região, idioma e bloqueios anti-bot.
- Metadados embutidos de PDF/EPUB podem vir poluídos e causar ambiguidades.
- Em acervos heterogêneos, alguns casos devem ficar para revisão manual.

## Testes

Rodar suíte:

```bash
python -m pytest -q tests/
```

Lint:

```bash
flake8 renomear_ebooks.py tests/
```

Cobertura atual inclui parsing de nome, heurísticas de autores/títulos e tolerância a falhas externas com mocks.
