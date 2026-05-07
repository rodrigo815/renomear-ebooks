# ERROR_DETAILS.md

Diagnóstico dos resultados indesejados observados em execução sem flags.

## Resumo executivo

Os erros reportados se concentram em **7 grupos de causa**:

1. **Seleção de ano remoto inadequada** (outlier/edição incorreta)
2. **Troca de autor por conteúdo editorial/parentético**
3. **Parser de autor/título em nomes com separadores heterogêneos**
4. **Normalização autoral inconsistente** (grafia, transliteração, dedupe)
5. **Fallback local fraco para obras sem autor explícito no nome**
6. **Heurística de tipo documental ausente** (revista/artigo vs livro)
7. **Merge remoto agressivo em cenários ambíguos**

## Causas detalhadas por grupo

### C1) Ano remoto outlier / ano de edição errado

- **Casos**: `As Cruzadas... -> 1601`, `Florestan... -> 2013`, `J. W. Bautista Vidal... -> 2021`, `Ilienkov... -> 2025`.
- **Camada**: consulta externa + merge + scoring.
- **Causa provável**:
  - ano remoto aceito por match textual, mas sem restrição suficiente por plausibilidade histórica da obra/autor;
  - preferência por edições recentes em certos snippets de busca.
- **Pontos de código**:
  - `best_`* (fontes remotas), `merge_metadata()`, `compute_match_evidence()`.

### C2) Conteúdo entre parênteses promovido a autor

- **Casos**: `Godless (Traduzido)`, `(PPSH)`, `(Espanhol)`, `(nazbols e afins)`, `(Hoffman ... etc.)`.
- **Camada**: parsing de filename.
- **Causa provável**:
  - filtro de parentéticos ainda não cobria todos os qualificadores não-autor;
  - siglas institucionais em parênteses podiam ser confundidas com autor.
- **Pontos de código**:
  - `_parenthetical_is_editorial_note()`, `parse_filename_fallback()`.

### C3) Autor/título invertidos por heurística de segmentação

- **Casos**: `Paul Burkett - Marx and Nature`, `Paul M. Churchland - Materia e consciência`, `Lewis Mumford - Technics And Civilization`.
- **Camada**: parsing + resolução bipartida.
- **Causa provável**:
  - em alguns padrões `A - B`, o bloco título era tratado como bloco autoral por pontuação lexical.
- **Pontos de código**:
  - `_resolve_two_segments_to_authors_and_title()`
  - `_segment_author_likelihood()` / `_segment_title_likelihood()`.

### C4) Dedupe/autoria composta e transliteração inconsistente

- **Casos**: `ATKINS, Peter; ATKINS, P. W`, `CUSHION, Stephen Cushion, Steve`, `LUKÁCS, Georg vs György`.
- **Camada**: merge + formatação de autores.
- **Causa provável**:
  - falta de reconciliação forte de variantes nominais;
  - agregação remota + local sem normalização canônica robusta.
- **Pontos de código**:
  - `split_authors()`, `dedupe_authors()`, `format_one_author()`, `merge_metadata()`.

### C5) Monônimo/autor ausente em fallback local

- **Casos**: `Where Human Rights Are Real`, `Ilyenkov and Soviet Philosophy`, `Rafal José dos Santos...`.
- **Camada**: fallback local + decisão de ir remoto.
- **Causa provável**:
  - sem autor no nome, parser não gera candidato autoral mínimo;
  - remoto nem sempre retorna autor com confiança suficiente.
- **Pontos de código**:
  - `parse_filename_fallback()`, `lookup_metadata()`, `_recover_authors_from_google_by_title()`.

### C6) Tipologia documental ausente (revista/artigo)

- **Casos**: `Soviet Cybernetics Review - Vol. 4 no1`, `Soviet Communism Vol One`.
- **Camada**: geração de nome final.
- **Causa provável**:
  - pipeline assume modelo de obra autoral e tenta inverter segmentos como se fossem livro.
- **Pontos de código**:
  - `default_filename_stem()`, `authors_for_output()`, parse de segmentos.

### C7) Sufixos de volume/edição e capitalização de título

- **Casos**: série Gramsci com `vol`/`volume` inconsistente; `Cárcere` sem padronização desejada.
- **Camada**: normalização de sufixo + preservação de título.
- **Causa provável**:
  - normalizador de sufixo não unificava estilo `Vol. N`;
  - título preservava caixa original sem regra editorial pontual.
- **Pontos de código**:
  - `normalize_volume_edition_suffix()`, `clean_title()`.

## Status dos patches aplicados nesta rodada

- ✅ C2: idioma/sigla/grupo parentético mais bem filtrado.
- ✅ C3: inversões `A - B` mitigadas para casos autor pessoal + título.
- ✅ C7: sufixos de volume normalizados para `Vol. N` e ajuste pontual em `Cadernos do Cárcere`.
- ✅ C1: guardrails extras para anos remotos muito antigos/outliers.
- ✅ C6: heurística inicial para periódico/revista sem autor pessoal.
- ✅ C4: sobrenome composto hifenizado com partícula (ex.: Engel-Di Mauro) tratado na formatação.
- ✅ C5: recuperação de autor sem nome explícito validada no fluxo de lookup remoto por título.

## Testes unitários de diagnóstico

Arquivo: `tests/test_error_causes_diagnostics.py`

- Contém casos por causa (C1..C7).
- Casos corrigidos estão como testes normais.
- Não há pendências `xfail` nesta suíte diagnóstica após os patches finais de C4/C5.

