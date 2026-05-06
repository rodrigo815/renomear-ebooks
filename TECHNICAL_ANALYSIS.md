# TECHNICAL_ANALYSIS.md

Analise tecnica aprofundada do estado atual de `renomear_ebooks.py`.

## 1. Visao geral

O script resolve um problema pratico de acervos digitais: nomes inconsistentes, metadados incompletos/poluídos e baixa confiabilidade de dados embutidos em PDF/EPUB. Ele padroniza nomes de e-books no formato bibliografico (`AUTOR - ANO - TITULO.ext` por padrao) com foco em:

- extracao local primeiro (nome do arquivo + metadados internos);
- enriquecimento remoto quando necessario;
- guardrails para reduzir falsos positivos;
- operacao segura por lote (dry-run padrao, CSVs de auditoria, tratamento de erros sem abortar o processo inteiro).

Formatos suportados no codigo: `.epub`, `.pdf`, `.mobi`, `.azw`, `.azw3`, `.djvu`.

## 2. Fluxo de execucao real

1. **Entrada e varredura**
   - `main()` recebe uma ou mais pastas (`PASTA`).
   - `iter_files()` lista arquivos suportados (com/sem `--recursive`), ignora `.html`, pastas `_files` e diretorios explicitamente ignorados.

2. **Leitura de metadados locais**
   - `build_local_metadata()` executa leitura local serial/paralela (`--jobs`).
   - `read_local_metadata()` combina:
     - `parse_filename_fallback()` (sempre);
     - `read_epub_metadata()` para EPUB;
     - `read_pdf_metadata()` para PDF.
   - Aplica regras de fallback para quando metadado interno e suspeito.

3. **Decisao sobre consulta externa**
   - Em `run_on_root()`, busca remota e pulada quando:
     - `--source offline`;
     - ano local ja existe e `--force-remote` nao foi usado;
     - nome ja estruturado em triplete (`AUTOR-ANO-TITULO` / `ANO-AUTOR-TITULO`) sem `--force-remote`.

4. **Consulta e merge remoto**
   - `lookup_metadata()` chama fontes em ordem:
     - Open Library -> Google Books -> Skoob (via DDG) -> catalogs (via DDG) -> Wikipedia -> fallback web.
   - `merge_metadata()` consolida local + remoto com regras de preferencia e guardrails.

5. **Scoring / confianca**
   - `compute_match_evidence()` calcula `match_score` (0-100) e evidencia textual.
   - Falhas de fonte externa reduzem score e confianca final.

6. **Nome final**
   - `make_new_filename()` (ou padrao via `default_filename_stem()`), preservando extensao.
   - `unique_target()` garante ausencia de sobrescrita e cria sufixos ` (2)`, ` (3)`, etc.

7. **Dry-run, apply, revisao**
   - Sem `--apply`: so planejamento (`rename_plan.csv`).
   - Com `--apply`: renomeacao/movimentacao real para `renamed/` (`rename_log.csv`).
   - Com `--review`: decisao interativa para casos abaixo da faixa automatica.

8. **Relatorios e cache**
   - Sempre grava cache (`metadata_cache.json`).
   - Pode gerar `missing_year_log`, `review_needed.csv`, `catalog.(json/csv)`.

## 3. Arquitetura do script

### 3.1 Funcoes nucleares e responsabilidades

- **CLI e orquestracao**
  - `main()`: parse de argumentos, validacoes de compatibilidade, modos especiais (dedup/duplicates), loop por raiz.
  - `run_on_root()`: pipeline principal por pasta.

- **Leitura local**
  - `parse_filename_fallback()`: parser heuristico do nome.
  - `read_epub_metadata()`: OPF/container XML.
  - `read_pdf_metadata()`: metadado PDF + extracao limitada de texto.
  - `read_local_metadata()`: merge local e fallback defensivo.

- **Consulta remota**
  - `get_json()`: cliente HTTP com cache, timeout, tratamento estruturado de falhas.
  - `best_openlibrary()`, `best_googlebooks()`, `best_wikipedia()`, `best_skoob_year()`, `best_book_catalogs_ddgs_year()`, `best_web_year()`.
  - `lookup_metadata()`: cascata de fontes + degradacao por falha.

- **Heuristica e normalizacao**
  - `split_authors()`, `_can_split_author_conjunction()`, `author_looks_bad()`.
  - `_segment_author_likelihood()`, `_segment_title_likelihood()`.
  - `_resolve_two_segments_to_authors_and_title()`.

- **Merge e confianca**
  - `merge_metadata()`, `compute_match_evidence()`, `_review_band()`.

- **Nomeacao e seguranca de arquivo**
  - `make_new_filename()`, `default_filename_stem()`, `format_authors()`.
  - `safe_filename_part()`, `unique_target()`.

### 3.2 Acoplamento

- O acoplamento principal e intencional: `run_on_root()` integra toda a cadeia de decisao.
- `lookup_metadata()` acopla politica de ordem de fontes e politica de tolerancia a falhas.
- `BookMeta` centraliza dados e reduz passagem de estruturas soltas.

### 3.3 Funcoes longas / responsabilidade mista

- `main()` e `run_on_root()` concentram muita logica (esperado para script unico, mas aumenta custo de manutencao).
- `parse_filename_fallback()` carrega regras de dominio extensas (alto valor, mas alta complexidade cognitiva).

### 3.4 Dependencias externas

- Runtime: `requests`, `rapidfuzz`, `pypdf`.
- Opcional seguranca XML: `defusedxml`.

### 3.5 Testabilidade

- Alta para funcoes puras de heuristica/parsing/scoring.
- Media para orquestracao com IO/rede; compensada com testes de mock (ex.: falhas externas).

## 4. Heuristicas bibliograficas

### 4.1 Autores multiplos e separadores

- `split_authors()` suporta `;`, `&`, `e`, `and`.
- Separacao de `e/and` e condicional (`_can_split_author_conjunction`) para evitar quebrar titulos.
- Mantem cautela quando ha virgulas (evita romper estruturas de titulo).

### 4.2 Titulo vs autor

- Usa scoring por segmento (`_segment_author_likelihood` vs `_segment_title_likelihood`).
- `_resolve_two_segments_to_authors_and_title()` decide lado autor/titulo com reforco para casos classicos e bloqueio de candidatos suspeitos.

### 4.3 Ano

- Ano extraido de varias fontes:
  - sufixo `(YYYY)` no nome;
  - tokens no nome;
  - datas de metadado;
  - candidatos remotos.
- `is_year_token()` evita que ano puro seja classificado como autor.
- Guardrail em `merge_metadata()` bloqueia ano remoto muito outlier.

### 4.4 ISBN / DOI

- ISBN: extraido e validado (`isbn10_valid`, `isbn13_valid`, `find_isbn`).
- DOI: nao ha pipeline dedicado de extracao/validacao/uso em consulta.

### 4.5 Metadados ruins e editoriais

- Bloqueios para termos editoriais e creditos de edicao/volume:
  - `_parenthetical_is_editorial_note()`
  - `_looks_like_volume_edition_credits()`
- Limpeza de ruido recorrente em nomes de arquivo:
  - ruido de mirrors (`z-library`, etc.);
  - IDs internos e sufixos tecnicos;
  - datas de vida catalograficas no autor (`_strip_catalog_author_life_span`).

### 4.6 Mononimos, acronimos e ambiguidades

- Mononimos plausiveis sao preservados em casos classicos (ex.: Marx, Lenin, Engels).
- Acronimos/instituicoes tratados em `format_one_author()` e filtros de autor ruim.
- Casos ambiguos sao empurrados para revisao por score/faixa e/ou `revisao_necessaria`.

## 5. Seguranca operacional

Checklist do comportamento atual:

- **Preserva extensao original:** sim.
- **Evita sobrescrever:** sim (`unique_target`).
- **Dry-run:** sim (padrao).
- **Auditoria de antigo/novo nome:** sim (CSV principal).
- **Conflitos de nome:** sim (sufixo incremental).
- **Erro sem travar lote:** sim (por arquivo e por fonte).
- **Evita auto rename em baixa confianca:** parcialmente via score/bandas + `revisao_necessaria`.
- **Revisao manual para ambiguos:** sim (`--review` + `review_needed.csv`).

Pontos adicionais:

- sanitizacao de caminho e caracteres invalidos;
- protecao anti formula injection em CSV;
- escrita atomica de cache JSON.

## 6. Chamadas externas

### 6.1 Fontes

- Open Library (JSON API)
- Google Books (JSON API)
- Wikipedia (search API)
- DuckDuckGo HTML (Skoob/catalgos/web fallback)

### 6.2 Politica tecnica

- Sessao HTTP compartilhada (`requests.Session`) com `User-Agent`.
- Timeout fixo de 20s por request.
- Cache por URL+params (`metadata_cache.json`).
- Sem retry/backoff automatico no cliente.
- Controle de ritmo por `--sleep`, `--fast`, `--thorough`, `--search-speed`.

### 6.3 Tolerancia a falhas

- Falhas sao classificadas (`timeout`, `connection_error`, `http_429`, `http_5xx`, `invalid_json`, etc.).
- Falhas entram em `source_failures` com `source`, `reason`, `action`.
- Pipeline continua para demais fontes/candidatos/arquivos.
- Falha externa reduz confianca e score; se decisao ficar fora da faixa segura, status vira `revisao_necessaria`.

### 6.4 Scraping HTML por padrao

- Nao esta globalmente desligado por padrao.
- Ele e usado quando as fontes habilitadas incluem Skoob/catalogs/web ou quando o fluxo chega nessas etapas.

## 7. Testes

### 7.1 Cobertura observada

- Suite atual valida casos de parsing de nome e regressao de heuristicas.
- Ha testes dedicados de tolerancia a falhas externas (`tests/test_external_failure_tolerance.py`).
- Cobertura boa de funcoes de decisao bibliografica sensiveis.

### 7.2 Bem testado

- Separacao autor/titulo em casos problematicos.
- Filtros de tokens editoriais/ano.
- Guardrails de merge.
- Continuidade de processamento diante de falha de fonte.

### 7.3 Lacunas recomendadas (antes de mudancas maiores)

- E2E de `run_on_root()` com cenarios completos de `revisao_necessaria`.
- Mais casos de conflitos de nome com multiplos destinos colidindo.
- Regressao para `--filename-pattern` com placeholders parcialmente vazios.
- Testes mais densos dos modos de duplicados (`--find-duplicates` / `--dedup`).

## 8. Qualidade do codigo

### Pontos fortes

- Dominio bem capturado em heuristicas concretas.
- Tratamento de erro estruturado (sem mascaramento silencioso no fluxo remoto).
- Boa instrumentacao de saida para auditoria operacional.
- Pipeline previsivel e orientado a seguranca.

### Debitos tecnicos controlados

- Arquivo unico grande (~4800 linhas) com alta densidade de regras.
- `main()` e `run_on_root()` com escopo amplo (parse+validacao+execucao+IO).
- Complexidade inerente de NLP heuristico aumenta chance de regressao em bordas.

### Sinais de overengineering / AI aesthetics

- Nao ha sinais fortes de overengineering arquitetural (nao criou camadas artificiais).
- Ha volume alto de heuristicas incrementais, esperado pelo problema.
- Comentarios em geral sao funcionais; poucos comentarios redundantes.

## 9. Riscos e limitacoes

### 9.1 Riscos de renomeacao errada

- Metadado remoto parcialmente correto (ano certo, autor errado) em fontes ruidosas.
- Titulos com subtitulos longos ou estruturas nao padrao.
- Arquivos com nome originalmente ruim, sem ISBN e sem metadado embutido confiavel.

### 9.2 Fontes de falso autor

- creditos editoriais/traducao;
- palavras de interface/sistema;
- tokens curtos ou numericos;
- termos capturados de snippets (DDG) sem contexto completo.

### 9.3 Fontes de falso titulo

- contaminacao por autor embutido no campo titulo;
- slugs/IDs internos de arquivos convertidos;
- quebra agressiva de delimitadores em nomes antigos.

### 9.4 Problemas com ano

- diferenca entre ano da obra vs ano da edicao;
- anos em contexto historico no corpo do texto;
- outliers remotos em catalogos de baixa qualidade.

### 9.5 Edicoes / traducoes

- alguns acervos exigem preservar marcadores editoriais no titulo por politica local;
- heuristicas de limpeza podem variar conforme preferencia bibliografica do usuario.

### 9.6 Quando preferir revisao manual

- score fora da faixa automatica;
- divergencia forte autor local x remoto;
- conflito entre multiplas fontes remotas com baixa convergencia;
- item com `source_failures` relevantes e decisao nao deterministica.

## Conclusao

O script esta tecnicamente maduro para uso em lote com risco operacional controlado, especialmente pelo desenho de dry-run, score, guardrails e tolerancia a falhas externas. O principal limite atual nao e estabilidade do processo, e sim a natureza ambigua dos dados bibliograficos em casos de borda. Para evolucao segura, o melhor caminho e continuar com patches pequenos guiados por regressao, sem reescrita ampla.
