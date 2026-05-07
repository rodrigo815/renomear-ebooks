# TECHNICAL_ANALYSIS.md

Análise técnica aprofundada do estado atual de `renomear_ebooks.py`.

## 1. Visão geral

O script resolve um problema prático de acervos digitais: nomes inconsistentes, metadados incompletos/poluídos e baixa confiabilidade de dados embutidos em PDF/EPUB. Ele padroniza nomes de e-books no formato bibliográfico (`AUTOR - ANO - TITULO.ext` por padrão) com foco em:

- extração local primeiro (nome do arquivo + metadados internos);
- enriquecimento remoto quando necessário;
- guardrails para reduzir falsos positivos;
- operação segura por lote (dry-run padrão, CSVs de auditoria, tratamento de erros sem abortar o processo inteiro).

Formatos suportados no codigo: `.epub`, `.pdf`, `.mobi`, `.azw`, `.azw3`, `.djvu`.

## 2. Fluxo de execução real

1. **Entrada e varredura**
   - `main()` recebe uma ou mais pastas (`PASTA`).
   - `iter_files()` lista arquivos suportados (com/sem `--recursive`), ignora `.html`, pastas `_files` e diretórios explicitamente ignorados.

2. **Leitura de metadados locais**
   - `build_local_metadata()` executa leitura local serial/paralela (`--jobs`).
   - `read_local_metadata()` combina:
     - `parse_filename_fallback()` (sempre);
     - `read_epub_metadata()` para EPUB;
     - `read_pdf_metadata()` para PDF.
   - Aplica regras de fallback para quando metadado interno e suspeito.

3. **Decisão sobre consulta externa**
   - Em `run_on_root()`, busca remota e pulada quando:
     - `--source offline`;
     - ano local já existe e `--force-remote` não foi usado;
     - nome já estruturado em triplete (`AUTOR-ANO-TITULO` / `ANO-AUTOR-TITULO`) sem `--force-remote`.

4. **Consulta e merge remoto**
   - `lookup_metadata()` chama fontes em ordem:
     - Open Library -> Google Books -> Skoob (via DDG) -> catalogs (via DDG) -> Wikipedia -> fallback web.
   - `merge_metadata()` consolida local + remoto com regras de preferência e guardrails.

5. **Scoring / confiança**
   - `compute_match_evidence()` calcula `match_score` (0-100) e evidência textual.
   - Falhas de fonte externa reduzem score e confiança final.

6. **Nome final**
   - `make_new_filename()` (ou padrão via `default_filename_stem()`), preservando extensão.
   - `unique_target()` garante ausência de sobrescrita e cria sufixos ` (2)`, ` (3)`, etc.

7. **Dry-run, apply, revisão**
   - Sem `--apply`: só planejamento (`rename_plan.csv`).
   - Com `--apply`: renomeação/movimentação real para `renamed/` (`rename_log.csv`).
   - Com `--review`: decisão interativa para casos abaixo da faixa automática.

8. **Relatórios e cache**
   - Sempre grava cache (`metadata_cache.json`).
   - Pode gerar `missing_year_log`, `review_needed.csv`, `catalog.(json/csv)`.

## 3. Arquitetura do script

### 3.1 Funções nucleares e responsabilidades

- **CLI e orquestração**
  - `main()`: parse de argumentos, validações de compatibilidade, modos especiais (dedup/duplicates), loop por raiz.
  - `run_on_root()`: pipeline principal por pasta.

- **Leitura local**
  - `parse_filename_fallback()`: parser heurístico do nome.
  - `read_epub_metadata()`: OPF/container XML.
  - `read_pdf_metadata()`: metadado PDF + extração limitada de texto.
  - `read_local_metadata()`: merge local e fallback defensivo.

- **Consulta remota**
  - `get_json()`: cliente HTTP com cache, timeout, tratamento estruturado de falhas.
  - `best_openlibrary()`, `best_googlebooks()`, `best_wikipedia()`, `best_skoob_year()`, `best_book_catalogs_ddgs_year()`, `best_web_year()`.
  - `lookup_metadata()`: cascata de fontes + degradação por falha.

- **Heurística e normalização**
  - `split_authors()`, `_can_split_author_conjunction()`, `author_looks_bad()`.
  - `_segment_author_likelihood()`, `_segment_title_likelihood()`.
  - `_resolve_two_segments_to_authors_and_title()`.

- **Merge e confiança**
  - `merge_metadata()`, `compute_match_evidence()`, `_review_band()`.

- **Nomeação e seguranca de arquivo**
  - `make_new_filename()`, `default_filename_stem()`, `format_authors()`.
  - `safe_filename_part()`, `unique_target()`.

### 3.2 Acoplamento

- O acoplamento principal e intencional: `run_on_root()` integra toda a cadeia de decisão.
- `lookup_metadata()` acopla política de ordem de fontes e política de tolerância a falhas.
- `BookMeta` centraliza dados e reduz passagem de estruturas soltas.

### 3.3 Funções longas / responsabilidade mista

- `main()` e `run_on_root()` concentram muita lógica (esperado para script único, mas aumenta custo de manutenção).
- `parse_filename_fallback()` carrega regras de domínio extensas (alto valor, mas alta complexidade cognitiva).

### 3.4 Dependências externas

- Runtime: `requests`, `rapidfuzz`, `pypdf`.
- Opcional segurança XML: `defusedxml`.

### 3.5 Testabilidade

- Alta para funções puras de heurística/parsing/scoring.
- Média para orquestração com IO/rede; compensada com testes de mock (ex.: falhas externas).

## 4. Heurísticas bibliográficas

### 4.1 Autores múltiplos e separadores

- `split_authors()` suporta `;`, `&`, `e`, `and`.
- Separação de `e/and` e condicional (`_can_split_author_conjunction`) para evitar quebrar títulos.
- Mantém cautela quando há vírgulas (evita romper estruturas de título).

### 4.2 Título vs autor

- Usa scoring por segmento (`_segment_author_likelihood` vs `_segment_title_likelihood`).
- `_resolve_two_segments_to_authors_and_title()` decide lado autor/título com reforço para casos clássicos e bloqueio de candidatos suspeitos.

### 4.3 Ano

- Ano extraído de varias fontes:
  - sufixo `(YYYY)` no nome;
  - tokens no nome;
  - datas de metadado;
  - candidatos remotos.
- `is_year_token()` evita que ano puro seja classificado como autor.
- Guardrail em `merge_metadata()` bloqueia ano remoto muito outlier.

### 4.4 ISBN / DOI

- ISBN: extraido e validado (`isbn10_valid`, `isbn13_valid`, `find_isbn`).
- DOI: não há pipeline dedicado de extração/validação/uso em consulta.

### 4.5 Metadados ruins e editoriais

- Bloqueios para termos editoriais e créditos de edição/volume:
  - `_parenthetical_is_editorial_note()`
  - `_looks_like_volume_edition_credits()`
- Limpeza de ruído recorrente em nomes de arquivo:
  - ruído de mirrors (`z-library`, etc.);
  - IDs internos e sufixos técnicos;
  - datas de vida catalográficas no autor (`_strip_catalog_author_life_span`).

### 4.6 Monônimos, acrônimos e ambiguidades

- Monônimos plausíveis são preservados em casos clássicos (ex.: Marx, Lenin, Engels).
- Acrônimos/instituições tratados em `format_one_author()` e filtros de autor ruim.
- Casos ambíguos são empurrados para revisão por score/faixa e/ou `revisao_necessaria`.

## 5. Segurança operacional

Checklist do comportamento atual:

- **Preserva extensão original:** sim.
- **Evita sobrescrever:** sim (`unique_target`).
- **Dry-run:** sim (padrão).
- **Auditoria de antigo/novo nome:** sim (CSV principal).
- **Conflitos de nome:** sim (sufixo incremental).
- **Erro sem travar lote:** sim (por arquivo e por fonte).
- **Evita auto rename em baixa confiança:** parcialmente via score/bandas + `revisao_necessaria`.
- **Revisão manual para ambíguos:** sim (`--review` + `review_needed.csv`).

Pontos adicionais:

- sanitização de caminho e caracteres inválidos;
- proteção anti formula injection em CSV;
- escrita atômica de cache JSON.

## 6. Chamadas externas

### 6.1 Fontes

- Open Library (JSON API)
- Google Books (JSON API)
- Wikipedia (search API)
- DuckDuckGo HTML (Skoob/catalogs/web fallback)

### 6.2 Política técnica

- Sessão HTTP compartilhada (`requests.Session`) com `User-Agent`.
- Timeout fixo de 20s por request.
- Cache por URL+params (`metadata_cache.json`).
- Sem retry/backoff automático no cliente.
- Controle de ritmo por `--sleep`, `--fast`, `--thorough`, `--search-speed`.

### 6.3 Tolerância a falhas

- Falhas são classificadas (`timeout`, `connection_error`, `http_429`, `http_5xx`, `invalid_json`, etc.).
- Falhas entram em `source_failures` com `source`, `reason`, `action`.
- Pipeline continua para demais fontes/candidatos/arquivos.
- Falha externa reduz confiança e score; se decisão ficar fora da faixa segura, status vira `revisao_necessaria`.

### 6.4 Scraping HTML por padrão

- Não está globalmente desligado por padrão.
- Ele é usado quando as fontes habilitadas incluem Skoob/catalogs/web ou quando o fluxo chega nessas etapas.

## 7. Testes

### 7.1 Cobertura observada

- Suíte atual valida casos de parsing de nome e regressão de heurísticas.
- Há testes dedicados de tolerância a falhas externas (`tests/test_external_failure_tolerance.py`).
- Cobertura boa de funções de decisão bibliográfica sensiveis.

### 7.2 Bem testado

- Separação autor/titulo em casos problemáticos.
- Filtros de tokens editoriais/ano.
- Guardrails de merge.
- Continuidade de processamento diante de falha de fonte.

### 7.3 Lacunas recomendadas (antes de mudanças maiores)

- E2E de `run_on_root()` com cenários completos de `revisao_necessaria`.
- Mais casos de conflitos de nome com múltiplos destinos colidindo.
- Regressão para `--filename-pattern` com placeholders parcialmente vazios.
- Testes mais densos dos modos de duplicados (`--find-duplicates` / `--dedup`).

## 8. Qualidade do código

### Pontos fortes

- Domínio bem capturado em heurísticas concretas.
- Tratamento de erro estruturado (sem mascaramento silencioso no fluxo remoto).
- Boa instrumentação de saida para auditoria operacional.
- Pipeline previsível e orientado a segurança.

### Débitos técnicos controlados

- Arquivo único grande (~4800 linhas) com alta densidade de regras.
- `main()` e `run_on_root()` com escopo amplo (parse+validação+execução+IO).
- Complexidade inerente de NLP heurístico aumenta chance de regressão em bordas.

### Sinais de overengineering / AI aesthetics

- Não há sinais fortes de overengineering arquitetural (não criou camadas artificiais).
- Há volume alto de heurísticas incrementais, esperado pelo problema.
- Comentários em geral são funcionais; poucos comentários redundantes.

## 9. Riscos e limitações

### 9.1 Riscos de renomeação errada

- Metadado remoto parcialmente correto (ano certo, autor errado) em fontes ruidosas.
- Títulos com subtítulos longos ou estruturas não padrão.
- Arquivos com nome originalmente ruim, sem ISBN e sem metadado embutido confiável.

### 9.2 Fontes de falso autor

- créditos editoriais/tradução;
- palavras de interface/sistema;
- tokens curtos ou numéricos;
- termos capturados de snippets (DDG) sem contexto completo.

### 9.3 Fontes de falso título

- contaminação por autor embutido no campo título;
- slugs/IDs internos de arquivos convertidos;
- quebra agressiva de delimitadores em nomes antigos.

### 9.4 Problemas com ano

- diferença entre ano da obra vs ano da edição;
- anos em contexto histórico no corpo do texto;
- outliers remotos em catálogos de baixa qualidade.

### 9.5 Edições / traduções

- alguns acervos exigem preservar marcadores editoriais no título por política local;
- heurísticas de limpeza podem variar conforme preferência bibliográfica do usuário.

### 9.6 Quando preferir revisão manual

- score fora da faixa automática;
- divergência forte autor local x remoto;
- conflito entre múltiplas fontes remotas com baixa convergência;
- item com `source_failures` relevantes e decisão não determinística.

## Conclusão

O script está tecnicamente maduro para uso em lote com risco operacional controlado, especialmente pelo desenho de dry-run, score, guardrails e tolerância a falhas externas. O principal limite atual não é estabilidade do processo, e sim a natureza ambígua dos dados bibliográficos em casos de borda. Para evolução segura, o melhor caminho é continuar com patches pequenos guiados por regressão, sem reescrita ampla.
