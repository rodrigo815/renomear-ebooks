# renomear_ebooks.py

Script em Python para **renomear e organizar e-books** no padrão:

`SOBRENOME, Nome - Ano - Titulo.ext`

Suporta **EPUB**, **PDF**, **MOBI**, **AZW**, **AZW3** e **DJVU**.
Os arquivos processados são **movidos/renomeados para a subpasta `renamed`** dentro da pasta que você passar como argumento (exceto se você já rodar apontando diretamente para uma pasta chamada `renamed`).

Por padrão, em **cada** pasta raiz informada, só entram arquivos **no nível imediato** dessa pasta. Use **`--recursive`** para incluir também todas as subpastas.

### Filtrar por extensão (`--exts`)

Só processa as extensões listadas (vírgula; com ou sem `.`; maiúsculas ou minúsculas). Devem ser tipos **suportados** pelo script; outras são ignoradas com aviso no stderr.

```bash
python renomear_ebooks.py "E:\Livros" --exts "pdf,EPUB,.mobi" --recursive
```

Sem `--exts`, o comportamento padrão é o conjunto completo de tipos suportados (e `.html` continua de fora).

### Velocidade vs consistência (`--fast` / `--thorough`)

São **mutuamente exclusivos**.

| Flag | Efeito principal |
|------|------------------|
| **`--fast`** | Pausa HTTP ~**0 s**; no máximo **1** página de PDF por ficheiro; com `--source all` só **Open Library + Google Books** (sem Skoob, catalogs agregados, Wikipedia, fallback web nem enriquecimento extra de autores por snippet). |
| **`--thorough`** | Pausa HTTP ≥ **0,35 s**; **5–15** páginas de PDF; com `--source all` usa **todas** as fontes. **Não** liga por si a ida à rede quando já há ano local — use **`--force-remote`** / **`--fetch-remote-always`**. |

Exemplo:

```bash
python renomear_ebooks.py "E:\Livros" --source all --fast --jobs 6 --quiet
```

### Filtrar fontes (`--sources`)

Com **`--source all`**, lista separada por vírgulas quais backends remotos rodar (ordem interna fixa):

`openlibrary`, `google`, `skoob`, `catalogs`, `wikipedia`, `web`

```bash
python renomear_ebooks.py "E:\Livros" --source all --sources openlibrary,google,wikipedia --quiet
```

Tem precedência sobre o subconjunto definido por **`--search-speed`** e sobre o conjunto implícito de **`--fast`**. **`--fast`** continua a definir pausa HTTP, páginas de PDF e se há enriquecimento extra de autores; use **`--search-speed`** em vez de **`--fast`** se quiser afinar só a lista de fontes sem esse perfil “turbo”.

### Velocidade de busca (`--search-speed` 1–5)

**Mutuamente exclusivo** com **`--fast`** e **`--thorough`** (use um dos três).

| N | Fontes remotas (resumo) | Pausa / PDF / enriquecimento de autores |
|---|-------------------------|----------------------------------------|
| **1** | Todas (OL → … → web) | Pausa ≥ 0,35 s; ≥ 5 páginas PDF (até 15); enriquece autores por DDG se preciso (não ativa `--force-remote`) |
| **2** | Até Wikipedia (sem web) | Pausa ≥ 0,22 s; `--max-pdf-pages`; enriquece |
| **3** | Até catalogs agregados | Pausa ≥ 0,15 s; até 3 páginas PDF; sem enriquecimento |
| **4** | Até Skoob | Pausa ≥ 0,08 s; até 2 páginas PDF; sem enriquecimento |
| **5** | Só Open Library + Google Books | Pausa 0 s; 1 página PDF; sem enriquecimento (parecido ao `--fast` sem `--sources`) |

```bash
python renomear_ebooks.py "E:\Livros" --source all --search-speed 2 --quiet
```

---

## Requisitos

- Python 3.10+ recomendado.
- Pacotes Python (instale na venv ou globalmente):

```bash
pip install requests rapidfuzz pypdf
```

`pypdf` é opcional para PDFs sem metadado embutido útil; sem ele, o script usa fallback pelo nome do arquivo.

---

## Uso rápido

**Simulação** (não altera arquivos; gera plano em CSV):

```bash
python renomear_ebooks.py "E:\Livros"
```

**Várias pastas** (cada uma com seu próprio `renamed/`):

```bash
python renomear_ebooks.py "E:\Livros" "D:\OutraPasta" --recursive
```

**Aplicar** renomeações de verdade:

```bash
python renomear_ebooks.py "E:\Livros" --apply
```

No Windows, se `python` não estiver no PATH:

```bash
py -3 renomear_ebooks.py "E:\Livros" --apply
```

---

## Padrão do nome do ficheiro (`--filename-pattern`)

Por omissão o script usa o formato clássico **`SOBRENOME, Nome - Ano - Título.ext`**.

Com **`--filename-pattern`** pode definir um modelo com marcadores (maiúsculas ou minúsculas):

| Marcador | Conteúdo |
|----------|-----------|
| **`%AUTHOR%`** | Autores formatados (com `et al.` se `--max-authors` limitar) |
| **`%DATE%`** | Ano identificado; se faltar, usa **`--unknown-year-text`** com `--unknown-year sd`, ou vazio com `omit` |
| **`%TITLE%`** | Título |
| **`%PUBLISHER%`** | Editora (ex.: EPUB `dc:publisher`, Google Books quando existir; pode ficar vazio) |
| **`%FORMAT%`** | Extensão **com** ponto (ex.: `.pdf`) |

Se o modelo **não** incluir `%FORMAT%`, a extensão correcta é acrescentada no fim. Caracteres inválidos em nomes de ficheiro são normalizados como no modo padrão.

```bash
python renomear_ebooks.py "E:\Livros" --filename-pattern "%DATE%_%AUTHOR% - %TITLE%%FORMAT%"
```

---

## Onde ficam os resultados

Dado `PASTA` = pasta da biblioteca (ex.: `E:\Livros`). Com **várias** pastas na linha de comando, o mesmo esquema vale **para cada** `PASTA`.

| Caminho | Conteúdo |
|---------|----------|
| `PASTA\renamed\` | Arquivos renomeados |
| `PASTA\renamed\rename_plan.csv` | Simulação: original → novo |
| `PASTA\renamed\rename_log.csv` | Com `--apply`: log das operações |
| `PASTA\renamed\metadata_cache.json` | Cache de respostas HTTP |

---

## Overrides de autor (`author_overrides.json`)

Na **raiz da pasta-alvo** (por padrão `PASTA\author_overrides.json`), use um JSON de mapeamento:

- **Chave**: texto como aparece no metadado ou no nome do arquivo (comparação normalizada).
- **Valor**: como você quer no nome final (já no estilo `SOBRENOME, Nome`).

Exemplo:

```json
{
  "Bassi": "BASSI, A.",
  "Ghirardi": "GHIRARDI, G.C."
}
```

Outro caminho:

```bash
python renomear_ebooks.py "E:\Livros" --overrides "E:\meus_overrides.json"
```

---

## Ano e fontes remotas

- **`--source offline`**: só metadado local (arquivo + leitura leve de PDF/EPUB).
- **`--source all`** (padrão): tenta completar ano (e eventualmente autores) via **Open Library**, **Google Books**, **[Skoob](https://www.skoob.com.br/)** (indireto: `site:skoob.com.br` no DuckDuckGo), **catalogs** (vários `site:` numa só leva de buscas DDG — ver abaixo), **Wikipedia** e **fallback web** (busca textual; não substitui leitura direta de lojas).
- **`--source skoob`**: só a heurística Skoob (útil para testar ou acervos em português).
- **`--source catalogs`**: só a heurística agregada em catálogos via DuckDuckGo (útil para testar).

#### Catálogos cobertos por `--source catalogs` / etapa em `all`

Sem chaves de API: o script usa o **DuckDuckGo HTML** com filtros `site:` agrupados (3 pedidos HTTP por livro nesta etapa). Domínios alinhados aos sites que indicaste:

| Catálogo | URL de referência |
|----------|-------------------|
| WorldCat | [search.worldcat.org](https://search.worldcat.org/) (`site:worldcat.org`) |
| Goodreads | [goodreads.com](https://www.goodreads.com/) |
| The StoryGraph | [thestorygraph.com](https://thestorygraph.com/) |
| LibraryThing | [librarything.com](https://www.librarything.com/) |
| BookBrowse | [bookbrowse.com](https://www.bookbrowse.com/) |
| BookBrainz | [bookbrainz.org](https://bookbrainz.org/) |
| Amazon Books | [amazon.com/books](https://www.amazon.com/Books/s?srs=17276798011&rh=n%3A283155) (`site:amazon.com`) |
| ISBNdb | [isbndb.com](https://isbndb.com/) |

**Nota:** são heurísticas em **snippets** de motor de busca; precisão e disponibilidade dependem do indexador e do DDG (incl. bloqueios anti-bot), como no Skoob e no fallback web.

Comportamento de performance:

- Se o **ano já foi encontrado na leitura local**, a fase de **busca na rede** é **omitida**, salvo **`--force-remote`** ou **`--fetch-remote-always`** (mesma opção, dois nomes).

### O que fundir do remoto (`--remote-metadata` / `--keep-local-metadata`)

Depois de uma busca remota bem-sucedida, o script combina metadado local + remoto:

- **`--remote-metadata`** (lista CSV): campos que **podem** ser preenchidos ou sobrescritos a partir do remoto. Valores: `title`, `authors`, `year`, `isbn`, `publisher` (aliases: `date`, `ano`, `author`, `titulo`, `editora`, …). **Omitir a flag** = todos os campos podem receber dados remotos.
- **`--keep-local-metadata`** (lista CSV): campos em que **manter o local** quando já existir valor; o remoto não substitui (se o local estiver vazio, usa-se o remoto).

Exemplo: ir sempre à rede, mas no ficheiro só atualizar **ano** e **ISBN**, mantendo **autor** e **título** do ficheiro:

```bash
python renomear_ebooks.py "E:\Livros" --source all --force-remote --remote-metadata year,isbn --keep-local-metadata authors,title
```

Estratégia de escolha entre vários anos candidatos:

- **`--year-strategy original`** (padrão): tende ao ano mais antigo plausível (aproxima “obra original”).
- **`--year-strategy edition`**: tende ao mais recente (reimpressão / edição).

Quando o ano continua desconhecido:

- **`--unknown-year sd`**: insere um **placeholder** no lugar do ano (por omissão `s.d.`).
- **`--unknown-year-text TEXTO`**: texto desse placeholder (ex.: `ND`, `sem data`, `????`); caracteres inválidos para nome de ficheiro são normalizados. Se ficar vazio, volta a `s.d.`. Só vale com `sd`.
- **`--unknown-year omit`**: omite o segmento de ano quando não há data (fica `AUTOR - Título`; com ano, `AUTOR - Ano - Título`).
- **`--omit-date-if-missing`**: atalho para o mesmo efeito que **`omit`** (útil se já usares `sd` noutro script e quiseres esta flag explícita). Se passares `sd` e esta flag, prevalece `omit` (aviso no stderr).

### Log só de itens sem ano

Gera um CSV com caminho original e **nome como ficaria com o placeholder de ano** (modo `sd`, texto em **`--unknown-year-text`**) forçado:

```bash
python renomear_ebooks.py "E:\Livros" --source all --missing-year-log
```

Ou com nome explícito:

```bash
python renomear_ebooks.py "E:\Livros" --missing-year-log "E:\Livros\renamed\sem_data.csv"
```

---

## Pastas e arquivos ignorados na varredura

O script **não processa**:

- arquivos `.html`;
- qualquer pasta cujo nome termine em `_files`;
- pastas cujos nomes (em minúsculas) estejam em `IGNORED_DIR_NAMES` no código (por padrão **vazio**, para não obrigar commits com nomes locais). Ajuste só na sua cópia se precisar.

---

## Desempenho (lotes grandes)

| Flag | Efeito |
|------|--------|
| `--limit N` | Processa só os N primeiros arquivos (teste rápido). |
| `--jobs N` | Threads para leitura local de metadados (PDF/EPUB). |
| `--only-missing-year` | Só entradas sem ano **após** leitura local (útil com `--source all`). |
| `--quiet` | Não imprime linha a linha no console. |
| `--sleep SEG` | Intervalo entre requisições HTTP. |

Exemplo “turbo” de simulação em parte do acervo:

```bash
python renomear_ebooks.py "E:\Livros" --source all --limit 200 --jobs 6 --quiet
```

---

## Ajuda integrada (`--help`)

O texto de ajuda foi enriquecido (descrição, defaults e exemplos). Rode:

```bash
python renomear_ebooks.py --help
```

---

## Avisos

- Metadado de PDFs piratas/escaneados é frequentemente **ruim**; o script tenta priorizar o **nome do arquivo** quando isso acontece.
- A pista **Skoob** passa pelo DuckDuckGo (`site:skoob.com.br`), como o fallback web: em alguns IPs o DDG pode devolver página de desafio em vez de resultados.
- Anos inferidos na rede podem ser **edição**, não “ano em que o texto foi escrito”; use `--year-strategy` e revise o CSV antes de `--apply`.
- No Windows, o console pode usar `cp1252`; caracteres muito exóticos podem aparecer substituídos na saída do terminal (o CSV continua em UTF-8 com BOM).
