# renomear_ebooks.py

Script em Python para **renomear e organizar e-books** no padrão:

`SOBRENOME, Nome - Ano - Titulo.ext`

Suporta **EPUB**, **PDF**, **MOBI**, **AZW**, **AZW3** e **DJVU**.  
Os arquivos processados são **movidos/renomeados para a subpasta `renamed`** dentro da pasta que você passar como argumento (exceto se você já rodar apontando diretamente para uma pasta chamada `renamed`).

Por padrão, em **cada** pasta raiz informada, só entram arquivos **no nível imediato** dessa pasta. Use **`--recursive`** para incluir também todas as subpastas.

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
- **`--source all`** (padrão): tenta completar ano (e eventualmente autores) via **Open Library**, **Google Books**, **Wikipedia** e **fallback web** (busca textual; não substitui leitura direta de lojas).

Comportamento de performance:

- Se o **ano já foi encontrado na leitura local**, as fontes remotas são **puladas**, salvo se você usar **`--force-remote`**.

Estratégia de escolha entre vários anos candidatos:

- **`--year-strategy original`** (padrão): tende ao ano mais antigo plausível (aproxima “obra original”).
- **`--year-strategy edition`**: tende ao mais recente (reimpressão / edição).

Quando o ano continua desconhecido:

- **`--unknown-year sd`**: usa `s.d.` no nome.
- **`--unknown-year omit`**: omite o segmento de ano.

### Log só de itens sem ano

Gera um CSV com caminho original e **nome como ficaria com `s.d.`** forçado:

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
- Anos inferidos na rede podem ser **edição**, não “ano em que o texto foi escrito”; use `--year-strategy` e revise o CSV antes de `--apply`.
- No Windows, o console pode usar `cp1252`; caracteres muito exóticos podem aparecer substituídos na saída do terminal (o CSV continua em UTF-8 com BOM).
