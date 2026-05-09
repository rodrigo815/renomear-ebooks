from __future__ import annotations

import argparse
import concurrent.futures
import csv
import difflib
import os
import shutil
from collections import defaultdict
import hashlib
import json
import logging
import re
import sys
import tempfile
import time
import urllib.parse
import zipfile
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from rapidfuzz import fuzz

try:
    import defusedxml.ElementTree as ET  # type: ignore[import-not-found]
    _HAS_DEFUSED_XML = True
except ImportError:
    import xml.etree.ElementTree as ET  # type: ignore[no-redef]
    _HAS_DEFUSED_XML = False

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


_HTTP_SESSION: requests.Session | None = None
_AUTHOR_CITATION_CACHE: dict[str, tuple[str, str] | None] = {}


def _get_http_session() -> requests.Session:
    """Sessao global com keep-alive; evita reabrir conexao a cada GET."""
    global _HTTP_SESSION
    if _HTTP_SESSION is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8, pool_maxsize=16, max_retries=0
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": "ebook-renamer/1.0"})
        _HTTP_SESSION = s
    return _HTTP_SESSION


def _resolved_path(p: Path, _cache: dict[Path, Path] = {}) -> Path:
    """resolve() em cache por instancia (poupa syscalls em loops)."""
    rp = _cache.get(p)
    if rp is None:
        try:
            rp = p.resolve()
        except OSError:
            rp = p.absolute()
        _cache[p] = rp
    return rp


_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")


def _csv_safe(value: Any) -> str:
    """Anti formula-injection: prefixa "'" em celulas que abrem com =, +, -, @, TAB, CR."""
    if value is None:
        return ""
    s = str(value)
    if not s:
        return s
    first = s[0]
    if first in _CSV_FORMULA_PREFIXES or first in ("\t", "\r"):
        return "'" + s
    return s


def _csv_safe_row(row: dict[str, Any]) -> dict[str, str]:
    return {k: _csv_safe(v) for k, v in row.items()}


_WINDOWS_RESERVED_NAMES = frozenset({
    "con", "prn", "aux", "nul",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
})


SUPPORTED_EXTS = frozenset({".epub", ".pdf", ".mobi", ".azw", ".azw3", ".djvu"})

_LOG_MESES_PT = (
    "jan",
    "fev",
    "mar",
    "abr",
    "mai",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dez",
)
_LOG_STRICT: dict[str, bool] = {"omit_console": False}


def log_set_omit_console(value: bool) -> None:
    _LOG_STRICT["omit_console"] = bool(value)


def _log_timestamp() -> str:
    now = datetime.now()
    ms = now.microsecond // 1000
    return (
        f"{now.day:02d} {_LOG_MESES_PT[now.month - 1]} {now.year} "
        f"{now.hour:02d}:{now.minute:02d}:{now.second:02d}.{ms:03d}"
    )


def _print_console(stream, line: str) -> None:
    try:
        print(line, file=stream, flush=True)
    except UnicodeEncodeError:
        enc = getattr(stream, "encoding", None) or "utf-8"
        safe = line.encode(enc, errors="replace").decode(enc, errors="replace")
        print(safe, file=stream, flush=True)


def _log_emit(level: str, msg: str, stream) -> None:
    if _LOG_STRICT["omit_console"] and level != "FATAL":
        return
    _print_console(stream, f"{_log_timestamp()} [{level}] {msg}")


def log_info(msg: str) -> None:
    _log_emit("INFO", msg, sys.stdout)


def log_warn(msg: str) -> None:
    _log_emit("WARN", msg, sys.stderr)


def log_error(msg: str) -> None:
    _log_emit("ERROR", msg, sys.stderr)


def log_fatal(msg: str) -> None:
    _log_emit("FATAL", msg, sys.stderr)


def parse_exts_csv(raw: str) -> frozenset[str]:
    """Lista separada por virgula: aceita com ou sem ponto; normaliza para minusculas com ponto."""
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    if not parts:
        raise ValueError("lista de extensoes vazia")
    out: set[str] = set()
    for p in parts:
        e = p.lower()
        if not e.startswith("."):
            e = "." + e
        out.add(e)
    unknown = out - SUPPORTED_EXTS
    if unknown:
        unk = ", ".join(sorted(unknown))
        log_warn(f"Extensoes ignoradas (nao suportadas): {unk}")
    allowed = frozenset(out & SUPPORTED_EXTS)
    if not allowed:
        raise ValueError(
            "nenhuma extensao valida apos filtrar; suportadas: "
            + ", ".join(sorted(SUPPORTED_EXTS))
        )
    return allowed


# Nomes de pastas (em minusculas) a ignorar na varredura; vazio = nenhuma.
# Ex.: frozenset({"anarquismo"}) — use so localmente se precisar, sem commitar.
IGNORED_DIR_NAMES: frozenset[str] = frozenset()

PARTICLES = {
    "da", "de", "do", "das", "dos", "di", "del", "della", "du",
    "van", "von", "der", "den", "ter", "ten", "la", "le", "el", "al",
}

INSTITUTION_WORDS = {
    "república", "estado", "ministério", "universidade", "organização",
    "partido", "comitê", "departamento", "instituto", "fundação",
    "secretaria", "governo", "academia", "rpd", "onu", "unesco",
}

STOP_TITLE_WORDS = {
    "pdf", "epub", "mobi", "azw", "azw3", "djvu", "livro", "ebook",
    "scanned", "scan", "ocr", "converted", "zlib", "libgen",
}

BAD_AUTHOR_WORDS = {
    "administrator", "administrador", "admin", "user", "owner", "unknown",
    "scanner", "scan", "converter", "convertido", "icecream", "pdf", "acrobat",
    "adobe", "microsoft", "word", "utilizador", "usuario",
    "traduzido", "traduzida", "translator", "translation",
    "etc", "etc.",
}

BAD_TITLE_WORDS = {
    "unknown", "untitled", "document", "scan", "scanner", "converted",
}

# Sobrenomes/cognomes frequentes em ficheiros "Marx - Titulo.pdf" / "Gramsci - Titulo.pdf";
# usado em heuristicas de bipartido e parentese final (editora vs coautor).
AUTHOR_ONE_WORD_SURNAMES = frozenset(
    {
        "marx",
        "engels",
        "lenin",
        "darwin",
        "nietzsche",
        "gramsci",
        "freud",
        "kant",
        "hegel",
        "sartre",
        "weber",
        "arendt",
    }
)


@dataclass
class BookMeta:
    path: str
    title: str = ""
    authors: list[str] | None = None
    year: str = ""
    isbn: str = ""
    publisher: str = ""
    series: str = ""
    subjects: list[str] | None = None
    source: str = ""
    confidence: float = 0.0
    notes: str = ""
    match_score: int = 0
    evidence: dict[str, str] = field(default_factory=dict)
    source_failures: list[dict[str, str]] = field(default_factory=list)
    # Sufixo de volume/edição extraído do nome (ex.: "Vol.1 3ªed") — sempre no fim do stem.
    filename_extra_suffix: str = ""
    # Ano extraido de sufixo (AAAA) no fim do stem; prevalece sobre remoto no merge.
    filename_paren_year: bool = False

    def __post_init__(self) -> None:
        if self.authors is None:
            self.authors = []
        if self.subjects is None:
            self.subjects = []


def compact_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def normalize_for_match(s: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    words = [w for w in s.split() if w not in STOP_TITLE_WORDS]
    return " ".join(words)


def strip_accents(s: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFKD", s or "")
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def clean_title(s: str) -> str:
    s = compact_spaces(s)
    s = s.strip(" .-_")
    s = re.sub(r"(?i)\bcadernos do cárcere\b", "Cadernos do Cárcere", s)
    return s


def append_note(meta: BookMeta, note: str) -> None:
    note = compact_spaces(note)
    if not note:
        return
    if not meta.notes:
        meta.notes = note
    elif note not in meta.notes:
        meta.notes += f" | {note}"


def is_year_token(s: str) -> bool:
    t = compact_spaces(s)
    return bool(re.fullmatch(r"(?:1[4-9]\d{2}|20\d{2})", t))


def author_looks_bad(author: str) -> bool:
    a = compact_spaces(author)
    if not a:
        return True
    if is_year_token(a):
        return True
    if _looks_like_volume_edition_credits(a):
        return True
    if re.search(
        r"z-library|z-lib\.|1lib\.sk|singlelogin|librarylol|b-ok\.|zlib\.",
        a,
        re.I,
    ):
        return True
    n = normalize_for_match(a)
    words = set(n.split())
    if words & BAD_AUTHOR_WORDS:
        return True
    if re.search(r"\.(pdf|docx?|txt|rtf)\b", a, re.I):
        return True
    if re.search(r"\bp\d+\b", a, re.I):
        return True
    if re.fullmatch(r"[A-Z0-9]{6,}", a):
        return True
    toks = a.split()
    if (
        len(toks) >= 6
        and toks[0].lower().strip(".'’") in {"o", "a", "os", "as", "the", "el", "la", "los", "las"}
        and re.search(r"\b(?:da|de|do|dos|das|the|of|and|e)\b", a, re.I)
    ):
        return True
    return False


def title_looks_bad(title: str) -> bool:
    t = clean_title(title)
    if not t:
        return True
    n = normalize_for_match(t)
    words = set(n.split())
    if words & BAD_TITLE_WORDS:
        return True
    if re.search(r"\.(pdf|docx?|txt|rtf)\b", t, re.I):
        return True
    return False


def _looks_like_internal_id_title(s: str) -> bool:
    """Titulo que parece UUID, hash MD5/SHA hex, ou slug interno (ex.: metadado EPUB corrupto)."""
    t = compact_spaces(s)
    if not t or len(t) < 12:
        return False
    tl = re.sub(r"[\s\-_]", "", t.lower())
    if re.fullmatch(r"[0-9a-f]{24,}", tl):
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}[0-9a-f]{4}[0-9a-f]{4}[0-9a-f]{4}[0-9a-f]{12}",
        tl,
    ):
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        t.lower(),
    ):
        return True
    alnum = re.sub(r"[^0-9a-f]", "", tl)
    if len(alnum) >= 18 and len(tl) >= 18 and len(alnum) / len(tl) >= 0.82:
        if not re.search(r"[a-zà-ÿ]{4,}", t.lower()):
            return True
    return False


def _normalize_filename_hyphens(s: str) -> str:
    """Travessao/en-dash e similares -> separador forte ' - ' (evita colar AUTOR-TITULO e ligar ao hifen do sobrenome composto ASCII)."""
    s = compact_spaces(s)
    s = re.sub(r"\s*[\u2013\u2014\u2212\u2010\u2011]\s*", " - ", s)
    return compact_spaces(s)


def _underscore_subtitle_as_colon(s: str) -> str:
    """Underscore + espaco costuma substituir ':' ilegal no nome do ficheiro (subtitulo)."""
    s = compact_spaces(s)
    return compact_spaces(re.sub(r"(?<=[^\s_])_\s+(?=\S)", ": ", s))


_PORTAL_SUFFIX_RE = re.compile(
    r"\s*\([^)]*(?:z-library|z-lib\.|1lib\.sk|singlelogin|librarylol|b-ok\.|zlib\.)[^)]*\)",
    re.I,
)

_CIA_REGISTRY_TAIL_RE = re.compile(
    r"\s+-\s+CIA-RDP[0-9A-Z][0-9A-Z\-]{6,}\s*$",
    re.I,
)

_DOKUMEN_PREFIX_RE = re.compile(r"^\s*dokumen\.pub[_\-\s]+", re.I)
_ISBN_TAIL_RE = re.compile(r"(?:[-_\s]+(?:97[89]\d{10}|\d{10})){1,3}\s*$")


def _sanitize_filename_stem_noise(st: str) -> str:
    """Remove sufixos de portal (Z-Library etc.) e referencias CIA-RDP do stem antes do parse."""
    s = compact_spaces(st)
    s = _DOKUMEN_PREFIX_RE.sub("", s)
    while True:
        prev = s
        s = _PORTAL_SUFFIX_RE.sub("", s)
        s = compact_spaces(s)
        if s == prev:
            break
    s = _CIA_REGISTRY_TAIL_RE.sub("", s)
    s = _ISBN_TAIL_RE.sub("", s)
    return compact_spaces(s)


_EDITORIAL_PAREN_RE = re.compile(
    r"^(traduzid[oa]|translated?|obras?|scanned?|scan|ocr|illustr|abrev|compil|org\.?|ed\.)\w*$",
    re.I,
)


def _parenthetical_is_editorial_note(inner: str) -> bool:
    """Conteudo entre parenteses que nao e nome de pessoa (traduzido, OCR, etc.)."""
    t = compact_spaces(inner)
    if not t:
        return True
    if _EDITORIAL_PAREN_RE.match(t):
        return True
    if re.fullmatch(r"[A-ZÀ-Ý]{2,8}", t):
        return True
    if author_looks_bad(t):
        return True
    tl = t.lower()
    if tl in {"espanhol", "spanish", "português", "portugues", "english", "ingles"}:
        return True
    if re.fullmatch(r"[a-zà-ÿ0-9 .,'’-]+", tl) and " e " in f" {tl} ":
        return True
    if "traduz" in tl or "translat" in tl:
        return True
    if re.search(
        r"\b(?:edition|ed\.?|book club|classics?|cole[cç][aã]o|edi[cç][aã]o)\b",
        tl,
        re.I,
    ):
        return True
    return False


def _count_trailing_name_initials(tokens: list[str]) -> int:
    """Tokens finais tipo 'G.' ou 'S.' (iniciais de nome russo/tecnicas)."""
    n = 0
    for i in range(len(tokens) - 1, -1, -1):
        if re.fullmatch(r"[A-ZÀ-Ý]\.", tokens[i], re.I):
            n += 1
        else:
            break
    return n


def _looks_like_translator_credit(s: str) -> bool:
    sl = compact_spaces(s).lower()
    if not sl:
        return False
    return bool(
        re.search(r"\b(?:traduz|translat|revis(?:ao|ão|ões|oes))\w*", sl)
        or re.search(r"\btrad\.?\b", sl)
    )


def _authors_look_suspicious(authors: list[str] | None) -> bool:
    """Heuristica: lista de 'autores' parece fragmentos de titulo (parse do ficheiro falhou)."""
    if not authors:
        return False
    au = [compact_spaces(a) for a in authors if compact_spaces(a)]
    if not au:
        return False
    articles = frozenset(
        {"o", "a", "os", "as", "the", "an", "um", "uma", "uns", "umas", "le", "la", "les", "un"}
    )
    for a in au:
        toks = a.split()
        if (
            len(toks) >= 2
            and toks[0].lower().strip(".'’") in articles
            and not re.search(r"\b(?:de|da|do|dos|das|del|van|von)\b", toks[0], re.I)
        ):
            return True
    if len(au) >= 4:
        return True
    title_like = 0
    for a in au:
        al = _segment_author_likelihood(a)
        tl = _segment_title_likelihood(a)
        if tl > al + 0.08:
            title_like += 1
    if len(au) >= 2 and title_like >= max(1, (len(au) + 1) // 2):
        return True
    return False


def _remote_bibliographic_trustworthy(remote: BookMeta | None) -> bool:
    """Registro remoto suficiente para nao acumular mais fontes (ano + titulo + autor legitimo)."""
    if not remote:
        return False
    if not compact_spaces(remote.title or ""):
        return False
    if not compact_spaces(remote.year or ""):
        return False
    au = remote.authors or []
    if not au or authors_list_looks_bad(au):
        return False
    if _authors_look_suspicious(au):
        return False
    return True


def authors_list_looks_bad(authors: list[str] | None) -> bool:
    if not authors:
        return True
    return all(author_looks_bad(a) for a in authors)


def dedupe_authors(authors: list[str]) -> list[str]:
    def _author_sig(a: str) -> tuple[str, str]:
        a = compact_spaces(a)
        if not a:
            return "", ""
        if "," in a:
            before, after = [compact_spaces(x) for x in a.split(",", 1)]
            sb = normalize_for_match(before).split()
            sa = normalize_for_match(after).split()
            surname = " ".join(sb) if sb else ""
            first = sa[0][0] if sa and sa[0] else ""
            return surname, first
        toks = normalize_for_match(a).split()
        if not toks:
            return "", ""
        surname = toks[-1]
        first = toks[0][0] if toks and toks[0] else ""
        return surname, first

    def _author_richness(a: str) -> tuple[int, int]:
        if "," in a:
            before, after = [compact_spaces(x) for x in a.split(",", 1)]
            b = normalize_for_match(before).split()
            c = normalize_for_match(after).split()
            initials = sum(1 for t in c if len(t) == 1)
            # Prefere nome dado mais completo (menos iniciais).
            return (len(b) + len(c), len("".join(b + c)) - initials * 2)
        toks = normalize_for_match(a).split()
        initials = sum(1 for t in toks[:-1] if len(t) == 1)
        return (len(toks), len("".join(toks)) - initials * 2)

    def _is_initials_heavy(a: str) -> bool:
        if "," in a:
            _before, after = [compact_spaces(x) for x in a.split(",", 1)]
            toks = normalize_for_match(after).split()
        else:
            toks = normalize_for_match(a).split()[:-1]
        if not toks:
            return False
        initials = sum(1 for t in toks if len(t) == 1)
        return initials >= max(1, len(toks))

    out: list[str] = []
    norms: list[str] = []
    for a in authors:
        a = compact_spaces(a)
        if not a:
            continue
        n = normalize_for_match(a)
        if not n:
            continue
        sig = _author_sig(a)
        replaced = False
        for i, prev in enumerate(out):
            pnorm = norms[i]
            if fuzz.token_set_ratio(n, pnorm) >= 94:
                replaced = True
                break
            psig = _author_sig(prev)
            if sig[0] and sig[0] == psig[0] and sig[1] and sig[1] == psig[1]:
                if _author_richness(a) > _author_richness(prev):
                    out[i] = a
                    norms[i] = n
                replaced = True
                break
            if sig[0] and sig[0] == psig[0]:
                if _is_initials_heavy(a) != _is_initials_heavy(prev):
                    if _author_richness(a) > _author_richness(prev):
                        out[i] = a
                        norms[i] = n
                    replaced = True
                    break
        if replaced:
            continue
        out.append(a)
        norms.append(n)
    return out


def author_is_weak(author: str) -> bool:
    a = compact_spaces(author)
    if not a:
        return True
    # Single-token names (only surname) are weak for bibliography output.
    return len(a.split()) <= 1


def authors_need_enrichment(authors: list[str] | None) -> bool:
    if not authors:
        return True
    return all(author_is_weak(a) for a in authors)


def surnames_compatible(local_authors: list[str], remote_authors: list[str]) -> bool:
    if not local_authors or not remote_authors:
        return False
    for la in local_authors:
        l_last = normalize_for_match(la).split()
        if not l_last:
            continue
        token = l_last[-1]
        found = False
        for ra in remote_authors:
            r_tokens = normalize_for_match(ra).split()
            if not r_tokens:
                continue
            if fuzz.ratio(token, r_tokens[-1]) >= 88:
                found = True
                break
        if not found:
            return False
    return True


def prefer_author_order(authors: list[str], first_author: str) -> list[str]:
    if not authors or not first_author:
        return authors
    first_norm = normalize_for_match(first_author)
    if not first_norm:
        return authors
    best_idx = None
    best_score = -1
    for i, a in enumerate(authors):
        score = fuzz.token_set_ratio(normalize_for_match(a), first_norm)
        if score > best_score:
            best_score = score
            best_idx = i
    if best_idx is not None and best_score >= 80 and best_idx != 0:
        return [authors[best_idx]] + authors[:best_idx] + authors[best_idx + 1 :]
    return authors


def title_contains_authors(title: str, authors: list[str]) -> bool:
    if not title or not authors:
        return False
    nt = normalize_for_match(title)
    if not nt:
        return False
    hits = 0
    for a in authors:
        na = normalize_for_match(a)
        if not na:
            continue
        if fuzz.token_set_ratio(nt, na) >= 65:
            hits += 1
    return hits >= 1


def safe_filename_part(s: str, max_len: int = 180) -> str:
    s = compact_spaces(s)

    replacements = {
        ":": " -",
        "/": "-",
        "\\": "-",
        "?": "",
        "*": "",
        "\"": "",
        "<": "",
        ">": "",
        "|": "-",
    }

    for old, new in replacements.items():
        s = s.replace(old, new)

    s = re.sub(r"[\x00-\x1f\x7f]", "", s)
    s = re.sub(r"\.{2,}", ".", s)
    s = re.sub(r"\s+", " ", s).strip(" .")

    if len(s) > max_len:
        s = s[:max_len].rstrip(" .-_")

    if not s:
        return "sem_nome"

    base = s.split(".", 1)[0].lower() if "." in s else s.lower()
    if base in _WINDOWS_RESERVED_NAMES:
        s = "_" + s

    return s


def extract_year_candidates(s: str) -> list[int]:
    if not s:
        return []
    current_year = datetime.now().year
    years = []
    for y in re.findall(r"\b(1[4-9]\d{2}|20\d{2})\b", s):
        yi = int(y)
        if 1450 <= yi <= current_year + 1:
            years.append(yi)
    return years


# Grupos (site: a OR site: b) no DuckDuckGo para cata anos em snippets sem API/chave.
# Referencias: WorldCat, Goodreads, StoryGraph, LibraryThing, BookBrowse, BookBrainz,
# Amazon Books, ISBNdb — ver README.
DDG_CATALOG_SITE_GROUPS: tuple[tuple[str, ...], ...] = (
    ("worldcat.org", "goodreads.com", "thestorygraph.com"),
    ("librarything.com", "bookbrowse.com", "bookbrainz.org"),
    ("amazon.com", "isbndb.com"),
)

# Ordem logica do pipeline remoto (subconjuntos sao respeitados via frozenset).
REMOTE_SOURCE_KEYS: tuple[str, ...] = (
    "openlibrary",
    "google",
    "skoob",
    "catalogs",
    "wikipedia",
    "web",
)
ALL_REMOTE_SOURCES: frozenset[str] = frozenset(REMOTE_SOURCE_KEYS)

# Velocidade 1 = mais fontes (mais lento); 5 = menos fontes (mais rapido).
SEARCH_SPEED_TO_SOURCES: dict[int, frozenset[str]] = {
    1: ALL_REMOTE_SOURCES,
    2: frozenset({"openlibrary", "google", "skoob", "catalogs", "wikipedia"}),
    3: frozenset({"openlibrary", "google", "skoob", "catalogs"}),
    4: frozenset({"openlibrary", "google", "skoob"}),
    5: frozenset({"openlibrary", "google"}),
}

REMOTE_SOURCE_COST_ESTIMATE: dict[str, float] = {
    "openlibrary": 0.0001,
    "google": 0.0002,
    "skoob": 0.0002,
    "catalogs": 0.0002,
    "wikipedia": 0.0001,
    "web": 0.0002,
}


MERGE_METADATA_FIELDS: frozenset[str] = frozenset(
    {"title", "authors", "year", "isbn", "publisher"}
)


def parse_merge_metadata_csv(raw: str) -> frozenset[str]:
    """Campos de metadado para merge local+remoto: title, authors, year, isbn, publisher."""
    aliases = {
        "date": "year",
        "ano": "year",
        "author": "authors",
        "autor": "authors",
        "autores": "authors",
        "titulo": "title",
        "titulo_do_livro": "title",
        "editora": "publisher",
    }
    parts = [p.strip().lower() for p in (raw or "").split(",") if p.strip()]
    out: set[str] = set()
    for p in parts:
        p = aliases.get(p, p)
        if p not in MERGE_METADATA_FIELDS:
            allowed = ", ".join(sorted(MERGE_METADATA_FIELDS))
            raise ValueError(f"campo desconhecido '{p}'; permitidos: {allowed}")
        out.add(p)
    if not out:
        raise ValueError("lista de campos vazia")
    return frozenset(out)


def parse_remote_sources_csv(raw: str) -> frozenset[str]:
    """Lista separada por virgula: openlibrary, google, skoob, catalogs, wikipedia, web."""
    parts = [p.strip().lower() for p in (raw or "").split(",") if p.strip()]
    aliases = {
        "googlebooks": "google",
        "gb": "google",
        "ol": "openlibrary",
        "open-library": "openlibrary",
        "wiki": "wikipedia",
    }
    out: set[str] = set()
    for p in parts:
        p = aliases.get(p, p)
        if p not in ALL_REMOTE_SOURCES:
            allowed = ", ".join(sorted(ALL_REMOTE_SOURCES))
            raise ValueError(f"fonte desconhecida '{p}'; permitidas: {allowed}")
        out.add(p)
    if not out:
        raise ValueError("lista de fontes vazia em --sources")
    return frozenset(out)


def publication_adjacent_years(text: str) -> list[int]:
    """Anos perto de expressoes de publicacao/edicao em texto solto (HTML, snippets)."""
    patterns = [
        r"(?i)(?:publication date|published|released|launch(?:ed)?|copyright|©|data de publica[cç][aã]o|publicado|lan[cç]amento|edi[cç][aã]o)[^\n]{0,90}\b(1[4-9]\d{2}|20\d{2})\b",
        r"(?i)\b(1[4-9]\d{2}|20\d{2})\b[^\n]{0,90}(?:publication date|published|released|launch(?:ed)?|copyright|©|data de publica[cç][aã]o|publicado|lan[cç]amento|edi[cç][aã]o)",
    ]
    out: list[int] = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            out.extend(extract_year_candidates(m.group(0)))
    return out


def years_near_substrings(text: str, needles: tuple[str, ...], window: int = 120) -> list[int]:
    """Anos em janelas ao redor de substrings (ex.: dominios em resultados de busca)."""
    out: list[int] = []
    tl = text.lower()
    for needle in needles:
        nl = needle.lower()
        start = 0
        while True:
            i = tl.find(nl, start)
            if i == -1:
                break
            chunk = text[max(0, i - window) : i + len(needle) + window]
            out.extend(extract_year_candidates(chunk))
            start = i + 1
    return out


def year_from_string(s: str, prefer: str = "first") -> str:
    years = extract_year_candidates(s)
    if not years:
        return ""
    if prefer == "latest":
        return str(max(years))
    if prefer == "earliest":
        return str(min(years))
    return str(years[0])


def infer_year_from_pdf_text(text: str, year_strategy: str = "original") -> str:
    if not text:
        return ""
    sample = text[:12000]

    context_patterns = [
        r"(?i)(?:copyright|©|published|publication date|publicado|data de publica[cç][aã]o|impress[aã]o|reimpress[aã]o|lan[cç]ado)[^\n]{0,80}\b(1[4-9]\d{2}|20\d{2})\b",
        r"(?i)\b(1[4-9]\d{2}|20\d{2})\b[^\n]{0,80}(?:copyright|©|published|publication date|publicado|data de publica[cç][aã]o|impress[aã]o|reimpress[aã]o|lan[cç]ado)",
    ]
    for pat in context_patterns:
        m = re.search(pat, sample)
        if m:
            return m.group(1)

    # Avoid blind guesses from body text (years in historical content often
    # are not publication years). If no publication-context marker is found,
    # do not infer year from plain text.
    return ""


def title_variants(title: str) -> list[str]:
    t = clean_title(title)
    if not t:
        return []
    variants = [t]

    short = re.split(r"\s+[-:]\s+", t, maxsplit=1)[0].strip()
    if short and short not in variants:
        variants.append(short)

    no_paren = re.sub(r"\([^)]*\)", " ", t)
    no_paren = compact_spaces(no_paren)
    if no_paren and no_paren not in variants:
        variants.append(no_paren)

    ascii_v = strip_accents(t)
    if ascii_v and ascii_v not in variants:
        variants.append(ascii_v)

    lowered = re.sub(r"^(?:o|a|os|as|the|el|la|los|las)\s+", "", t, flags=re.I)
    lowered = compact_spaces(lowered)
    if lowered and lowered not in variants:
        variants.append(lowered)

    return variants[:5]


def _strip_catalog_author_life_span(s: str) -> str:
    """Remove sufixos catalograficos ', 1919-' ou ', 1919-2020' (datas de vida, nao parte do nome)."""
    t = compact_spaces(s)
    if not t:
        return t
    t = re.sub(r",\s*\d{4}\s*[-–—]\s*\d{4}\s*$", "", t)
    t = re.sub(r",\s*\d{4}\s*[-–—]\s*$", "", t)
    return compact_spaces(t)


def split_authors(raw: str | list[str] | None) -> list[str]:
    if not raw:
        return []

    if isinstance(raw, list):
        items = raw
    else:
        text = compact_spaces(raw)
        text = re.sub(r"\s*&\s+", ";", text)

        def _replace_conjunction(m: re.Match[str]) -> str:
            left = compact_spaces(m.group("left"))
            right = compact_spaces(m.group("right"))
            return f"{left}; {right}" if _can_split_author_conjunction(left, right) else m.group(0)
        if "," not in text:
            text = re.sub(
                r"(?P<left>[^;]+?)\s+(?:and|e)\s+(?P<right>[^;]+)",
                _replace_conjunction,
                text,
                flags=re.I,
            )
        else:
            # Virgula + " e " costuma ser titulo ("Hegel, Marx e a Tradicao"), nao lista de autores.
            text = re.sub(
                r"(?P<left>[^;]+?)\s+and\s+(?P<right>[^;]+)",
                _replace_conjunction,
                text,
                flags=re.I,
            )
        if ";" not in text and "," in text:
            comma_parts = [compact_spaces(p) for p in text.split(",") if compact_spaces(p)]
            # Treat "Nome Sobrenome, Nome Sobrenome" as two authors.
            # Keep "SOBRENOME, Nome" intact (usually one side has a single token).
            if (
                len(comma_parts) == 2
                and all(len(p.split()) >= 2 for p in comma_parts)
                and not comma_parts[0].isupper()
            ):
                items = comma_parts
            else:
                items = re.split(r"\s*;\s*", text)
        else:
            items = re.split(r"\s*;\s*", text)

    out = []

    expanded_items: list[str] = []
    for item in items:
        ci = compact_spaces(item)
        if not ci:
            continue
        # Lista mista: "A, B" (nome completo, nao formato SOBRENOME, Nome) -> separa autores.
        if "," in ci and ";" not in ci:
            comma_parts = [compact_spaces(p) for p in ci.split(",") if compact_spaces(p)]
            if (
                len(comma_parts) == 2
                and all(len(p.split()) >= 2 for p in comma_parts)
                and not comma_parts[0].isupper()
                and not re.match(r"^[A-ZÀ-Ý][A-ZÀ-Ý'’\-]+$", comma_parts[0])
            ):
                expanded_items.extend(comma_parts)
                continue
        expanded_items.append(ci)

    for item in expanded_items:
        item = _strip_catalog_author_life_span(compact_spaces(item))
        if item and item not in out:
            out.append(item)

    return out


def _can_split_author_conjunction(left: str, right: str) -> bool:
    l, r = compact_spaces(left), compact_spaces(right)
    if not l or not r:
        return False
    if is_year_token(l) or is_year_token(r):
        return False
    if not re.search(r"[A-Za-zÀ-ÿ]", l) or not re.search(r"[A-Za-zÀ-ÿ]", r):
        return False
    if _looks_like_volume_edition_credits(l) or _looks_like_volume_edition_credits(r):
        return False
    if _parenthetical_is_editorial_note(l) or _parenthetical_is_editorial_note(r):
        return False
    lw, rw = len(l.split()), len(r.split())
    if not (1 <= lw <= 4 and 1 <= rw <= 4):
        return False
    strong_mononyms = AUTHOR_ONE_WORD_SURNAMES
    if lw == 1 and rw == 1:
        if normalize_for_match(l) not in strong_mononyms or normalize_for_match(r) not in strong_mononyms:
            return False
    if _segment_title_likelihood(l) >= 0.55 or _segment_title_likelihood(r) >= 0.55:
        return False
    if _segment_author_likelihood(l) < 0.18 or _segment_author_likelihood(r) < 0.18:
        return False
    return True


def isbn10_valid(s: str) -> bool:
    if len(s) != 10:
        return False

    total = 0

    for i, ch in enumerate(s):
        if ch.upper() == "X" and i == 9:
            val = 10
        elif ch.isdigit():
            val = int(ch)
        else:
            return False

        total += (10 - i) * val

    return total % 11 == 0


def isbn13_valid(s: str) -> bool:
    if len(s) != 13 or not s.isdigit():
        return False

    total = sum((1 if i % 2 == 0 else 3) * int(ch) for i, ch in enumerate(s[:12]))
    check = (10 - (total % 10)) % 10

    return check == int(s[12])


def find_isbn(text: str) -> str:
    if not text:
        return ""

    candidates = re.findall(
        r"(?:ISBN(?:-1[03])?[:\s]*)?((?:97[89][\-\s]?)?\d[\d\-\s]{8,16}[\dXx])",
        text,
        flags=re.I,
    )

    for c in candidates:
        s = re.sub(r"[^0-9Xx]", "", c)

        if len(s) == 13 and isbn13_valid(s):
            return s

        if len(s) == 10 and isbn10_valid(s):
            return s

    return ""


def _looks_like_volume_edition_credits(s: str) -> bool:
    """Conteúdo típico de (Vol... / ...ed.) — não é lista de autores."""
    t = compact_spaces(s)
    if not t or len(t) > 140:
        return False
    if len(t.split()) > 14:
        return False
    sl = t.lower()
    if re.fullmatch(r"[12][0-9]{3}", sl.strip("()")):
        return False
    if re.search(r"\b(?:vol\.?|volume)\s*\d", sl, re.I):
        return True
    if re.search(r"\bv\.\s*\d", sl, re.I):
        return True
    if re.search(r"\b(?:tomo|tom)\s*\d", sl, re.I):
        return True
    if re.search(r"\b(?:t\.)\s*\d", sl, re.I):
        return True
    if re.search(
        r"\d{1,2}\s*[ªºa]\s*\.?\s*(?:ed\.?|edi[cç][aã]o|edicao|edition)\b",
        sl,
        re.I,
    ):
        return True
    if re.search(r"\b(?:ed\.?|edi[cç][aã]o|edicao|edition)\s*\d", sl, re.I):
        return True
    if re.search(r"\d\s*[aª]\.?\s*ed\b", sl, re.I):
        return True
    if re.search(r"\b(?:revis|rev\.|atualizad)\w*", sl, re.I) and re.search(
        r"\bed\b", sl, re.I
    ):
        return True
    if re.search(r"\bbook club(?: edition)?\b", sl, re.I):
        return True
    if re.search(r"\b(?:penguin|oxford|signet)\s+.*\bclassics?\b", sl, re.I):
        return True
    if re.search(r"\b(?:cole[cç][aã]o|edi[cç][aã]o)\b", sl, re.I):
        return True
    return False


def strip_trailing_volume_edition_parenthetical(stem: str) -> tuple[str, str]:
    """Remove um ou mais sufixos finais (...vol/ed...) e devolve texto base + sufixo plano."""
    s = compact_spaces(stem)
    bits: list[str] = []
    tail_re = re.compile(r"^(.+?)\s*\(([^()]{1,160})\)\s*$")
    while True:
        m = tail_re.match(s)
        if not m:
            break
        inner = compact_spaces(m.group(2))
        if not _looks_like_volume_edition_credits(inner):
            break
        bits.insert(0, inner)
        s = compact_spaces(m.group(1))
    suffix = compact_spaces(" ".join(bits))
    return s, suffix


def normalize_volume_edition_suffix(s: str) -> str:
    """Normaliza abreviaturas de edição (ex.: 3a.ed -> 3ªed)."""
    t = compact_spaces(s)
    if not t:
        return ""
    t = re.sub(
        r"(\d)\s*([aA])(?:\.|\s)+ed\b",
        r"\1ªed",
        t,
        flags=re.I,
    )
    t = re.sub(r"\b(?:vol(?:ume)?\.?)\s*([0-9IVXLC]+)\b", r"Vol. \1", t, flags=re.I)
    return compact_spaces(t)


def _segment_author_likelihood(seg: str) -> float:
    """Heuristica 0-1: o segmento parece bloco de autor(es)?"""
    t = compact_spaces(seg)
    if not t:
        return 0.0
    if is_year_token(t):
        return 0.0
    wc = len(t.split())
    score = 0.04
    if 1 <= wc <= 10:
        score += 0.14
    if 2 <= wc <= 6:
        score += 0.18
    if re.search(r"\s([&]|\+)\s|\band\b|\be\b\s", t, re.I):
        score += 0.34
    if "," in t:
        score += 0.18
    if wc >= 5 and re.search(r"\b(?:and|e)\b", t, re.I):
        score -= 0.55
    parts_sa = split_authors(t)
    if len(parts_sa) >= 2:
        score += 0.26
    elif len(parts_sa) == 1 and 2 <= wc <= 5:
        score += 0.1
    n = normalize_for_match(t)
    nwords = set(n.split())
    nh = len(nwords & STOP_TITLE_WORDS)
    if nh:
        score -= min(0.28, 0.07 * nh)
    if _looks_like_volume_edition_credits(t):
        score -= 0.55
    if _looks_like_internal_id_title(t):
        score -= 0.5
    if wc > 14:
        score -= 0.38
    elif wc > 10:
        score -= 0.12
    return max(0.0, min(1.0, score))


def _segment_title_likelihood(seg: str) -> float:
    """Heuristica 0-1: o segmento parece titulo de obra?"""
    t = compact_spaces(seg)
    if not t:
        return 0.0
    if is_year_token(t):
        return 0.0
    wc = len(t.split())
    score = 0.08
    if wc >= 5:
        score += 0.22
    elif wc >= 3:
        score += 0.12
    n = normalize_for_match(t)
    nwords = set(n.split())
    if nwords & STOP_TITLE_WORDS:
        score += 0.08 * min(3, len(nwords & STOP_TITLE_WORDS))
    if re.search(r"\b(de|do|da|dos|das|the|of|un|une|des|les)\b", t, re.I):
        score += 0.14
    if wc <= 2 and "," not in t:
        score -= 0.15
    if _looks_like_internal_id_title(t):
        score -= 0.55
    if _looks_like_volume_edition_credits(t):
        score -= 0.2
    return max(0.0, min(1.0, score))


def _resolve_two_segments_to_authors_and_title(left: str, right: str) -> tuple[list[str], str]:
    """Decide se left ou right e o lado dos autores (titulo pode ser o outro)."""
    left, right = compact_spaces(left), compact_spaces(right)
    if not left or not right:
        return [], compact_spaces(f"{left} {right}".strip())
    if is_year_token(left) or is_year_token(right):
        return [], clean_title(compact_spaces(f"{left} - {right}"))

    la, ra = _segment_author_likelihood(left), _segment_author_likelihood(right)
    lt, rt = _segment_title_likelihood(left), _segment_title_likelihood(right)
    strong_mononyms = AUTHOR_ONE_WORD_SURNAMES
    nl, nr = normalize_for_match(left), normalize_for_match(right)

    def _looks_like_person_name_segment(seg: str) -> bool:
        s = compact_spaces(seg)
        if author_looks_bad(s):
            return False
        toks = s.split()
        if toks and toks[0].lower().strip(".'’") in {"o", "a", "os", "as", "the", "el", "la", "los", "las"}:
            return False
        if re.search(r"\b(?:review|journal|magazine|bulletin|volume|vol\.?)\b", s, re.I):
            return False
        if re.search(r"\b(?:and|e|of)\b", s, re.I) and len(s.split()) >= 3:
            return False
        if not (1 <= len(toks) <= 6):
            return False
        if not re.search(r"[A-Za-zÀ-ÿ]", s):
            return False
        al = _segment_author_likelihood(s)
        tl = _segment_title_likelihood(s)
        if tl > al + 0.06:
            return False
        return True

    left_personish = _looks_like_person_name_segment(left)
    right_personish = _looks_like_person_name_segment(right)
    if left_personish and not right_personish:
        return split_authors(left), right
    if right_personish and not left_personish:
        return split_authors(right), left
    if (
        len(left.split()) == 1
        and nl in strong_mononyms
        and re.search(r"[A-Za-zÀ-ÿ]", right)
        and not author_looks_bad(left)
    ):
        return split_authors(left), right
    if (
        len(right.split()) == 1
        and nr in strong_mononyms
        and re.search(r"[A-Za-zÀ-ÿ]", left)
        and not author_looks_bad(right)
    ):
        return split_authors(right), left

    left_is_author = la + rt * 0.55
    right_is_author = ra + lt * 0.55

    margin = 0.06

    if left_is_author >= right_is_author + margin:
        aul = split_authors(left)
        if aul or la >= 0.2:
            return aul if aul else split_authors(left), right
    if right_is_author >= left_is_author + margin:
        aur = split_authors(right)
        if aur or ra >= 0.2:
            return aur if aur else split_authors(right), left

    lw, rw = len(left.split()), len(right.split())
    if lw == 1 and not is_year_token(left) and re.search(r"[A-Za-zÀ-ÿ]", left):
        if rt >= 0.26 and not _looks_like_volume_edition_credits(left) and not author_looks_bad(left):
            return split_authors(left), right
    if rw == 1 and not is_year_token(right) and re.search(r"[A-Za-zÀ-ÿ]", right):
        if lt >= 0.26 and not _looks_like_volume_edition_credits(right) and not author_looks_bad(right):
            return split_authors(right), left
    if lw <= 6 and lw <= rw and re.search(r"[A-Za-zÀ-ÿ]", left) and not is_year_token(left):
        return split_authors(left), right
    if rw <= 6 and rw < lw and re.search(r"[A-Za-zÀ-ÿ]", right) and not is_year_token(right):
        return split_authors(right), left
    if lw <= 6 and re.search(r"[A-Za-zÀ-ÿ]", left) and not is_year_token(left):
        return split_authors(left), right
    if rw <= 6 and re.search(r"[A-Za-zÀ-ÿ]", right) and not is_year_token(right):
        return split_authors(right), left

    return [], clean_title(compact_spaces(f"{left} - {right}"))


def _strip_trailing_paren_publication_year(stem: str) -> tuple[str, str]:
    """Remove '(1999)' publicacao no fim do stem. 'Autor - Titulo (1999)' -> base + '1999'."""
    s = compact_spaces(stem)
    m = re.match(r"^(?P<base>.+?)\s*\((?P<yr>1[4-9]\d{2}|20\d{2})\)\s*$", s)
    if not m:
        return s, ""
    return compact_spaces(m.group("base")), m.group("yr")


def _sanitize_mixed_hyphen_underscore(s: str) -> str:
    """Higieniza misturas Karl_Marx_-_O_Capital ou Karl_Marx - O_Capital antes do parse bipartido."""
    s = compact_spaces(_normalize_filename_hyphens(s))
    # Underscores a abraçar hifen(s) → um unico separador " - "
    s = re.sub(r"_+\s*-\s*_+", " - ", s)
    s = re.sub(r"_+\s*-", " - ", s)
    s = re.sub(r"-\s*_+", " - ", s)
    s = compact_spaces(re.sub(r"(?:\s*-\s*){2,}", " - ", s))
    return s


def _normalize_underscore_separators(s: str) -> str:
    """Trata _, __ e ' _ ' como separador semelhante ao hifen com espacos."""
    s2 = compact_spaces(s)
    s2 = re.sub(r"\s+_{2,}\s+", " - ", s2)
    s2 = re.sub(r"_{2,}", " - ", s2)
    if s2.count("_") == 1:
        s2 = s2.replace("_", " - ")
    else:
        s2 = re.sub(r"(?<=\S)_(?=\S)", " ", s2)
    return compact_spaces(re.sub(r"(?:\s*-\s*){2,}", " - ", s2))


def _expand_filename_separators_for_bipartite(stem: str) -> str:
    """Prepara stem: higieniza _/- mistos, dois-pontos via _, depois underscores restantes (Autor/Titulo)."""
    s = _sanitize_mixed_hyphen_underscore(stem)
    s = _underscore_subtitle_as_colon(s)
    s = _normalize_underscore_separators(s)
    return compact_spaces(s)


def _bipartite_split_once(stem: str) -> tuple[str, str] | None:
    """Obtem dois segmentos (antes/depois do primeiro separador forte) ou None."""
    raw = _expand_filename_separators_for_bipartite(stem)
    parts = re.split(r"\s+-\s+", raw, maxsplit=1)
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]
    cands = list(re.finditer(r"(?<=[A-Za-zÀ-ÿ0-9])-(?=[A-Za-zÀ-ÿ0-9])", raw))
    if len(cands) == 1:
        m = cands[0]
        L, R = compact_spaces(raw[: m.start()]), compact_spaces(raw[m.end() :])
        if len(L) >= 3 and len(R) >= 4:
            return L, R
    return None


def _finish_filename_meta(meta: BookMeta, file_suffix: str) -> BookMeta:
    if file_suffix:
        meta.filename_extra_suffix = normalize_volume_edition_suffix(file_suffix)
    return meta


def _parse_filename_triplet_author_year_title(path: Path, stem: str) -> BookMeta | None:
    m = re.match(
        r"^(.+?)\s+-\s+((?:1[4-9]\d{2}|20\d{2}|s\.d\.))\s+-\s+(.+)$",
        stem,
        re.I,
    )
    if not m:
        return None
    authors = split_authors(m.group(1))
    year = "" if m.group(2).lower() == "s.d." else m.group(2)
    title = m.group(3)
    return BookMeta(
        str(path),
        clean_title(title),
        authors,
        year,
        source="filename",
        confidence=0.35,
        filename_paren_year=False,
    )


def _parse_filename_triplet_year_author_title(path: Path, stem: str) -> BookMeta | None:
    m = re.match(r"^((?:1[4-9]\d{2}|20\d{2}))\s+-\s+(.+?)\s+-\s+(.+)$", stem)
    if not m:
        return None
    year = m.group(1)
    authors = split_authors(m.group(2))
    title = m.group(3)
    return BookMeta(
        str(path),
        clean_title(title),
        authors,
        year,
        source="filename",
        confidence=0.35,
        filename_paren_year=False,
    )


def _parse_filename_nested_editor_parenthetical(
    path: Path, stem: str, year: str, filename_paren_year: bool
) -> BookMeta | None:
    m = re.match(
        r"^(.+?)\s*\(([^()]+)\s*\((?:eds?\.?|org\.?|trad\.?)\)\)\s*$",
        stem,
        re.I,
    )
    if not m:
        return None
    title = m.group(1)
    authors = split_authors(m.group(2))
    return BookMeta(
        str(path),
        clean_title(title),
        authors,
        year,
        source="filename",
        confidence=0.27,
        filename_paren_year=filename_paren_year,
    )


def _parse_filename_simple_parenthetical(
    path: Path, stem: str, year: str, filename_paren_year: bool
) -> BookMeta | None:
    m = re.match(r"^(.+?)\s*\(([^()]+)\)$", stem)
    if not m:
        return None
    inner = compact_spaces(m.group(2))
    if re.fullmatch(r"(?:1[4-9]\d{2}|20\d{2})", inner):
        return BookMeta(
            str(path),
            clean_title(m.group(1)),
            [],
            inner,
            source="filename",
            confidence=0.28,
            filename_paren_year=True,
        )
    if _parenthetical_is_editorial_note(inner):
        base = m.group(1)
        pair = _bipartite_split_once(base)
        if pair:
            left, right = pair
            authors, title_base = _resolve_two_segments_to_authors_and_title(left, right)
            title = f"{title_base} ({inner})" if title_base else f"{base} ({inner})"
        else:
            title = stem
            authors = []
    else:
        base = m.group(1)
        pair = _bipartite_split_once(base)
        if pair:
            left, right = pair
            authors_bt, title_bt = _resolve_two_segments_to_authors_and_title(left, right)
            if authors_bt and title_bt:
                inner_st = compact_spaces(inner)
                in_toks = inner_st.split()
                left_st = pair[0]
                n_inner = normalize_for_match(inner_st)
                l_ntoks = left_st.split()
                if (
                    len(in_toks) == 1
                    and n_inner not in AUTHOR_ONE_WORD_SURNAMES
                    and (
                        len(l_ntoks) >= 2
                        or (
                            len(l_ntoks) == 1
                            and normalize_for_match(l_ntoks[0]) in AUTHOR_ONE_WORD_SURNAMES
                        )
                    )
                ):
                    return BookMeta(
                        str(path),
                        clean_title(title_bt),
                        authors_bt,
                        year,
                        source="filename",
                        confidence=0.32,
                        filename_paren_year=filename_paren_year,
                        publisher=inner_st,
                    )
        title = m.group(1)
        authors = split_authors(inner)
    return BookMeta(
        str(path),
        clean_title(title),
        authors,
        year,
        source="filename",
        confidence=0.25,
        filename_paren_year=filename_paren_year,
    )


def _parse_filename_bipartite_fallback(path: Path, stem: str, year: str, filename_paren_year: bool) -> BookMeta:
    title = stem
    authors: list[str] = []
    conf_fb = 0.15
    pair = _bipartite_split_once(stem)
    if pair:
        left, right = pair
        if re.search(r"\b(?:review|journal|magazine|bulletin)\b", left, re.I) and re.search(
            r"\b(?:vol\.?|volume|no\.?|nº)\b",
            right,
            re.I,
        ):
            return BookMeta(
                str(path),
                clean_title(compact_spaces(f"{left} - {right}")),
                [],
                year,
                source="filename",
                confidence=0.18,
                filename_paren_year=filename_paren_year,
            )
        if re.search(r"[A-Za-zÀ-ÿ]", left + right):
            authors, title = _resolve_two_segments_to_authors_and_title(left, right)
            if authors:
                conf_fb = 0.2
    title = re.sub(r"\b(1[4-9]\d{2}|20\d{2})\b", " ", title)
    return BookMeta(
        str(path),
        clean_title(title),
        authors,
        year,
        source="filename",
        confidence=conf_fb,
        filename_paren_year=filename_paren_year,
    )


def parse_filename_fallback(path: Path) -> BookMeta:
    stem_raw = compact_spaces(path.stem)
    stem_raw = _sanitize_filename_stem_noise(stem_raw)
    stem_hyp = _normalize_filename_hyphens(stem_raw)
    stem, file_suffix = strip_trailing_volume_edition_parenthetical(stem_hyp)
    stem, paren_yr = _strip_trailing_paren_publication_year(stem)
    filename_paren_year = bool(paren_yr)
    year = (
        paren_yr
        or year_from_string(stem)
        or year_from_string(stem_hyp)
        or year_from_string(stem_raw)
    )
    for parser in (
        lambda: _parse_filename_triplet_author_year_title(path, stem),
        lambda: _parse_filename_triplet_year_author_title(path, stem),
        lambda: _parse_filename_nested_editor_parenthetical(path, stem, year, filename_paren_year),
        lambda: _parse_filename_simple_parenthetical(path, stem, year, filename_paren_year),
    ):
        parsed = parser()
        if parsed is not None:
            return _finish_filename_meta(parsed, file_suffix)

    parsed = _parse_filename_bipartite_fallback(path, stem, year, filename_paren_year)
    return _finish_filename_meta(parsed, file_suffix)


def filename_triplet_structured_stem(path: Path) -> bool:
    """True se o stem segue AUTOR - ANO|s.d. - TITULO ou ANO - AUTOR - TITULO (mesmas duas regex de parse_filename_fallback)."""
    stem = _normalize_filename_hyphens(compact_spaces(path.stem))
    if re.match(
        r"^(.+?)\s+-\s+((?:1[4-9]\d{2}|20\d{2}|s\.d\.))\s+-\s+(.+)$",
        stem,
        re.I,
    ):
        return True
    return bool(
        re.match(r"^((?:1[4-9]\d{2}|20\d{2}))\s+-\s+(.+?)\s+-\s+(.+)$", stem)
    )


def prioritize_triplet_filename_over_local(local: BookMeta, path: Path) -> BookMeta:
    """Quando o nome do ficheiro ja traz autor, ano (ou s.d.) e titulo em triplete, prioriza esse parse sobre EPUB/outros."""
    if not filename_triplet_structured_stem(path):
        return local
    fb = parse_filename_fallback(path)
    if fb.confidence < 0.35:
        return local
    out = replace(
        local,
        title=fb.title or local.title,
        authors=list(fb.authors) if fb.authors else list(local.authors or []),
        year=fb.year or local.year,
        confidence=max(local.confidence, fb.confidence),
        filename_paren_year=bool(
            getattr(fb, "filename_paren_year", False) or getattr(local, "filename_paren_year", False)
        ),
    )
    append_note(out, "nome em AUTOR-ANO-TITULO: sem busca remota")
    return out


_EPUB_MAX_XML_BYTES = 8 * 1024 * 1024  # 8 MiB para metadados (container/OPF)


def _safe_zip_read(z: zipfile.ZipFile, name: str, max_bytes: int) -> bytes:
    """Le um membro do zip com limite de tamanho (mitiga zip-bomb / OPF gigantes)."""
    try:
        info = z.getinfo(name)
    except KeyError as exc:
        raise FileNotFoundError(name) from exc
    if info.file_size > max_bytes:
        raise ValueError(f"membro {name!r} excede {max_bytes} bytes")
    with z.open(info, "r") as fh:
        data = fh.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError(f"membro {name!r} maior que limite ao descomprimir")
    return data


def read_epub_metadata(path: Path) -> BookMeta:
    meta = BookMeta(str(path), source="epub", confidence=0.5)

    try:
        with zipfile.ZipFile(path) as z:
            container_xml = _safe_zip_read(z, "META-INF/container.xml", _EPUB_MAX_XML_BYTES)
            root = ET.fromstring(container_xml)

            ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
            rootfile = root.find(".//c:rootfile", ns)

            if rootfile is None:
                raise ValueError("rootfile ausente no EPUB")

            opf_path = rootfile.attrib["full-path"]
            if "\x00" in opf_path or opf_path.startswith("/") or ".." in opf_path.split("/"):
                raise ValueError("OPF path suspeito")

            opf_bytes = _safe_zip_read(z, opf_path, _EPUB_MAX_XML_BYTES)
            opf = ET.fromstring(opf_bytes)

            titles: list[str] = []
            creators: list[str] = []
            identifiers: list[str] = []
            dates: list[str] = []
            publishers: list[str] = []
            subjects_acc: list[str] = []
            series_val = ""

            for el in opf.iter():
                tag = el.tag
                local = tag.split("}", 1)[1] if "}" in tag else tag
                if local in {"title", "creator", "identifier", "date", "publisher", "subject"}:
                    txt = compact_spaces(el.text or "")
                    if not txt:
                        continue
                    if local == "title":
                        titles.append(txt)
                    elif local == "creator":
                        creators.append(txt)
                    elif local == "identifier":
                        identifiers.append(txt)
                    elif local == "date":
                        dates.append(txt)
                    elif local == "publisher":
                        publishers.append(txt)
                    elif local == "subject":
                        subjects_acc.append(txt)
                elif local == "meta" and not series_val:
                    name = (el.attrib.get("name") or "").lower()
                    if name == "calibre:series":
                        series_val = compact_spaces(el.attrib.get("content") or "")

            if titles:
                meta.title = clean_title(titles[0])
            if creators:
                meta.authors = split_authors(creators)
            if publishers:
                meta.publisher = compact_spaces(publishers[0])
            if identifiers:
                meta.isbn = find_isbn(" ".join(identifiers))
            if dates:
                meta.year = year_from_string(" ".join(dates))
            if series_val:
                meta.series = series_val
            if subjects_acc:
                seen: set[str] = set()
                for t in subjects_acc:
                    k = t.lower()
                    if k not in seen:
                        seen.add(k)
                        meta.subjects.append(t)

    except Exception as e:
        meta.notes = f"falha ao ler EPUB: {e}"

    return meta


def read_pdf_metadata(path: Path, max_pages: int = 3, year_strategy: str = "original") -> BookMeta:
    meta = BookMeta(str(path), source="pdf", confidence=0.35)

    if PdfReader is None:
        meta.notes = "pypdf não instalado"
        return meta

    try:
        logging.getLogger("pypdf").setLevel(logging.ERROR)
        reader = PdfReader(str(path), strict=False)
        md = reader.metadata or {}

        title = ""
        author = ""

        if md:
            title = getattr(md, "title", None) or md.get("/Title", "") or ""
            author = getattr(md, "author", None) or md.get("/Author", "") or ""
            raw_dates = " ".join(
                str(x) for x in [
                    md.get("/CreationDate", ""),
                    md.get("/ModDate", ""),
                    getattr(md, "creation_date", "") if hasattr(md, "creation_date") else "",
                    getattr(md, "modification_date", "") if hasattr(md, "modification_date") else "",
                ] if x
            )
            if raw_dates and not meta.year:
                prefer_mode = "latest" if year_strategy == "edition" else "earliest"
                meta.year = year_from_string(raw_dates, prefer=prefer_mode)

        if title and not title_looks_bad(str(title)):
            meta.title = clean_title(str(title))

        if author and not author_looks_bad(str(author)):
            meta.authors = split_authors(str(author))

        chunks: list[str] = []
        for page in reader.pages[:max_pages]:
            try:
                chunks.append(page.extract_text() or "")
            except Exception:
                continue
        text = "\n".join(chunks)

        meta.isbn = find_isbn(text)

        if not meta.year:
            meta.year = infer_year_from_pdf_text(text, year_strategy=year_strategy)

    except Exception as e:
        meta.notes = f"falha ao ler PDF: {e}"

    return meta


def read_local_metadata(path: Path, max_pdf_pages: int, year_strategy: str = "original") -> BookMeta:
    fallback = parse_filename_fallback(path)

    if path.suffix.lower() == ".epub":
        meta = read_epub_metadata(path)
    elif path.suffix.lower() == ".pdf":
        meta = read_pdf_metadata(path, max_pages=max_pdf_pages, year_strategy=year_strategy)
    else:
        meta = BookMeta(str(path), source="unsupported", confidence=0.0)

    if path.suffix.lower() == ".pdf" and fallback.authors and fallback.title:
        # For PDFs with a structured filename, treat filename as canonical for
        # author/title and use embedded metadata mostly for year/ISBN.
        meta.title = fallback.title

        candidate_authors = dedupe_authors(meta.authors or [])
        fallback_authors = dedupe_authors(fallback.authors)
        candidate_authors = prefer_author_order(candidate_authors, fallback_authors[0])

        merged = fallback_authors[:]
        fb_last = normalize_for_match(fallback_authors[0]).split()
        fb_last_tok = fb_last[-1] if fb_last else ""
        for a in candidate_authors:
            if _looks_like_translator_credit(a):
                continue
            na = normalize_for_match(a)
            if any(fuzz.token_set_ratio(na, normalize_for_match(b)) >= 90 for b in merged):
                continue
            if len(fallback_authors) == 1:
                fb = normalize_for_match(fallback_authors[0])
                if fb and fuzz.token_set_ratio(na, fb) < 42:
                    continue
                # Evita anexar autor remoto/local extra quando o sobrenome diverge.
                cand_toks = na.split()
                cand_last = cand_toks[-1] if cand_toks else ""
                if fb_last_tok and cand_last and fuzz.ratio(cand_last, fb_last_tok) < 88:
                    continue
            merged.append(a)
        merged = dedupe_authors(merged)
        meta.authors = merged or fallback_authors

        if title_contains_authors(meta.title, meta.authors):
            meta.title = fallback.title
        append_note(meta, "PDF prioriza autor/titulo do nome do arquivo")

    if path.suffix.lower() == ".pdf" and fallback.title:
        if not meta.title or title_looks_bad(meta.title):
            meta.title = fallback.title
            append_note(meta, "titulo do PDF descartado; usando nome do arquivo")
        else:
            mt = normalize_for_match(meta.title)
            ft = normalize_for_match(fallback.title)
            if mt and ft and fuzz.token_set_ratio(mt, ft) < 50:
                meta.title = fallback.title
                append_note(meta, "titulo do PDF muito diferente; usando nome do arquivo")

    if path.suffix.lower() == ".pdf" and fallback.authors:
        if not meta.authors or authors_list_looks_bad(meta.authors):
            meta.authors = fallback.authors
            append_note(meta, "autores do PDF descartados; usando nome do arquivo")
        else:
            ma = normalize_for_match(" ".join(meta.authors))
            fa = normalize_for_match(" ".join(fallback.authors))
            if ma and fa and fuzz.token_set_ratio(ma, fa) < 55:
                meta.authors = fallback.authors
                append_note(meta, "autores do PDF muito diferentes; usando nome do arquivo")

    if not meta.title:
        meta.title = fallback.title

    if not meta.authors:
        meta.authors = fallback.authors

    if getattr(fallback, "filename_paren_year", False) and compact_spaces(fallback.year or ""):
        meta.year = fallback.year
        meta.filename_paren_year = True
    elif not meta.year:
        meta.year = fallback.year

    if not meta.source or meta.source == "unsupported":
        meta.source = fallback.source

    meta.confidence = max(meta.confidence, fallback.confidence)

    if fallback.notes and not meta.notes:
        meta.notes = fallback.notes

    if path.suffix.lower() in {".epub", ".mobi", ".azw", ".azw3"}:
        if _looks_like_internal_id_title(meta.title or "") and compact_spaces(fallback.title or ""):
            meta.title = fallback.title
            append_note(meta, "titulo do ficheiro substitui id/slug no metadado embeddado")
        if authors_list_looks_bad(meta.authors) and fallback.authors and not authors_list_looks_bad(
            fallback.authors
        ):
            meta.authors = list(fallback.authors)
            append_note(meta, "autores do ficheiro substituem metadado embeddado duvidoso")

    sfx = compact_spaces(getattr(fallback, "filename_extra_suffix", ""))
    if sfx:
        meta.filename_extra_suffix = sfx

    return meta


def patch_meta_from_filename_if_merged_suspect(path: Path, meta: BookMeta) -> BookMeta:
    """Evita renomear para titulo hash / UNKNOWN / autor remoto absurdo quando o nome do ficheiro e claro."""
    fb = parse_filename_fallback(path)
    out = meta

    if (
        _looks_like_internal_id_title(compact_spaces(out.title or ""))
        and compact_spaces(fb.title or "")
        and not _looks_like_internal_id_title(fb.title)
    ):
        out = replace(out, title=fb.title)
        append_note(out, "failsafe: titulo restaurado a partir do nome do ficheiro")

    if authors_list_looks_bad(out.authors) and fb.authors and not authors_list_looks_bad(fb.authors):
        out = replace(out, authors=list(fb.authors))
        append_note(out, "failsafe: autores restaurados a partir do nome do ficheiro")
    elif (
        fb.authors
        and not authors_list_looks_bad(fb.authors)
        and out.authors
        and not authors_list_looks_bad(out.authors)
    ):
        nfb = normalize_for_match(" ".join(fb.authors))
        nmo = normalize_for_match(" ".join(out.authors))
        if nfb and nmo and fuzz.token_set_ratio(nfb, nmo) < 42:
            out = replace(out, authors=list(fb.authors))
            append_note(out, "failsafe: autores do ficheiro preferidos (forte discordancia com remoto)")

    return out


def cache_key(url: str, params: dict[str, Any] | None) -> str:
    raw = url + "?" + urllib.parse.urlencode(params or {}, doseq=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


_HTML_CACHE_TRUNCATE = 96 * 1024  # 96 KiB de HTML por entrada (suficiente p/ snippets)


def _register_source_failure(
    source_failures: list[dict[str, str]] | None,
    source: str | None,
    reason: str,
    action: str = "ignored_source_and_continued",
) -> None:
    if source_failures is None or not source:
        return
    source_failures.append(
        {"source": source, "reason": compact_spaces(reason), "action": action}
    )


def _classify_external_error(exc: Exception, status_code: int | None = None) -> str:
    if status_code == 429:
        return "rate_limit_reached_http_429"
    if status_code in {500, 502, 503, 504}:
        return f"http_{status_code}_temporary_unavailable"
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection_error"
    if isinstance(exc, requests.exceptions.HTTPError):
        return f"http_error_{status_code or 'unknown'}"
    return f"{type(exc).__name__}: {exc}"


def get_json(
    url: str,
    params: dict[str, Any] | None,
    cache: dict[str, Any],
    sleep_s: float,
    source: str | None = None,
    source_failures: list[dict[str, str]] | None = None,
) -> Any:
    key = cache_key(url, params)

    if key in cache:
        return cache[key]

    try:
        if sleep_s > 0:
            time.sleep(sleep_s)

        session = _get_http_session()
        r = session.get(url, params=params, timeout=20)

        r.raise_for_status()
        ctype = (r.headers.get("Content-Type", "") or "").lower()
        if "json" in ctype:
            data = r.json()
        else:
            data = r.text
            if len(data) > _HTML_CACHE_TRUNCATE:
                data = data[:_HTML_CACHE_TRUNCATE]
        cache[key] = data
        return data

    except requests.exceptions.HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        reason = _classify_external_error(e, status_code=status)
        _register_source_failure(source_failures, source, reason)
        return {"_error": reason}
    except requests.exceptions.RequestException as e:
        reason = _classify_external_error(e)
        _register_source_failure(source_failures, source, reason)
        return {"_error": reason}
    except ValueError as e:
        reason = f"invalid_json: {e}"
        _register_source_failure(source_failures, source, reason)
        cache[key] = {"_error": reason}
        return cache[key]


def _subjects_from_openlibrary_isbn(data: dict[str, Any], limit: int = 25) -> list[str]:
    out: list[str] = []
    for s in (data.get("subjects") or [])[:limit]:
        if isinstance(s, dict):
            nm = compact_spaces(str(s.get("name", "") or s.get("title", "")))
            if nm:
                out.append(nm)
        elif isinstance(s, str) and compact_spaces(s):
            out.append(compact_spaces(s))
    return out


def _subjects_from_openlibrary_search_doc(doc: dict[str, Any], limit: int = 25) -> list[str]:
    raw = doc.get("subject") or doc.get("subject_key") or []
    out: list[str] = []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return out
    for s in raw[:limit]:
        if isinstance(s, str) and compact_spaces(s):
            out.append(compact_spaces(s))
    return out


def best_openlibrary(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    if meta.isbn:
        data = get_json(
            f"https://openlibrary.org/isbn/{meta.isbn}.json",
            None,
            cache,
            sleep_s,
            source="openlibrary",
            source_failures=source_failures,
        )

        if isinstance(data, dict) and "_error" not in data:
            title = data.get("title", "")
            year = year_from_string(str(data.get("publish_date", "")))

            if title or year:
                return BookMeta(
                    meta.path,
                    title=title,
                    authors=[],
                    year=year,
                    isbn=meta.isbn,
                    subjects=_subjects_from_openlibrary_isbn(data),
                    source="openlibrary:isbn",
                    confidence=0.75,
                )

    if not meta.title:
        return None

    best: tuple[float, dict[str, Any]] | None = None
    target_title = normalize_for_match(meta.title)
    target_author = normalize_for_match(" ".join(meta.authors or []))
    queries: list[dict[str, Any]] = []

    for tv in title_variants(meta.title):
        q1: dict[str, Any] = {"title": tv, "limit": 12}
        if meta.authors:
            q1["author"] = meta.authors[0]
        queries.append(q1)
        queries.append({"title": tv, "limit": 12})
        queries.append({"q": tv, "limit": 12})

    seen_keys: set[str] = set()
    unique_queries: list[dict[str, Any]] = []
    for q in queries:
        key = json.dumps(q, ensure_ascii=False, sort_keys=True)
        if key not in seen_keys:
            seen_keys.add(key)
            unique_queries.append(q)

    for params in unique_queries[:10]:
        data = get_json(
            "https://openlibrary.org/search.json",
            params,
            cache,
            sleep_s,
            source="openlibrary",
            source_failures=source_failures,
        )
        if not isinstance(data, dict) or "_error" in data:
            continue
        candidates = data.get("docs", []) or []
        if "docs" not in data:
            _register_source_failure(source_failures, "openlibrary", "missing_expected_field_docs")
            continue

        for doc in candidates:
            dt = normalize_for_match(str(doc.get("title", "")))
            da = normalize_for_match(" ".join(doc.get("author_name", [])[:5]))

            t_score = fuzz.token_set_ratio(target_title, dt) if target_title and dt else 0
            a_score = fuzz.token_set_ratio(target_author, da) if target_author and da else 60

            score = 0.78 * t_score + 0.22 * a_score

            min_title = 72 if not target_author else 60
            if t_score >= min_title and (not target_author or a_score >= 36):
                if best is None or score > best[0]:
                    best = (score, doc)

    if not best:
        return None

    score, doc = best

    years = [y for y in doc.get("publish_year", []) if isinstance(y, int)]
    first = doc.get("first_publish_year")
    all_years = [y for y in years if 1450 <= y <= datetime.now().year + 1]
    if isinstance(first, int) and 1450 <= first <= datetime.now().year + 1:
        all_years.append(first)
    if all_years:
        year = str(max(all_years) if year_strategy == "edition" else min(all_years))
    else:
        year = ""

    return BookMeta(
        meta.path,
        title=clean_title(str(doc.get("title", "") or meta.title)),
        authors=split_authors(doc.get("author_name", [])[:3]) or meta.authors,
        year=year,
        isbn=meta.isbn,
        subjects=_subjects_from_openlibrary_search_doc(doc),
        source="openlibrary:search",
        confidence=round(score / 100, 3),
    )


def best_googlebooks(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    queries: list[dict[str, Any]] = []

    if meta.isbn:
        queries.append({"q": f"isbn:{meta.isbn}", "maxResults": 10})

    tvars = title_variants(meta.title)
    if tvars:
        for tv in tvars:
            q_parts = [f'intitle:"{tv}"']
            if meta.authors:
                q_parts.append(f'inauthor:"{meta.authors[0]}"')
            queries.append({"q": " ".join(q_parts), "maxResults": 10})
            queries.append({"q": f'intitle:"{tv}"', "maxResults": 10})
            queries.append({"q": f'intitle:"{strip_accents(tv)}"', "maxResults": 10, "langRestrict": "en"})

    if not queries:
        return None

    target_title = normalize_for_match(meta.title)
    target_author = normalize_for_match(" ".join(meta.authors or []))

    best: tuple[float, dict[str, Any]] | None = None
    seen: set[str] = set()
    unique_queries: list[dict[str, Any]] = []
    for q in queries:
        key = json.dumps(q, ensure_ascii=False, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique_queries.append(q)

    for params in unique_queries[:12]:
        data = get_json(
            "https://www.googleapis.com/books/v1/volumes",
            params,
            cache,
            sleep_s,
            source="google",
            source_failures=source_failures,
        )

        if not isinstance(data, dict) or "_error" in data:
            continue

        items = data.get("items", []) or []
        if "items" not in data:
            _register_source_failure(source_failures, "google", "missing_expected_field_items")
            continue
        for item in items:
            info = item.get("volumeInfo", {})
            title = normalize_for_match(info.get("title", ""))
            authors = normalize_for_match(" ".join(info.get("authors", [])[:5]))

            t_score = fuzz.token_set_ratio(target_title, title) if target_title and title else 0
            a_score = fuzz.token_set_ratio(target_author, authors) if target_author and authors else 60

            score = 0.78 * t_score + 0.22 * a_score

            min_title = 70 if not target_author else 58
            if t_score >= min_title and (not target_author or a_score >= 34):
                if best is None or score > best[0]:
                    best = (score, info)

    if not best:
        return None

    score, info = best
    year = year_from_string(str(info.get("publishedDate", "")))
    pub = compact_spaces(str(info.get("publisher") or ""))
    subj: list[str] = []
    for c in (info.get("categories") or [])[:12]:
        for part in str(c).split("/"):
            p = compact_spaces(part)
            if p and p.lower() not in {x.lower() for x in subj}:
                subj.append(p)
    series = ""
    si = info.get("seriesInfo")
    if isinstance(si, dict):
        series = compact_spaces(str(si.get("title") or si.get("series") or ""))

    return BookMeta(
        meta.path,
        title=clean_title(info.get("title", "") or meta.title),
        authors=split_authors(info.get("authors", [])[:3]) or meta.authors,
        year=year,
        isbn=meta.isbn,
        publisher=pub,
        series=series,
        subjects=subj,
        source="googlebooks",
        confidence=round(score / 100, 3),
    )


def best_wikipedia(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    if not meta.title:
        return None

    author = meta.authors[0] if meta.authors else ""
    queries = []
    for tv in title_variants(meta.title):
        q = f"{tv} {author}".strip()
        if q:
            queries.append(q)

    best_year = ""
    best_title = ""
    best_score = 0.0
    target_title_norm = normalize_for_match(meta.title)

    found_years: list[int] = []
    for q in queries[:6]:
        data = get_json(
            "https://en.wikipedia.org/w/api.php",
            {
                "action": "query",
                "list": "search",
                "srsearch": q,
                "format": "json",
                "srlimit": 5,
            },
            cache,
            sleep_s,
            source="wikipedia",
            source_failures=source_failures,
        )
        if not isinstance(data, dict) or "_error" in data:
            continue

        for item in (data.get("query", {}) or {}).get("search", []) or []:
            title = str(item.get("title", ""))
            snippet = re.sub(r"<[^>]+>", " ", str(item.get("snippet", "")))
            txt = f"{title} {snippet}"
            ys = extract_year_candidates(txt)
            if not ys:
                continue
            result_norm = normalize_for_match(title)
            score = fuzz.token_set_ratio(target_title_norm, result_norm)
            # avoid false positives from generic/biography pages
            if score < 72:
                continue
            common_tokens = set(target_title_norm.split()) & set(result_norm.split())
            if len(common_tokens) < 2:
                continue
            if score > best_score:
                best_score = score
                best_title = title
                found_years = ys

    if found_years:
        best_year = str(max(found_years) if year_strategy == "edition" else min(found_years))

    if not best_year:
        return None

    return BookMeta(
        meta.path,
        title=meta.title,
        authors=meta.authors,
        year=best_year,
        isbn=meta.isbn,
        source=f"wikipedia:{best_title or 'search'}",
        confidence=0.55,
    )


def best_web_year(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    if not meta.title:
        return None

    base_title = title_variants(meta.title)[0] if title_variants(meta.title) else meta.title
    author = meta.authors[0] if meta.authors else ""
    probes = [
        f"\"{base_title}\" \"{author}\" z-library.sk",
        f"\"{base_title}\" \"{author}\" amazon",
        f"\"{base_title}\" \"{author}\" estante virtual",
        f"\"{base_title}\" \"{author}\" bibliography",
        f"\"{base_title}\" \"{author}\" wikipedia",
    ]

    years: list[int] = []
    for q in probes:
        data = get_json(
            "https://duckduckgo.com/html/",
            {"q": q},
            cache,
            sleep_s,
            source="web",
            source_failures=source_failures,
        )
        if not isinstance(data, str):
            continue
        text = re.sub(r"<[^>]+>", " ", data)
        text = compact_spaces(text)
        years.extend(publication_adjacent_years(text))

    if not years:
        return None

    y = str(max(years) if year_strategy == "edition" else min(years))
    return BookMeta(
        meta.path,
        title=meta.title,
        authors=meta.authors,
        year=y,
        isbn=meta.isbn,
        source="web:duckduckgo(zlib+amazon+estante+wikipedia+bibliography)",
        confidence=0.45,
    )


def best_skoob_year(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    """Tenta ano a partir de resultados do Skoob via DuckDuckGo (site:skoob.com.br).

    A busca direta em skoob.com.br costuma exigir login; snippets indexados
    ainda costumam trazer titulo/autor e ano de edicao.
    """
    if not meta.title:
        return None

    base_title = title_variants(meta.title)[0] if title_variants(meta.title) else meta.title
    author = meta.authors[0] if meta.authors else ""

    probes: list[str]
    if author:
        probes = [
            f'"{base_title}" "{author}" site:skoob.com.br',
            f'"{base_title}" site:skoob.com.br',
        ]
    else:
        probes = [f'"{base_title}" site:skoob.com.br']

    years: list[int] = []
    for q in probes:
        data = get_json(
            "https://duckduckgo.com/html/",
            {"q": q},
            cache,
            sleep_s,
            source="skoob",
            source_failures=source_failures,
        )
        if not isinstance(data, str):
            continue
        text = re.sub(r"<[^>]+>", " ", data)
        text = compact_spaces(text)
        years.extend(publication_adjacent_years(text))
        years.extend(years_near_substrings(text, ("skoob.com.br",)))

    if not years:
        return None

    y = str(max(years) if year_strategy == "edition" else min(years))
    return BookMeta(
        meta.path,
        title=meta.title,
        authors=meta.authors,
        year=y,
        isbn=meta.isbn,
        source="skoob:duckduckgo(site:skoob.com.br)",
        confidence=0.48,
    )


def best_book_catalogs_ddgs_year(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta | None:
    """Ano a partir de snippets do DDG restritos a varios catalogos (sem API/chave).

    Cada grupo em DDG_CATALOG_SITE_GROUPS vira uma busca (site:a OR site:b ...).
    """
    if not meta.title:
        return None

    base_title = title_variants(meta.title)[0] if title_variants(meta.title) else meta.title
    author = meta.authors[0] if meta.authors else ""

    years: list[int] = []
    for domains in DDG_CATALOG_SITE_GROUPS:
        site_clause = "(" + " OR ".join(f"site:{d}" for d in domains) + ")"
        if author:
            q = f'"{base_title}" "{author}" {site_clause}'
        else:
            q = f'"{base_title}" {site_clause}'
        data = get_json("https://duckduckgo.com/html/", {"q": q}, cache, sleep_s)
        if not isinstance(data, str):
            continue
        text = re.sub(r"<[^>]+>", " ", data)
        text = compact_spaces(text)
        years.extend(publication_adjacent_years(text))
        years.extend(years_near_substrings(text, domains))

    if not years:
        return None

    y = str(max(years) if year_strategy == "edition" else min(years))
    return BookMeta(
        meta.path,
        title=meta.title,
        authors=meta.authors,
        year=y,
        isbn=meta.isbn,
        source="catalogs:duckduckgo(worldcat+goodreads+storygraph+lt+bookbrowse+bookbrainz+amazon+isbndb)",
        confidence=0.46,
    )


def enrich_weak_authors_from_web(meta: BookMeta, cache: dict[str, Any], sleep_s: float) -> list[str]:
    authors = meta.authors or []
    if not authors or not authors_need_enrichment(authors):
        return authors

    title = title_variants(meta.title)[0] if title_variants(meta.title) else meta.title
    if not title:
        return authors

    query = f"\"{title}\" " + " ".join(f"\"{a}\"" for a in authors[:3])
    data = get_json(
        "https://duckduckgo.com/html/",
        {"q": query},
        cache,
        sleep_s,
    )
    if not isinstance(data, str):
        return authors

    text = re.sub(r"<[^>]+>", " ", data)
    text = compact_spaces(text)
    enriched: list[str] = []

    for a in authors:
        surname = compact_spaces(a)
        if not surname:
            continue
        # Try initials/full first-name followed by surname in snippets.
        patterns = [
            rf"\b([A-Z][a-z]{{1,20}})\s+({re.escape(surname)})\b",
            rf"\b(([A-Z]\.\s*){{1,3}})\s*({re.escape(surname)})\b",
        ]
        candidate = ""
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                if len(m.groups()) >= 2 and m.group(2):
                    candidate = compact_spaces(f"{m.group(1)} {m.group(2)}")
                elif m.group(0):
                    candidate = compact_spaces(m.group(0))
                if candidate:
                    break
        enriched.append(candidate or surname)

    return dedupe_authors(enriched) or authors


def merge_metadata(
    local: BookMeta,
    remote: BookMeta | None,
    prefer_remote_title: bool = False,
    remote_merge_fields: frozenset[str] | None = None,
    keep_local_metadata: frozenset[str] | None = None,
) -> BookMeta:
    if not remote:
        return local

    rmf = remote_merge_fields if remote_merge_fields is not None else MERGE_METADATA_FIELDS
    klf = keep_local_metadata if keep_local_metadata is not None else frozenset()

    out = BookMeta(local.path)
    local_authors = local.authors or []
    remote_authors = remote.authors or []
    force_remote_core = bool(
        _authors_look_suspicious(local_authors)
        and remote_authors
        and not authors_list_looks_bad(remote_authors)
        and "authors" not in klf
    )

    def local_nonempty_title() -> bool:
        return bool(compact_spaces(local.title or ""))

    def local_nonempty_year() -> bool:
        return bool(compact_spaces(local.year or ""))

    def local_nonempty_isbn() -> bool:
        return bool(compact_spaces(local.isbn or ""))

    def local_nonempty_publisher() -> bool:
        return bool(compact_spaces(local.publisher or ""))

    # --- title ---
    if "title" in klf:
        out.title = (local.title if local_nonempty_title() else "") or (remote.title or "") or ""
        append_note(out, "decision:title=local_keep")
    elif "title" not in rmf:
        out.title = local.title or remote.title or ""
        append_note(out, "decision:title=local_priority")
    else:
        if force_remote_core and remote.title:
            out.title = remote.title
            append_note(out, "decision:title=remote_forced_by_suspicious_local")
        else:
            out.title = (
                remote.title
                if prefer_remote_title and remote.title
                else (local.title or remote.title or "")
            )
            if prefer_remote_title and remote.title:
                append_note(out, "decision:title=remote_preferred")
            else:
                append_note(out, "decision:title=local_if_present")

    # --- authors ---
    if "authors" in klf:
        out.authors = list(local_authors) if local_authors else list(remote_authors or [])
        append_note(out, "decision:authors=local_keep")
    elif "authors" not in rmf:
        out.authors = list(local_authors) if local_authors else list(remote_authors or [])
        append_note(out, "decision:authors=local_priority")
    elif force_remote_core:
        out.authors = list(remote_authors)
        append_note(out, "decision:authors=remote_forced_by_suspicious_local")
    elif (
        local_authors
        and remote_authors
        and not authors_need_enrichment(local_authors)
        and not authors_list_looks_bad(local_authors)
        and not surnames_compatible(local_authors, remote_authors)
    ):
        out.authors = list(local_authors)
        append_note(out, "guardrail: autor remoto bloqueado por incompatibilidade com nome local")
        append_note(out, "decision:authors=local_guardrail_incompatibility")
    elif local_authors and remote_authors and authors_need_enrichment(local_authors):
        if surnames_compatible(local_authors, remote_authors):
            out.authors = list(remote_authors)
            append_note(out, "decision:authors=remote_enrichment_compatible")
        else:
            out.authors = list(local_authors)
            append_note(out, "decision:authors=local_enrichment_incompatible")
    else:
        out.authors = list(local_authors or remote_authors or [])
        append_note(out, "decision:authors=fallback_local_or_remote")

    # --- year ---
    if "year" in klf:
        out.year = (local.year if local_nonempty_year() else "") or (remote.year or "") or ""
        append_note(out, "decision:year=local_keep")
    elif "year" not in rmf:
        out.year = local.year or remote.year or ""
        append_note(out, "decision:year=local_priority")
    else:
        if bool(getattr(local, "filename_paren_year", False)) and local_nonempty_year():
            out.year = local.year
            append_note(out, "decision:year=local_filename_paren")
        else:
            ry = compact_spaces(remote.year or "")
            ly = compact_spaces(local.year or "")
            if ry and ly and is_year_token(ry) and is_year_token(ly):
                ryi = int(ry)
                lyi = int(ly)
                if abs(ryi - lyi) >= 80 or (ryi < 1700 <= lyi):
                    out.year = ly
                    append_note(out, f"guardrail: ano remoto outlier ({ry}) ignorado")
                    append_note(out, "decision:year=local_guardrail_outlier")
                else:
                    out.year = ry
                    append_note(out, "decision:year=remote_with_guardrail_check")
            else:
                if ry and is_year_token(ry) and int(ry) < 1700:
                    out.year = ly or ""
                    append_note(out, f"guardrail: ano remoto muito antigo ({ry}) ignorado")
                    append_note(out, "decision:year=local_guardrail_ancient_remote")
                else:
                    out.year = ry or ly or ""
                    append_note(out, "decision:year=remote_or_local_fallback")

    # --- isbn ---
    if "isbn" in klf:
        out.isbn = (local.isbn if local_nonempty_isbn() else "") or (remote.isbn or "") or ""
    elif "isbn" not in rmf:
        out.isbn = local.isbn or remote.isbn or ""
    else:
        out.isbn = remote.isbn or local.isbn or ""

    # --- publisher ---
    if "publisher" in klf:
        lp = compact_spaces(local.publisher) if local_nonempty_publisher() else ""
        rp = compact_spaces(remote.publisher or "")
        out.publisher = lp or rp
    elif "publisher" not in rmf:
        lp = (local.publisher or "").strip()
        rp = (remote.publisher or "").strip()
        out.publisher = lp or rp
    else:
        lp = (local.publisher or "").strip()
        rp = (remote.publisher or "").strip()
        out.publisher = rp or lp

    out.source = f"{local.source}+{remote.source}"
    out.confidence = max(local.confidence, remote.confidence)

    def _uniq_subjects(seq: list[str]) -> list[str]:
        seen: set[str] = set()
        outl: list[str] = []
        for x in seq:
            c = compact_spaces(str(x))
            k = c.lower()
            if c and k not in seen:
                seen.add(k)
                outl.append(c)
        return outl[:40]

    lsu = list(local.subjects or [])
    rsu = list(remote.subjects or [])
    out.subjects = _uniq_subjects(lsu + rsu)
    out.series = compact_spaces(remote.series) or compact_spaces(local.series)

    notes = []

    if local.notes:
        notes.append(local.notes)

    if remote.notes:
        notes.append(remote.notes)

    out.notes = " | ".join(notes)

    sfx = compact_spaces(
        getattr(local, "filename_extra_suffix", "")
        or getattr(remote, "filename_extra_suffix", "")
    )
    if sfx:
        out.filename_extra_suffix = sfx

    out.filename_paren_year = bool(getattr(local, "filename_paren_year", False))
    out.source_failures = list(getattr(local, "source_failures", []) or []) + list(
        getattr(remote, "source_failures", []) or []
    )

    return out


REVIEW_SCORE_AUTO = 90
REVIEW_SCORE_PROMPT = 70


def compute_match_evidence(local: BookMeta, merged: BookMeta) -> tuple[int, dict[str, str]]:
    """Pontuacao 0-100 e dicionario explicativo (concordancia local vs metadado final)."""
    ev: dict[str, str] = {}
    score = 0

    lt = normalize_for_match(local.title or "")
    mt = normalize_for_match(merged.title or "")
    if lt and mt:
        tr = fuzz.token_set_ratio(lt, mt)
        ev["titulo"] = f"similaridade {tr}%"
        if tr >= 95:
            score += 40
        elif tr >= 85:
            score += 32
        elif tr >= 70:
            score += 22
    elif not lt and mt:
        ev["titulo"] = "titulo local vazio"
        score += 10

    la = normalize_for_match(" ".join(local.authors or []))
    ma = normalize_for_match(" ".join(merged.authors or []))
    if la and ma:
        ar = fuzz.token_set_ratio(la, ma)
        ev["autores"] = f"similaridade {ar}%"
        if ar >= 88:
            score += 30
        elif ar >= 72:
            score += 22
        elif ar < 45:
            score -= 40
            ev["autores"] += "; penalizacao divergencia"
    elif not la and ma:
        score += 12
        ev["autores"] = "autor local vazio"

    li = re.sub(r"[^0-9Xx]", "", local.isbn or "")
    mi = re.sub(r"[^0-9Xx]", "", merged.isbn or "")
    if li and mi and li == mi:
        score += 20
        ev["isbn"] = "local e remoto iguais"
    elif mi and not li:
        score += 10
        ev["isbn"] = "ISBN so remoto"

    lp = normalize_for_match(local.publisher or "")
    mp = normalize_for_match(merged.publisher or "")
    if lp and mp and fuzz.token_set_ratio(lp, mp) >= 80:
        score += 10
        ev["editora"] = "concordancia"

    src = merged.source or ""
    if "+" in src:
        parts = [p for p in src.split("+") if p.strip()]
        if len(parts) >= 2:
            score += 15
            ev["fontes"] = "varias fontes no merge: " + src.replace("+", " + ")

    tit = merged.title or ""
    if re.search(r".+:.+", tit) and len(tit) > 55:
        score -= 25
        ev["titulo"] = (ev.get("titulo", "") + "; possivel subtitulo longo").strip("; ")

    failures = list(getattr(merged, "source_failures", []) or [])
    if failures:
        penalty = min(36, 12 * len(failures))
        score -= penalty
        ev["falhas_fontes"] = "; ".join(
            f"{f.get('source', '?')}={f.get('reason', 'unknown')}" for f in failures[:6]
        )

    score = max(0, min(100, score))
    return score, ev


def _recover_authors_from_google_by_title(
    merged: BookMeta,
    enabled_remote_sources: frozenset[str],
    cache: dict[str, Any],
    sleep_s: float,
    source_failures: list[dict[str, str]] | None = None,
) -> BookMeta:
    """Quando nao ha autor utilizavel mas ha titulo, tenta Google Books so com titulo."""
    if merged.authors and not authors_list_looks_bad(merged.authors):
        return merged
    if not compact_spaces(merged.title or ""):
        return merged
    if "google" not in enabled_remote_sources:
        return merged
    probe = replace(merged, authors=[])
    gb = best_googlebooks(probe, cache, sleep_s, source_failures=source_failures)
    if gb and gb.authors and not authors_list_looks_bad(gb.authors):
        out = replace(merged, authors=list(gb.authors))
        append_note(out, "autor recuperado via Google Books (busca por titulo)")
        return out
    return merged


def lookup_metadata(
    meta: BookMeta,
    enabled_remote_sources: frozenset[str],
    cache: dict[str, Any],
    sleep_s: float,
    prefer_remote_title: bool,
    year_strategy: str = "original",
    skip_author_enrich: bool = False,
    remote_merge_fields: frozenset[str] | None = None,
    keep_local_metadata: frozenset[str] | None = None,
) -> BookMeta:
    remote: BookMeta | None = None
    source_failures: list[dict[str, str]] = []

    def _run_source(source: str, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs, source_failures=source_failures)
        except Exception as exc:
            _register_source_failure(source_failures, source, f"source_error: {type(exc).__name__}: {exc}")
            return None

    if "openlibrary" in enabled_remote_sources:
        remote = _run_source("openlibrary", best_openlibrary, meta, cache, sleep_s, year_strategy=year_strategy)
        if _remote_bibliographic_trustworthy(remote):
            merged = merge_metadata(
                meta,
                remote,
                prefer_remote_title=prefer_remote_title,
                remote_merge_fields=remote_merge_fields,
                keep_local_metadata=keep_local_metadata,
            )
            if not skip_author_enrich and authors_need_enrichment(merged.authors):
                merged.authors = enrich_weak_authors_from_web(merged, cache, sleep_s)
            merged = _recover_authors_from_google_by_title(
                merged, enabled_remote_sources, cache, sleep_s, source_failures=source_failures
            )
            merged.source_failures = list(source_failures)
            if source_failures:
                merged.confidence = max(0.0, merged.confidence - min(0.35, 0.12 * len(source_failures)))
                append_note(
                    merged,
                    "falhas em fontes externas: "
                    + "; ".join(f"{x.get('source')}={x.get('reason')}" for x in source_failures[:5]),
                )
            return merged

    if (not remote or not remote.year) and "google" in enabled_remote_sources:
        gb = _run_source("google", best_googlebooks, meta, cache, sleep_s)

        if not remote:
            remote = gb
        elif gb:
            if not remote.year:
                remote.year = gb.year
            if not remote.authors:
                remote.authors = gb.authors
            rsu = list(remote.subjects or [])
            seen_s = {x.lower() for x in rsu}
            for s in gb.subjects or []:
                c = compact_spaces(str(s))
                if c and c.lower() not in seen_s:
                    seen_s.add(c.lower())
                    rsu.append(c)
            remote.subjects = rsu[:40]
            if gb.series and not compact_spaces(remote.series or ""):
                remote.series = gb.series

    if (not remote or not remote.year) and "skoob" in enabled_remote_sources:
        sk = _run_source("skoob", best_skoob_year, meta, cache, sleep_s, year_strategy=year_strategy)
        if not remote:
            remote = sk
        elif sk and not remote.year:
            remote.year = sk.year

    if (not remote or not remote.year) and "catalogs" in enabled_remote_sources:
        cat = _run_source(
            "catalogs",
            best_book_catalogs_ddgs_year,
            meta,
            cache,
            sleep_s,
            year_strategy=year_strategy,
        )
        if not remote:
            remote = cat
        elif cat and not remote.year:
            remote.year = cat.year

    if (not remote or not remote.year) and "wikipedia" in enabled_remote_sources:
        wk = _run_source("wikipedia", best_wikipedia, meta, cache, sleep_s, year_strategy=year_strategy)
        if not remote:
            remote = wk
        elif wk and not remote.year:
            remote.year = wk.year

    if (not remote or not remote.year) and "web" in enabled_remote_sources:
        web = _run_source("web", best_web_year, meta, cache, sleep_s, year_strategy=year_strategy)
        if not remote:
            remote = web
        elif web and not remote.year:
            remote.year = web.year

    merged = merge_metadata(
        meta,
        remote,
        prefer_remote_title=prefer_remote_title,
        remote_merge_fields=remote_merge_fields,
        keep_local_metadata=keep_local_metadata,
    )
    if not skip_author_enrich and authors_need_enrichment(merged.authors):
        merged.authors = enrich_weak_authors_from_web(merged, cache, sleep_s)
    merged = _recover_authors_from_google_by_title(
        merged, enabled_remote_sources, cache, sleep_s, source_failures=source_failures
    )
    merged.source_failures = list(source_failures)
    if source_failures:
        merged.confidence = max(0.0, merged.confidence - min(0.35, 0.12 * len(source_failures)))
        append_note(
            merged,
            "falhas em fontes externas: "
            + "; ".join(f"{x.get('source')}={x.get('reason')}" for x in source_failures[:5]),
        )
    return merged


def is_acronym_token(t: str) -> bool:
    return bool(re.fullmatch(r"[A-ZÀ-Ý0-9]{2,}", t.strip(".")))


def apply_author_overrides(author: str, overrides: dict[str, str]) -> str | None:
    if author in overrides:
        return overrides[author]

    n = normalize_for_match(author)

    for k, v in overrides.items():
        if normalize_for_match(k) == n:
            return v

    return None


def _sanitize_author_list(authors: list[str] | None) -> list[str]:
    if not authors:
        return []
    return [a for a in authors if compact_spaces(a) and not author_looks_bad(a)]


def authors_for_output(meta: BookMeta) -> list[str]:
    raw = _sanitize_author_list(meta.authors)
    if raw:
        return dedupe_authors(raw)
    if not meta.path:
        return []
    fb = parse_filename_fallback(Path(meta.path))
    return dedupe_authors(_sanitize_author_list(fb.authors))


def title_for_filename(meta: BookMeta) -> str:
    p = Path(meta.path)
    t = compact_spaces(meta.title or "")
    fb = parse_filename_fallback(p)
    aus = authors_for_output(meta)
    if aus and t:
        joined = normalize_for_match(" ".join(aus))
        nt = normalize_for_match(t)
        if nt and joined and fuzz.token_set_ratio(nt, joined) >= 78:
            fb_t = compact_spaces(fb.title or "")
            if fb_t:
                t = fb_t
    if not t:
        t = compact_spaces(fb.title or "") or p.stem
    return compact_spaces(t)


def format_one_author(author: str, overrides: dict[str, str]) -> str:
    author = _strip_catalog_author_life_span(compact_spaces(author))
    author = compact_spaces(re.sub(r"[;:]+", " ", author))
    author = compact_spaces(re.sub(r"\s*,\s*,+", ", ", author)).strip(" ,;:-")

    override = apply_author_overrides(author, overrides)

    if override:
        return override

    if "," in author:
        before, after = [compact_spaces(x) for x in author.split(",", 1)]
        return f"{before.upper()}, {after}" if after else before.upper()

    tokens = author.split()

    if not tokens:
        return ""

    ni = _count_trailing_name_initials(tokens)
    if 1 <= ni <= 4 and len(tokens) >= 2 and ni == len(tokens) - 1:
        surname = " ".join(tokens[:-ni])
        given = " ".join(tokens[-ni:])
        if compact_spaces(surname):
            return f"{surname.upper()}, {given}"

    author_lower = normalize_for_match(author)

    is_institution = (
        len(tokens) > 6
        or any(w in author_lower.split() for w in INSTITUTION_WORDS)
        or is_acronym_token(tokens[0])
    )

    if is_institution:
        return author.upper()

    def _is_particle_token(tok: str) -> bool:
        t = tok.lower().strip(".")
        if t in PARTICLES:
            return True
        if "-" in t and t.split("-")[-1] in PARTICLES:
            return True
        return False

    def _is_ambiguous_person_name(ts: list[str]) -> bool:
        if len(ts) < 2:
            return True
        # Casos com particulas/hifenacoes no miolo frequentemente sao ambiguos
        # sem fonte bibliografica confiavel.
        for mid in ts[1:-1]:
            m = mid.lower().strip(".")
            if m in PARTICLES or ("-" in m and any(x in PARTICLES for x in m.split("-"))):
                return True
        if any(re.fullmatch(r"[A-ZÀ-Ý]\.", t, re.I) for t in ts[:1]):
            return True
        return False

    def _resolve_author_name_parts_from_web(raw: str) -> tuple[str, str] | None:
        key = normalize_for_match(raw)
        if not key:
            return None
        if key in _AUTHOR_CITATION_CACHE:
            return _AUTHOR_CITATION_CACHE[key]
        try:
            session = _get_http_session()
            r = session.get(
                "https://api.crossref.org/works",
                params={"query.author": raw, "rows": 5},
                timeout=6,
            )
            r.raise_for_status()
            data = r.json()
            items = ((data or {}).get("message", {}) or {}).get("items", []) or []
            for it in items[:5]:
                for a in (it.get("author") or [])[:6]:
                    fam = compact_spaces(str(a.get("family") or ""))
                    giv = compact_spaces(str(a.get("given") or ""))
                    if not fam:
                        continue
                    cand = normalize_for_match(f"{giv} {fam}")
                    if cand and (key in cand or cand in key):
                        _AUTHOR_CITATION_CACHE[key] = (fam, giv)
                        return _AUTHOR_CITATION_CACHE[key]
        except Exception:
            pass
        _AUTHOR_CITATION_CACHE[key] = None
        return None

    web_parts = _resolve_author_name_parts_from_web(author) if _is_ambiguous_person_name(tokens) else None
    if web_parts:
        fam, giv = web_parts
        return f"{fam.upper()}, {giv}" if giv else fam.upper()

    # Sem confiança suficiente (nem heurística forte, nem citação confiável),
    # preserva nome completo sem vírgula.
    if _is_ambiguous_person_name(tokens):
        return author

    last_parts = [tokens[-1]]
    i = len(tokens) - 2
    while i >= 0 and _is_particle_token(tokens[i]):
        last_parts.insert(0, tokens[i])
        i -= 1
    surname = " ".join(last_parts)
    given = " ".join(tokens[: i + 1])
    if given:
        return f"{surname.upper()}, {given}"
    return surname.upper()


def format_authors(authors: list[str], overrides: dict[str, str], max_authors: int) -> str:
    authors = [a for a in authors if compact_spaces(a)]

    if not authors:
        return ""

    if max_authors > 0 and len(authors) > max_authors:
        first = format_one_author(authors[0], overrides)
        return f"{first} et al."

    return "; ".join(format_one_author(a, overrides) for a in authors)


_FILENAME_PLACEHOLDER_RE = re.compile(
    r"%(?P<key>AUTHOR|DATE|TITLE|PUBLISHER|FORMAT)%",
    re.I,
)


def unknown_year_placeholder(unknown_year: str, label: str) -> str:
    """Texto do 'ano' quando desconhecido (modo sd); vazio com omit."""
    if unknown_year != "sd":
        return ""
    lab = compact_spaces(label or "")
    if not lab:
        lab = "s.d."
    return safe_filename_part(lab, max_len=48)


def default_filename_stem(
    meta: BookMeta,
    overrides: dict[str, str],
    max_authors: int,
    unknown_year: str,
    unknown_year_label: str = "s.d.",
) -> str:
    """Parte do nome sem extensao no padrao historico: AUTOR - ANO - TITULO."""
    tit = title_for_filename(meta)
    title = safe_filename_part(tit or Path(meta.path).stem, max_len=120)
    afmt = format_authors(authors_for_output(meta), overrides, max_authors)
    author_part = safe_filename_part(afmt, max_len=90) if afmt else ""
    year = meta.year or unknown_year_placeholder(unknown_year, unknown_year_label)

    if author_part and year:
        base = f"{author_part} - {year} - {title}"
    elif author_part:
        base = f"{author_part} - {title}"
    elif year:
        base = f"{year} - {title}"
    else:
        base = title

    base = safe_filename_part(base, max_len=190)
    extra = compact_spaces(getattr(meta, "filename_extra_suffix", "") or "")
    if extra:
        ex = safe_filename_part(normalize_volume_edition_suffix(extra), max_len=72)
        if ex:
            base = safe_filename_part(f"{base} - {ex}", max_len=220)
    return base


def _append_filename_extra_suffix_to_fullname(full_name: str, extra: str) -> str:
    """Com --filename-pattern: acrescenta sufixo vol./ed. antes da extensao."""
    ex = compact_spaces(normalize_volume_edition_suffix(extra or ""))
    if not ex:
        return full_name
    exs = safe_filename_part(ex, max_len=72)
    if not exs:
        return full_name
    p = Path(full_name)
    new_stem = safe_filename_part(f"{p.stem} - {exs}", max_len=200)
    return new_stem + p.suffix.lower()


def _normalize_final_filename(name: str) -> str:
    """Normalizacao final unica para todos os caminhos de nomeacao."""
    p = Path(name)
    stem = compact_spaces(p.stem)
    stem = re.sub(r"\s{2,}", " ", stem)
    stem = re.sub(r"\s*-\s*-\s*", " - ", stem)
    stem = re.sub(r"\s*[-,;:.]{2,}\s*", " - ", stem)
    stem = re.sub(r"^\s*[-,;:.]+\s*", "", stem)
    stem = re.sub(r"\s*[-,;:.]+\s*$", "", stem)
    stem = re.sub(r"\(\s*\)", "", stem)
    stem = compact_spaces(stem).strip(" -.;,:")
    stem = safe_filename_part(stem, max_len=200)
    if not stem or stem == "sem_nome":
        stem = "sem_nome"
    return stem + p.suffix.lower()


def classify_item_kind(path: Path, local: BookMeta, meta: BookMeta) -> tuple[str, float]:
    """Classifica item em book/article/magazine/report/unknown."""
    txt = normalize_for_match(
        " ".join(
            [
                path.stem or "",
                local.title or "",
                meta.title or "",
                meta.publisher or "",
                " ".join(meta.authors or []),
            ]
        )
    )
    if not txt:
        return "unknown", 0.2
    if re.search(r"\b(cia|report|information report|technical report|white paper)\b", txt):
        return "report", 0.85
    if re.search(r"\b(review|journal|proceedings|paper|article)\b", txt):
        return "article", 0.72
    # Revista/periodico: evitar falsos positivos — "vol"/"volume" e comum em livros
    # (cadernos, tomos); em PT \bno.? batia em "no Brasil" (no != numero).
    if re.search(r"\b(revista|magazine|periodico|gazette|fanzine)\b", txt):
        return "magazine", 0.72
    if re.search(r"\b(vol\.?|volume)\b", txt) and re.search(
        r"\b("
        r"journal|magazine|revista|proceedings|issue|iss\.|"
        r"numero|n[uú]mero|"
        r"no\.\s*\d|n[ºo°]\s*\d|#\s*\d+"
        r")\b",
        txt,
    ):
        return "magazine", 0.68
    if (meta.authors and meta.title) or (local.authors and local.title):
        return "book", 0.72
    if meta.title or local.title:
        return "unknown", 0.45
    return "unknown", 0.2


def make_new_filename(
    meta: BookMeta,
    ext: str,
    overrides: dict[str, str],
    max_authors: int,
    unknown_year: str,
    filename_pattern: str = "",
    unknown_year_label: str = "s.d.",
    item_kind: str = "book",
) -> str:
    ext_l = ext.lower()
    if not ext_l.startswith("."):
        ext_l = "." + ext_l

    pattern = compact_spaces(filename_pattern)
    if item_kind in {"article", "magazine", "report"}:
        title_fmt = safe_filename_part(title_for_filename(meta), max_len=150)
        year_fmt = meta.year or (
            unknown_year_placeholder(unknown_year, unknown_year_label)
            if unknown_year == "sd"
            else ""
        )
        if year_fmt:
            return _normalize_final_filename(f"{title_fmt} - {year_fmt}{ext_l}")
        return _normalize_final_filename(f"{title_fmt}{ext_l}")
    if not pattern:
        return _normalize_final_filename(
            default_filename_stem(
                meta, overrides, max_authors, unknown_year, unknown_year_label=unknown_year_label
            )
            + ext_l
        )

    afmt = format_authors(authors_for_output(meta), overrides, max_authors)
    author_fmt = safe_filename_part(afmt, max_len=120) if afmt else ""
    if unknown_year == "sd":
        date_fmt = meta.year or unknown_year_placeholder(unknown_year, unknown_year_label)
    else:
        date_fmt = meta.year or ""
    title_fmt = safe_filename_part(title_for_filename(meta), max_len=140)
    publisher_fmt = safe_filename_part(meta.publisher or "", max_len=100)
    format_fmt = ext_l

    def repl(m: re.Match[str]) -> str:
        k = m.group("key").upper()
        if k == "AUTHOR":
            return author_fmt
        if k == "DATE":
            return date_fmt
        if k == "TITLE":
            return title_fmt
        if k == "PUBLISHER":
            return publisher_fmt
        if k == "FORMAT":
            return format_fmt
        return m.group(0)

    has_format = bool(re.search(r"%FORMAT%", pattern, flags=re.I))
    stem = _FILENAME_PLACEHOLDER_RE.sub(repl, pattern)
    stem = compact_spaces(stem)
    # Quando placeholders ficam vazios, evita artefatos como "- Titulo" ou "Autor -".
    stem = re.sub(r"\s+-\s+-\s+", " - ", stem)
    stem = re.sub(r"^\s*-\s*", "", stem)
    stem = re.sub(r"\s*-\s*$", "", stem)
    stem = compact_spaces(stem).strip(" -")
    stem = safe_filename_part(stem, max_len=200)

    if not stem or stem == "sem_nome":
        return _normalize_final_filename(
            default_filename_stem(
                meta, overrides, max_authors, unknown_year, unknown_year_label=unknown_year_label
            )
            + ext_l
        )

    if not has_format:
        stem = stem + ext_l

    return _normalize_final_filename(
        _append_filename_extra_suffix_to_fullname(stem, meta.filename_extra_suffix or "")
    )


def unique_target(src: Path, filename: str, target_dir: Path, reserved: set[Path]) -> Path:
    safe = safe_filename_part(Path(filename).stem, max_len=200) + Path(filename).suffix.lower()
    target_dir_resolved = _resolved_path(target_dir)
    target = (target_dir / safe).resolve()

    try:
        target.relative_to(target_dir_resolved)
    except ValueError:
        target = (target_dir / ("sem_nome" + Path(filename).suffix.lower())).resolve()

    src_resolved = _resolved_path(src)

    if target == src_resolved:
        return target

    stem = target.stem
    suffix = target.suffix
    n = 2

    while target.exists() or target in reserved:
        target = (target_dir / f"{stem} ({n}){suffix}").resolve()
        n += 1

    reserved.add(target)

    return target


def iter_files(
    folder: Path,
    recursive: bool,
    exclude_dir: Path | None = None,
    allowed_exts: frozenset[str] | None = None,
) -> list[Path]:
    pattern = "**/*" if recursive else "*"
    exclude_dir_resolved = _resolved_path(exclude_dir) if exclude_dir else None
    exts = allowed_exts if allowed_exts is not None else SUPPORTED_EXTS

    def allow_suffix(suf: str) -> bool:
        s = suf.lower()
        if allowed_exts is None:
            return s in SUPPORTED_EXTS and s != ".html"
        return s in exts

    files: list[Path] = []
    for p in folder.glob(pattern):
        try:
            if not p.is_file():
                continue
        except OSError:
            continue
        if not allow_suffix(p.suffix):
            continue
        rp = _resolved_path(p)
        parents = list(rp.parents)
        if exclude_dir_resolved is not None and exclude_dir_resolved in parents:
            continue
        if any(
            (IGNORED_DIR_NAMES and parent.name.lower() in IGNORED_DIR_NAMES)
            or parent.name.lower().endswith("_files")
            for parent in parents
        ):
            continue
        files.append(p)

    return sorted(files, key=lambda p: str(p).lower())


def load_json(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    return {}


def save_json(path: Path, data: dict[str, Any]) -> None:
    """Escrita atomica: grava em ficheiro temporario na mesma pasta e renomeia."""
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    target_dir = path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(target_dir)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            fh.write(payload)
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_catalog_entries(
    output_dir: Path, entries: list[dict[str, Any]], fmt: str
) -> list[Path]:
    """Grava catalog.json e/ou catalog.csv em output_dir (ex.: PASTA/renamed/)."""
    out_paths: list[Path] = []
    if fmt in ("json", "both"):
        jp = output_dir / "catalog.json"
        jp.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
        out_paths.append(jp)
    if fmt in ("csv", "both"):
        cp = output_dir / "catalog.csv"
        fields = [
            "original_path",
            "renamed_path",
            "renamed_filename",
            "status",
            "title",
            "authors",
            "year",
            "isbn",
            "publisher",
            "series",
            "subjects",
            "source",
            "confidence",
            "match_score",
        ]
        with cp.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for e in entries:
                row: dict[str, Any] = {k: e.get(k, "") for k in fields}
                if isinstance(e.get("authors"), list):
                    row["authors"] = "; ".join(e["authors"])
                if isinstance(e.get("subjects"), list):
                    row["subjects"] = "; ".join(e["subjects"])
                w.writerow(_csv_safe_row(row))
        out_paths.append(cp)
    return out_paths


def _file_hash_digest(path: Path, algo: str) -> str:
    h = hashlib.md5() if algo == "md5" else hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _quality_for_dedup_pick(meta: BookMeta) -> tuple[int, int, int, int, int]:
    return (
        1 if compact_spaces(meta.year or "") else 0,
        1 if compact_spaces(meta.publisher or "") else 0,
        1 if compact_spaces(meta.isbn or "") else 0,
        len(compact_spaces(meta.title or "")),
        len(compact_spaces(" ".join(meta.authors or []))),
    )


def run_dedup_hashes(folder: Path, args: argparse.Namespace) -> Path:
    """Agrupa ficheiros com o mesmo MD5/SHA1; gera renamed/duplicates.csv; opcionalmente move duplicados."""
    if folder.name.lower() == "renamed":
        output_dir = folder
    else:
        output_dir = (folder / "renamed").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    files = iter_files(
        folder,
        args.recursive,
        exclude_dir=None,
        allowed_exts=args.ext_filter,
    )
    if args.limit and args.limit > 0:
        files = files[: args.limit]

    algo = getattr(args, "dedup_algorithm", "sha1")
    pairs = build_local_metadata(
        files,
        max_pdf_pages=min(args.effective_max_pdf_pages, 2),
        year_strategy=args.year_strategy,
        jobs=max(1, args.jobs),
    )
    meta_by: dict[Path, BookMeta] = {_resolved_path(p): m for p, m in pairs}

    sizes_by: dict[Path, int] = {}
    for p in files:
        try:
            sizes_by[p] = p.stat().st_size
        except OSError:
            sizes_by[p] = 0

    size_buckets: defaultdict[int, list[Path]] = defaultdict(list)
    for p in files:
        size_buckets[sizes_by[p]].append(p)

    candidates: list[Path] = []
    for sz, plist in size_buckets.items():
        if sz > 0 and len(plist) >= 2:
            candidates.extend(plist)

    buckets: defaultdict[str, list[Path]] = defaultdict(list)
    if candidates:
        n_jobs = max(1, args.jobs)
        if n_jobs == 1:
            for path in candidates:
                try:
                    dig = _file_hash_digest(path, algo)
                except OSError as e:
                    log_warn(f"Nao foi possivel ler {path}: {e}")
                    continue
                buckets[dig].append(path)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as ex:
                fut_to_path = {ex.submit(_file_hash_digest, p, algo): p for p in candidates}
                for fut in concurrent.futures.as_completed(fut_to_path):
                    p = fut_to_path[fut]
                    try:
                        dig = fut.result()
                    except OSError as e:
                        log_warn(f"Nao foi possivel ler {p}: {e}")
                        continue
                    buckets[dig].append(p)

    clusters = [(d, lst) for d, lst in buckets.items() if len(lst) >= 2]
    report_path = output_dir / "duplicates.csv"
    dup_dir = output_dir / "duplicates"
    if args.delete_dups:
        dup_dir.mkdir(parents=True, exist_ok=True)

    def sk_sort(p: Path) -> tuple[tuple[int, int, int, int, int], int]:
        m = meta_by.get(_resolved_path(p), BookMeta(str(p)))
        return (_quality_for_dedup_pick(m), sizes_by.get(p, 0))

    rows: list[dict[str, str]] = []
    for gi, (dig, paths_in_group) in enumerate(clusters, start=1):
        sorted_paths = sorted(paths_in_group, key=sk_sort, reverse=True)
        keeper = sorted_paths[0]
        keeper_bn = keeper.name.lower()

        for p in sorted_paths:
            m = meta_by.get(_resolved_path(p), BookMeta(str(p)))
            sz = str(sizes_by.get(p, 0))
            role = "manter" if p == keeper else "duplicado"
            sim_pct = str(
                round(
                    100.0
                    * difflib.SequenceMatcher(
                        None, keeper_bn, p.name.lower()
                    ).ratio(),
                    1,
                )
            )
            rows.append(
                {
                    "grupo": str(gi),
                    "algoritmo": algo,
                    "digest": dig,
                    "caminho": str(p),
                    "bytes": sz,
                    "titulo": m.title or "",
                    "ano": m.year or "",
                    "editora": m.publisher or "",
                    "funcao": role,
                    "similaridade_nomes_pct": sim_pct,
                }
            )
            if args.delete_dups and p != keeper:
                dest = dup_dir / p.name
                n = 2
                while dest.exists():
                    dest = dup_dir / f"{p.stem} ({n}){p.suffix}"
                    n += 1
                try:
                    p.rename(dest)
                except OSError as e:
                    log_error(f"Erro ao mover duplicado {p}: {e}")

    fields = [
        "grupo",
        "algoritmo",
        "digest",
        "caminho",
        "bytes",
        "titulo",
        "ano",
        "editora",
        "funcao",
        "similaridade_nomes_pct",
    ]
    with report_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow(_csv_safe_row(row))

    if clusters:
        log_info(f"Duplicados por hash ({algo}): {report_path} ({len(rows)} linhas).")
    else:
        log_info(f"Nenhum grupo com hash identico. CSV vazio (cabecalho): {report_path}")
    if args.delete_dups and clusters:
        log_info(f"Duplicados movidos para: {_resolved_path(dup_dir)}")
    return report_path


def resolve_supplementary_path(raw: Path, folder: Path) -> Path | None:
    """Resolve caminho do ficheiro suplementar: absoluto, cwd, ou relativo a folder."""
    if raw.is_absolute():
        return raw if raw.is_file() else None
    p = raw.expanduser()
    if p.is_file():
        return p.resolve()
    q = (folder / p).expanduser()
    if q.is_file():
        return q.resolve()
    return None


def _authors_from_cell(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [compact_spaces(str(x)) for x in val if compact_spaces(str(x))]
    s = compact_spaces(str(val))
    if not s:
        return []
    if ";" in s:
        return [compact_spaces(x) for x in s.split(";") if compact_spaces(x)]
    if "," in s:
        return [compact_spaces(x) for x in s.split(",") if compact_spaces(x)]
    return [s]


def _norm_path_lookup_key(p: str) -> str:
    try:
        return os.path.normcase(str(Path(p).expanduser().resolve()))
    except OSError:
        return os.path.normcase(compact_spaces(p))


def _row_dict_to_bookmeta(row: dict[str, Any], source_label: str) -> BookMeta | None:
    """Extrai chave de correspondencia e BookMeta a partir de um dict com chaves normalizadas."""
    def _s(key: str) -> str:
        v = row.get(key)
        if v is None:
            return ""
        if isinstance(v, (dict, list)) and key != "authors":
            return ""
        return compact_spaces(str(v))

    path_key = (
        _s("path")
        or _s("original")
        or _s("filepath")
        or _s("file")
        or _s("filename")
    )
    if not path_key:
        return None
    title = _s("title") or _s("titulo")
    year = _s("year") or _s("ano")
    isbn = _s("isbn")
    publisher = _s("publisher") or _s("editora")
    notes = _s("notes") or _s("notas")
    authors = _authors_from_cell(row.get("authors") if row.get("authors") is not None else row.get("autores"))
    series = _s("series") or _s("serie") or _s("coletanea")
    subjects = _authors_from_cell(
        row.get("subjects") if row.get("subjects") is not None else row.get("assuntos")
    )
    return BookMeta(
        path=path_key,
        title=title,
        authors=authors,
        year=year,
        isbn=isbn,
        publisher=publisher,
        series=series,
        subjects=subjects,
        source=f"supplement:{source_label}",
        confidence=1.0,
        notes=notes,
    )


def _normalize_header_map(headers: list[str]) -> dict[str, str]:
    """Mapeia nome de coluna original -> chave canonica."""
    canon = {
        "path": "path",
        "original": "original",
        "filepath": "filepath",
        "file": "file",
        "filename": "filename",
        "titulo": "title",
        "title": "title",
        "autores": "authors",
        "authors": "authors",
        "ano": "year",
        "year": "year",
        "isbn": "isbn",
        "editora": "publisher",
        "publisher": "publisher",
        "notas": "notes",
        "notes": "notes",
        "series": "series",
        "serie": "series",
        "coletanea": "series",
        "subjects": "subjects",
        "assuntos": "subjects",
        "temas": "subjects",
    }
    out: dict[str, str] = {}
    for h in headers:
        k = compact_spaces(h).lower()
        if k in canon:
            out[h] = canon[k]
    return out


def _parse_supplementary_json(data: Any, source_label: str) -> list[BookMeta]:
    out: list[BookMeta] = []
    if isinstance(data, dict):
        if "records" in data and isinstance(data["records"], list):
            data = data["records"]
        elif "files" in data and isinstance(data["files"], list):
            data = data["files"]
        elif data and all(isinstance(v, dict) for v in data.values()):
            for k, v in data.items():
                if not isinstance(v, dict):
                    continue
                d = {str(k2).lower(): v2 for k2, v2 in v.items()}
                d["path"] = compact_spaces(str(k))
                bm = _row_dict_to_bookmeta(d, source_label)
                if bm:
                    out.append(bm)
            return out
        else:
            return out
    if not isinstance(data, list):
        return out
    for item in data:
        if not isinstance(item, dict):
            continue
        row = {str(k).lower(): v for k, v in item.items()}
        bm = _row_dict_to_bookmeta(row, source_label)
        if bm:
            out.append(bm)
    return out


def _dict_rows_from_csv_reader(path: Path, delim: str | None) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig")
    if not text.strip():
        return []
    sample = text[: 4096]
    if delim is None:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
            delim = dialect.delimiter
        except csv.Error:
            delim = ","
    lines = text.splitlines()
    reader = csv.DictReader(lines, delimiter=delim)
    if not reader.fieldnames:
        return []
    hmap = _normalize_header_map(list(reader.fieldnames))
    rows: list[dict[str, str]] = []
    for raw in reader:
        uni: dict[str, str] = {}
        for ok, v in raw.items():
            if ok is None:
                continue
            nk = hmap.get(ok, ok.strip().lower())
            if isinstance(v, str):
                uni[nk] = v.strip()
            else:
                uni[nk] = str(v or "").strip()
        rows.append(uni)
    return rows


class SupplementaryIndex:
    """Indice de metadados extra: correspondencia por caminho resolvido ou por nome do ficheiro."""

    def __init__(
        self, items: list[BookMeta], label: str, base_folder: Path | None = None
    ) -> None:
        self.label = label
        self.by_resolved: dict[str, BookMeta] = {}
        self.by_basename: dict[str, list[BookMeta]] = defaultdict(list)
        for m in items:
            raw = compact_spaces(m.path)
            if not raw:
                continue
            try:
                pr = Path(raw)
                if pr.is_absolute():
                    self.by_resolved[_norm_path_lookup_key(raw)] = m
                elif base_folder is not None:
                    self.by_resolved[_norm_path_lookup_key(str((base_folder / pr).resolve()))] = m
            except OSError:
                pass
            bn = Path(raw).name.lower()
            if bn:
                self.by_basename[bn].append(m)

    def lookup(self, file_path: Path, local: BookMeta | None) -> BookMeta | None:
        try:
            rk = os.path.normcase(str(file_path.resolve()))
        except OSError:
            rk = ""
        if rk and rk in self.by_resolved:
            return self.by_resolved[rk]
        bn = file_path.name.lower()
        cand = self.by_basename.get(bn, [])
        if not cand:
            return None
        if len(cand) == 1:
            return cand[0]
        if local:
            lt = normalize_for_match(local.title or file_path.stem)
            scored: list[tuple[int, BookMeta]] = []
            for c in cand:
                rt = normalize_for_match(c.title or "")
                scored.append((fuzz.token_set_ratio(lt, rt) if rt else 0, c))
            scored.sort(key=lambda t: t[0], reverse=True)
            if scored[0][0] < 55:
                return None
            return scored[0][1]
        return cand[0]


def merge_supplementary_override(base: BookMeta, sup: BookMeta) -> BookMeta:
    """Campos nao vazios no suplemento substituem o metadado ja obtido."""
    au = list(sup.authors or []) if sup.authors and any(compact_spaces(a) for a in sup.authors) else list(base.authors or [])
    ser = (
        compact_spaces(sup.series)
        if compact_spaces(sup.series)
        else compact_spaces(base.series)
    )
    if sup.subjects and any(compact_spaces(x) for x in sup.subjects):
        subs_src = list(sup.subjects)
    else:
        subs_src = list(base.subjects or [])
    seen: set[str] = set()
    subs: list[str] = []
    for x in subs_src:
        c = compact_spaces(str(x))
        k = c.lower()
        if c and k not in seen:
            seen.add(k)
            subs.append(c)
    subs = subs[:40]

    sfx = compact_spaces(
        getattr(sup, "filename_extra_suffix", "") or getattr(base, "filename_extra_suffix", "")
    )

    return BookMeta(
        path=base.path,
        title=compact_spaces(sup.title) or base.title,
        authors=au,
        year=compact_spaces(sup.year) or base.year,
        isbn=compact_spaces(sup.isbn) or base.isbn,
        publisher=compact_spaces(sup.publisher) or base.publisher,
        series=ser,
        subjects=subs,
        source=(
            f"{compact_spaces(base.source)}+supplement(override)"
            if compact_spaces(base.source or "")
            else "supplement(override)"
        ),
        confidence=max(base.confidence, sup.confidence or 1.0),
        notes=" | ".join(x for x in (base.notes, sup.notes) if compact_spaces(x)),
        filename_extra_suffix=sfx,
    )


def apply_supplementary_merged(
    local: BookMeta,
    meta: BookMeta,
    sup_index: SupplementaryIndex | None,
    args: argparse.Namespace,
) -> BookMeta:
    if sup_index is None:
        return meta
    sup = sup_index.lookup(Path(meta.path), local)
    if sup is None:
        return meta
    if getattr(args, "supplementary_mode", "merge") == "override":
        return merge_supplementary_override(meta, sup)
    return merge_metadata(
        meta,
        sup,
        prefer_remote_title=args.prefer_remote_title,
        remote_merge_fields=args.remote_merge_fields,
        keep_local_metadata=args.keep_local_metadata_fields,
    )


def load_supplementary_data(
    path: Path, base_folder: Path | None = None
) -> SupplementaryIndex | None:
    label = str(path)
    ext = path.suffix.lower()
    items: list[BookMeta] = []
    try:
        if ext == ".json":
            raw_j = path.read_text(encoding="utf-8-sig")
            data = json.loads(raw_j)
            items = _parse_supplementary_json(data, path.name)
        elif ext in (".csv", ".txt"):
            delim = "\t" if ext == ".txt" else None
            rows = _dict_rows_from_csv_reader(path, delim)
            for uni in rows:
                bm = _row_dict_to_bookmeta(uni, path.name)
                if bm:
                    items.append(bm)
        else:
            log_warn(
                f"--supplementary-data suporta .json, .csv ou .txt (recebido: {ext or '(sem)'})."
            )
            return None
    except OSError as e:
        log_error(f"Erro ao ler ficheiro suplementar {path}: {e}")
        return None
    except json.JSONDecodeError as e:
        log_error(f"JSON invalido em {path}: {e}")
        return None
    if not items:
        log_warn(f"Nenhum registo util em {path}")
        return None
    return SupplementaryIndex(items, label, base_folder)


def build_local_metadata(
    files: list[Path],
    max_pdf_pages: int,
    year_strategy: str,
    jobs: int = 1,
) -> list[tuple[Path, BookMeta]]:
    if jobs <= 1:
        return [
            (p, read_local_metadata(p, max_pdf_pages=max_pdf_pages, year_strategy=year_strategy))
            for p in files
        ]

    out: list[tuple[Path, BookMeta]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as ex:
        future_map = {
            ex.submit(
                read_local_metadata,
                p,
                max_pdf_pages=max_pdf_pages,
                year_strategy=year_strategy,
            ): p
            for p in files
        }
        for fut in concurrent.futures.as_completed(future_map):
            p = future_map[fut]
            try:
                out.append((p, fut.result()))
            except Exception:
                out.append((p, parse_filename_fallback(p)))
    out.sort(key=lambda item: str(item[0]).lower())
    return out


class _HelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Mostra defaults do argparse e preserva quebras de linha no epilog."""


def _review_band(score: int) -> str:
    if score >= REVIEW_SCORE_AUTO:
        return "auto"
    if score >= REVIEW_SCORE_PROMPT:
        return "review"
    return "doubt"


def interactive_review_item(
    path: Path,
    local: BookMeta,
    meta: BookMeta,
    proposed_name: str,
    output_dir: Path,
    reserved: set[Path],
    overrides: dict[str, str],
    max_authors: int,
    args: argparse.Namespace,
) -> tuple[str, Path, str]:
    """Devolve (novo_nome, alvo, escolha) com escolha em accept|edit|skip|always_author."""
    log_info("\n--- Revisao ---")
    log_info(f"Original:\n  {path.name}\n")
    log_info(f"Sugestao:\n  {proposed_name}\n")
    log_info(f"Pontuacao: {meta.match_score}%")
    band = _review_band(meta.match_score)
    log_info(
        "Faixa: "
        + (
            "automatico (>=90%)"
            if band == "auto"
            else "revisar (70-89%)" if band == "review" else "duvidoso (<70%)"
        )
    )
    log_info(f"Confianca (metadado): {round(100 * meta.confidence)}%")
    log_info(f"Fonte: {meta.source or '(local)'}\n")
    if meta.evidence:
        log_info("Evidencias:")
        for k, v in sorted(meta.evidence.items()):
            log_info(f"  - {k}: {v}")
    suf = path.suffix.lower()
    while True:
        try:
            raw = input(
                "[A]ceitar / [E]ditar nome / [P]ular / [S]empre este autor (aceita e grava override): "
            ).strip()
        except EOFError:
            raw = "A"
        ch = (raw[:1] or "A").upper()
        if ch == "A":
            tgt = unique_target(path, proposed_name, output_dir, reserved)
            return proposed_name, tgt, "accept"
        if ch == "P":
            return path.name, path.resolve(), "skip"
        if ch == "E":
            log_info(
                "Novo nome de ficheiro (com ou sem extensao; se omitir extensao, mantem-se a actual)."
            )
            try:
                ed = input("> ").strip()
            except EOFError:
                ed = ""
            if not ed:
                continue
            p_ed = Path(ed)
            stem = p_ed.stem
            ext_new = p_ed.suffix.lower() or suf
            newn = safe_filename_part(stem, max_len=200) + ext_new
            tgt = unique_target(path, newn, output_dir, reserved)
            return newn, tgt, "edit"
        if ch == "S":
            lk = getattr(args, "review_author_lock", None)
            if lk is not None and local.authors:
                a0 = local.authors[0]
                lk[a0] = "; ".join(
                    format_authors(authors_for_output(meta), overrides, max_authors)
                )
            tgt = unique_target(path, proposed_name, output_dir, reserved)
            return proposed_name, tgt, "always_author"
        log_warn("Opcao invalida (use A, E, P ou S).")


def _resolve_output_dir_for_root(folder: Path) -> Path:
    if folder.name.lower() == "renamed":
        return folder
    return (folder / "renamed").resolve()


def _load_root_inputs(
    folder: Path, args: argparse.Namespace, cache_path: Path
) -> tuple[dict[str, Any], dict[str, Any], SupplementaryIndex | None]:
    overrides_path = Path(args.overrides)
    if not overrides_path.is_absolute():
        overrides_path = folder / overrides_path
    cache = load_json(cache_path)
    overrides = load_json(overrides_path)
    sup_index: SupplementaryIndex | None = None
    if compact_spaces(getattr(args, "supplementary_data", "")):
        sp = resolve_supplementary_path(Path(args.supplementary_data), folder)
        if sp:
            sup_index = load_supplementary_data(sp, folder)
        else:
            log_warn(f"Ficheiro --supplementary-data nao encontrado: {args.supplementary_data}")
    return cache, overrides, sup_index


def _collect_local_pairs_for_root(
    folder: Path, output_dir: Path, args: argparse.Namespace
) -> list[tuple[Path, BookMeta]]:
    exclude_dir = output_dir if output_dir != folder else None
    files = iter_files(
        folder,
        args.recursive,
        exclude_dir=exclude_dir,
        allowed_exts=args.ext_filter,
    )
    if args.limit and args.limit > 0:
        files = files[: args.limit]
    local_pairs = build_local_metadata(
        files,
        max_pdf_pages=args.effective_max_pdf_pages,
        year_strategy=args.year_strategy,
        jobs=max(1, args.jobs),
    )
    if args.only_missing_year:
        local_pairs = [(p, m) for (p, m) in local_pairs if not m.year]
    return local_pairs


def _should_use_offline_lookup(local: BookMeta, path: Path, args: argparse.Namespace) -> bool:
    skip_remote_triplet = filename_triplet_structured_stem(path) and not args.effective_force_remote
    return args.source == "offline" or (local.year and not args.effective_force_remote) or skip_remote_triplet


def _write_plan_csv(plan_path: Path, rows: list[dict[str, str]]) -> None:
    with plan_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "original",
                "novo",
                "status",
                "titulo",
                "autores",
                "ano",
                "isbn",
                "fonte",
                "confianca",
                "pontuacao",
                "evidencias",
                "source_failures",
                "notas",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(_csv_safe_row(row))


def _write_review_needed_csv(
    output_dir: Path, args: argparse.Namespace, review_needed_rows: list[dict[str, str]]
) -> Path | None:
    if not getattr(args, "review", False):
        return None
    review_path_out = output_dir / "review_needed.csv"
    with review_path_out.open("w", encoding="utf-8-sig", newline="") as rf:
        rw = csv.DictWriter(
            rf,
            fieldnames=[
                "original",
                "novo_sugerido",
                "pontuacao",
                "faixa",
                "escolha_revisao",
                "titulo",
                "autores",
                "evidencias",
                "source_failures",
            ],
        )
        rw.writeheader()
        for row in review_needed_rows:
            rw.writerow(_csv_safe_row(row))
    return review_path_out


def _write_deep_review_csv(
    output_dir: Path, args: argparse.Namespace, deep_review_rows: list[dict[str, str]]
) -> Path | None:
    if not getattr(args, "deep_review", False):
        return None
    p = output_dir / "deep_review.csv"
    with p.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "original",
                "novo_sugerido",
                "status",
                "pontuacao",
                "faixa",
                "escolha_revisao",
                "local_titulo",
                "local_autores",
                "local_ano",
                "final_titulo",
                "final_autores",
                "final_ano",
                "fonte",
                "evidencias",
                "source_failures",
                "notas",
            ],
        )
        w.writeheader()
        for row in deep_review_rows:
            w.writerow(_csv_safe_row(row))
    return p


def _write_missing_year_csv(
    output_dir: Path, args: argparse.Namespace, missing_year_rows: list[dict[str, str]]
) -> Path | None:
    if not args.missing_year_log:
        return None
    missing_path = Path(args.missing_year_log)
    if not missing_path.is_absolute():
        missing_path = output_dir / missing_path
    with missing_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "original",
                "novo_com_sd",
                "status_atual",
                "titulo",
                "autores",
                "fonte",
                "source_failures",
                "notas",
            ],
        )
        writer.writeheader()
        for row in missing_year_rows:
            writer.writerow(_csv_safe_row(row))
    return missing_path


def _write_run_summary_md(output_dir: Path, rows: list[dict[str, str]], total: int) -> Path:
    failures_by_source: defaultdict[str, int] = defaultdict(int)
    reasons: defaultdict[str, int] = defaultdict(int)
    ambiguity_patterns: defaultdict[str, int] = defaultdict(int)
    status_counts: defaultdict[str, int] = defaultdict(int)
    for r in rows:
        st = compact_spaces(r.get("status", ""))
        status_counts[st] += 1
        notas = normalize_for_match(r.get("notas", ""))
        if "conservador" in notas:
            ambiguity_patterns["decisao_conservadora"] += 1
        if "kind=unknown" in notas:
            ambiguity_patterns["item_kind_unknown"] += 1
        rawf = r.get("source_failures", "")
        try:
            fl = json.loads(rawf) if rawf else []
        except Exception:
            fl = []
        for f in fl or []:
            src = compact_spaces(str((f or {}).get("source", "?"))) or "?"
            rsn = compact_spaces(str((f or {}).get("reason", "unknown"))) or "unknown"
            failures_by_source[src] += 1
            reasons[rsn] += 1
    lines = [
        "# Run Summary",
        "",
        f"- Total processado: {total}",
        f"- Renomeado: {status_counts.get('renomeado', 0)}",
        f"- Revisao necessaria: {status_counts.get('revisao_necessaria', 0)}",
        f"- Skipped/revisao: {status_counts.get('pulado_revisao', 0)}",
        "",
        "## Falhas externas por fonte",
    ]
    if failures_by_source:
        for k, v in sorted(failures_by_source.items(), key=lambda kv: kv[1], reverse=True):
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- nenhuma")
    lines.append("")
    lines.append("## Top causas de erro")
    if reasons:
        for k, v in sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- nenhuma")
    lines.append("")
    lines.append("## Top padroes de ambiguidade")
    if ambiguity_patterns:
        for k, v in sorted(ambiguity_patterns.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- nenhum")
    p = output_dir / "run_summary.md"
    p.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return p


def _ensure_quarantine_dirs(output_dir: Path, enabled: bool) -> dict[str, Path]:
    q = {
        "originals": output_dir / "originals",
        "failed": output_dir / "failed",
        "converted": output_dir / "converted",
    }
    if enabled:
        for p in q.values():
            p.mkdir(parents=True, exist_ok=True)
    return q


def _copy_to_quarantine_original(path: Path, qdirs: dict[str, Path], enabled: bool) -> Path | None:
    if not enabled:
        return None
    dst = unique_target(path, path.name, qdirs["originals"], set())
    try:
        shutil.copy2(path, dst)
        return dst
    except OSError:
        return None


def _move_to_quarantine_failed(
    path: Path, qdirs: dict[str, Path], enabled: bool, reserved: set[Path]
) -> Path | None:
    if not enabled:
        return None
    dst = unique_target(path, path.name, qdirs["failed"], reserved)
    try:
        path.rename(dst)
        return dst
    except OSError:
        return None


def _load_author_aliases_for_root(folder: Path, args: argparse.Namespace) -> dict[str, str]:
    raw = compact_spaces(getattr(args, "author_aliases", "") or "")
    if not raw:
        return {}
    p = Path(raw)
    if not p.is_absolute():
        p = (folder / p).resolve()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, str] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            kk = normalize_for_match(str(k))
            vv = compact_spaces(str(v))
            if kk and vv:
                out[kk] = vv
    return out


def _apply_author_aliases(meta: BookMeta, local: BookMeta, aliases: dict[str, str]) -> BookMeta:
    if not aliases or not meta.authors:
        return meta
    # Nao sobrescrever autor local forte sem convergencia.
    local_strong = bool(local.authors) and float(local.confidence or 0.0) >= 0.6
    local_norm = {normalize_for_match(a) for a in (local.authors or [])}
    changed = False
    out_auth: list[str] = []
    for a in meta.authors:
        nk = normalize_for_match(a)
        target = aliases.get(nk)
        if target:
            if local_strong and nk not in local_norm:
                out_auth.append(a)
            else:
                out_auth.append(target)
                changed = True
        else:
            out_auth.append(a)
    if changed:
        meta.authors = dedupe_authors(out_auth)
        append_note(meta, "author_alias_applied")
    return meta


def _estimate_sources_cost(sources: frozenset[str]) -> float:
    return round(sum(REMOTE_SOURCE_COST_ESTIMATE.get(s, 0.0002) for s in sources), 6)


def _risk_recommendation(meta: BookMeta, item_kind: str, item_kind_conf: float) -> tuple[str, str]:
    band = _review_band(meta.match_score)
    if item_kind_conf < 0.6:
        return "high", "revisar_manual"
    if band == "duvidoso":
        return "high", "revisar_manual"
    if band == "revisar":
        return "medium", "revisar_manual"
    if item_kind in {"article", "magazine", "report"}:
        return "medium", "nome_conservador"
    return "low", "aplicar_ou_simular"


def _extract_signals_for_item(
    path: Path,
    local: BookMeta,
    args: argparse.Namespace,
    cache: dict[str, Any],
    sup_index: SupplementaryIndex,
    author_aliases: dict[str, str],
) -> tuple[BookMeta, dict[str, Any]]:
    """Fase A: extrai sinais locais/remotos/suplementares."""
    local_norm = prioritize_triplet_filename_over_local(local, path)
    should_use_offline = _should_use_offline_lookup(local_norm, path, args)
    conservative_reasons: list[str] = []
    budget_spent = float(getattr(args, "_estimated_cost_spent", 0.0) or 0.0)
    max_budget = float(getattr(args, "max_estimated_cost", 0.0) or 0.0)
    enabled_sources = frozenset(args.enabled_remote_sources)
    max_calls = int(getattr(args, "max_remote_calls_per_file", 0) or 0)
    if max_calls > 0:
        enabled_sources = frozenset(list(REMOTE_SOURCE_KEYS)[:max_calls]) & enabled_sources
        conservative_reasons.append(f"max_remote_calls_per_file={max_calls}")
    estimated_item_cost = _estimate_sources_cost(enabled_sources)
    if max_budget > 0 and (budget_spent + estimated_item_cost > max_budget):
        should_use_offline = True
        conservative_reasons.append("max_estimated_cost_exceeded")

    t0 = time.monotonic()
    if should_use_offline:
        meta = local_norm
    else:
        meta = lookup_metadata(
            local_norm,
            enabled_sources,
            cache,
            sleep_s=args.effective_sleep,
            prefer_remote_title=args.prefer_remote_title,
            year_strategy=args.year_strategy,
            skip_author_enrich=args.skip_author_enrich,
            remote_merge_fields=args.remote_merge_fields,
            keep_local_metadata=args.keep_local_metadata_fields,
        )
        setattr(args, "_estimated_cost_spent", budget_spent + estimated_item_cost)
    item_elapsed_s = time.monotonic() - t0
    timeout_s = float(getattr(args, "item_timeout_s", 0.0) or 0.0)
    if timeout_s > 0 and item_elapsed_s > timeout_s:
        meta = local_norm
        conservative_reasons.append(f"item_timeout_s_exceeded:{item_elapsed_s:.2f}s")
    meta = apply_supplementary_merged(local_norm, meta, sup_index, args)
    meta = patch_meta_from_filename_if_merged_suspect(path, meta)
    meta = _apply_author_aliases(meta, local_norm, author_aliases)
    ms, evd = compute_match_evidence(local_norm, meta)
    if sup_index and "supplement" in (meta.source or "").lower():
        evd["suplemento"] = f"ficheiro: {Path(sup_index.label).name}"
    meta.match_score = ms
    meta.evidence = evd
    kind, kind_conf = classify_item_kind(path, local_norm, meta)
    if conservative_reasons:
        append_note(meta, "decisao_conservadora: " + ", ".join(conservative_reasons))
    risk_level, recommendation = _risk_recommendation(meta, kind, kind_conf)
    signals = {
        "path": str(path),
        "local": {
            "title": local_norm.title,
            "authors": list(local_norm.authors or []),
            "year": local_norm.year,
            "source": local_norm.source,
        },
        "remote_or_merged": {
            "title": meta.title,
            "authors": list(meta.authors or []),
            "year": meta.year,
            "source": meta.source,
            "confidence": meta.confidence,
            "match_score": meta.match_score,
            "source_failures": list(getattr(meta, "source_failures", []) or []),
            "evidence": dict(meta.evidence or {}),
        },
        "item_kind": kind,
        "item_kind_confidence": kind_conf,
        "used_offline_lookup": should_use_offline,
        "item_elapsed_s": round(item_elapsed_s, 4),
        "estimated_item_cost": estimated_item_cost,
        "risk_level": risk_level,
        "recommendation": recommendation,
        "conservative_reasons": conservative_reasons,
    }
    return meta, signals


def _console_label_for_rename_status(status: str) -> str:
    """Rotulo amigavel no console; CSV/planos continuam com status tecnico (ex.: planejado)."""
    return {
        "planejado": "simulacao",
        "renomeado": "renomeado",
        "igual": "inalterado",
        "pulado_revisao": "pulado_revisao",
    }.get(status, status)


def run_on_root(
    folder: Path, args: argparse.Namespace
) -> tuple[int, int, Path, Path, Path | None, Path | None]:
    """Processa uma pasta-raiz; retorna (..., missing_path|None, review_needed_path|None)."""
    output_dir = _resolve_output_dir_for_root(folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "metadata_cache.json"
    plan_path = output_dir / ("rename_log.csv" if args.apply else "rename_plan.csv")
    cache, overrides, sup_index = _load_root_inputs(folder, args, cache_path)
    author_aliases = _load_author_aliases_for_root(folder, args)
    local_pairs = _collect_local_pairs_for_root(folder, output_dir, args)
    qdirs = _ensure_quarantine_dirs(output_dir, bool(getattr(args, "quarantine", False)))
    phase_artifacts: list[dict[str, Any]] = []

    reserved: set[Path] = set()
    rows: list[dict[str, str]] = []
    missing_year_rows: list[dict[str, str]] = []
    missing_year_count = 0
    review_needed_rows: list[dict[str, str]] = []
    deep_review_rows: list[dict[str, str]] = []
    catalog_entries: list[dict[str, Any]] = []
    q_reserved: set[Path] = set()

    for path, local in local_pairs:
        meta, signals = _extract_signals_for_item(path, local, args, cache, sup_index, author_aliases)
        local = prioritize_triplet_filename_over_local(local, path)
        phase_artifacts.append(signals)
        item_kind = str(signals.get("item_kind") or "book")
        item_kind_conf = float(signals.get("item_kind_confidence") or 0.0)

        eo: dict[str, str] = dict(overrides)
        if getattr(args, "review_author_lock", None):
            eo.update(args.review_author_lock)

        new_name = make_new_filename(
            meta,
            path.suffix,
            eo,
            args.max_authors,
            args.unknown_year,
            filename_pattern=args.filename_pattern,
            unknown_year_label=args.unknown_year_text,
            item_kind=item_kind,
        )

        target = unique_target(path, new_name, output_dir, reserved)
        review_choice = "auto"
        band = _review_band(meta.match_score)
        conservative_flag = "decisao_conservadora" in normalize_for_match(meta.notes or "")
        failure_review_required = (
            bool(getattr(meta, "source_failures", []))
            and band != "auto"
            and getattr(args, "execution_profile", "balanced") != "aggressive"
        )
        weak_kind_review_required = item_kind_conf < 0.6

        review_all = bool(getattr(args, "deep_review", False))
        should_open_review = bool(getattr(args, "review", False)) and (review_all or band != "auto")
        if getattr(args, "execution_profile", "balanced") == "safe":
            should_open_review = True
        if should_open_review:
            reserved.discard(target)
            new_name, target, review_choice = interactive_review_item(
                path,
                local,
                meta,
                new_name,
                output_dir,
                reserved,
                eo,
                args.max_authors,
                args,
            )

        path_resolved = _resolved_path(path)
        target_resolved = _resolved_path(target)
        status = "igual" if target_resolved == path_resolved else "planejado"
        if failure_review_required and status == "planejado":
            status = "revisao_necessaria"
            review_choice = "review_required"
        if weak_kind_review_required and status == "planejado":
            status = "revisao_necessaria"
            review_choice = "review_required_kind_unknown"
        if conservative_flag and status == "planejado":
            status = "revisao_necessaria"
            review_choice = "conservative_fallback"
        if getattr(args, "safe_require_manual", False) and band != "auto" and status == "planejado":
            status = "revisao_necessaria"
            review_choice = "safe_profile_manual_required"
        if review_choice == "skip":
            status = "pulado_revisao"
            new_name = path.name
            reserved.discard(target)
            target = path_resolved
            target_resolved = path_resolved

        if args.apply and status == "planejado":
            try:
                _copy_to_quarantine_original(path, qdirs, bool(getattr(args, "quarantine", False)))
                path.rename(target)
                status = "renomeado"
            except OSError as e:
                status = f"erro: {e}"
        if (
            args.apply
            and bool(getattr(args, "quarantine", False))
            and status == "revisao_necessaria"
            and bool(getattr(meta, "source_failures", []))
        ):
            failed_path = _move_to_quarantine_failed(path, qdirs, True, q_reserved)
            if failed_path is not None:
                status = f"falha_consulta_movida_quarantine: {failed_path}"
        if status.startswith("erro"):
            failed_path = _move_to_quarantine_failed(
                path,
                qdirs,
                bool(getattr(args, "quarantine", False)),
                q_reserved,
            )
            if failed_path is not None:
                status = f"falha_movida_quarantine: {failed_path}"

        rows.append(
            {
                "original": str(path),
                "novo": str(target),
                "status": status,
                "titulo": meta.title,
                "autores": "; ".join(meta.authors or []),
                "ano": meta.year,
                "isbn": meta.isbn,
                "fonte": meta.source,
                "confianca": str(meta.confidence),
                "pontuacao": str(meta.match_score),
                "evidencias": json.dumps(meta.evidence or {}, ensure_ascii=False),
                "source_failures": json.dumps(getattr(meta, "source_failures", []) or [], ensure_ascii=False),
                "notas": compact_spaces(f"{meta.notes} | kind={item_kind} conf={item_kind_conf:.2f}"),
            }
        )

        if getattr(args, "generate_catalog", False):
            catalog_entries.append(
                {
                    "original_path": str(path),
                    "renamed_path": str(target),
                    "renamed_filename": Path(target).name,
                    "status": status,
                    "title": meta.title or "",
                    "authors": list(meta.authors or []),
                    "year": meta.year or "",
                    "isbn": meta.isbn or "",
                    "publisher": meta.publisher or "",
                    "series": meta.series or "",
                    "subjects": list(meta.subjects or []),
                    "source": meta.source or "",
                    "confidence": meta.confidence,
                    "match_score": meta.match_score,
                    "source_failures": list(getattr(meta, "source_failures", []) or []),
                }
            )

        line = f"{_console_label_for_rename_status(status)}: {path.name} -> {Path(target).name}"
        if not args.quiet:
            if getattr(args, "only_review_needed", False):
                if status == "revisao_necessaria":
                    log_info(line)
            else:
                log_info(line)

        if getattr(args, "review", False) and (review_all or band != "auto"):
            review_needed_rows.append(
                {
                    "original": str(path),
                    "novo_sugerido": str(target) if review_choice != "skip" else "",
                    "pontuacao": str(meta.match_score),
                    "faixa": band,
                    "escolha_revisao": review_choice,
                    "titulo": meta.title,
                    "autores": "; ".join(meta.authors or []),
                    "evidencias": json.dumps(meta.evidence or {}, ensure_ascii=False),
                    "source_failures": json.dumps(getattr(meta, "source_failures", []) or [], ensure_ascii=False),
                }
            )
        if review_all:
            deep_review_rows.append(
                {
                    "original": str(path),
                    "novo_sugerido": str(target) if review_choice != "skip" else "",
                    "status": status,
                    "pontuacao": str(meta.match_score),
                    "faixa": band,
                    "escolha_revisao": review_choice,
                    "local_titulo": local.title,
                    "local_autores": "; ".join(local.authors or []),
                    "local_ano": local.year,
                    "final_titulo": meta.title,
                    "final_autores": "; ".join(meta.authors or []),
                    "final_ano": meta.year,
                    "fonte": meta.source,
                    "evidencias": json.dumps(meta.evidence or {}, ensure_ascii=False),
                    "source_failures": json.dumps(getattr(meta, "source_failures", []) or [], ensure_ascii=False),
                    "notas": meta.notes,
                }
            )

        if not meta.year:
            missing_year_count += 1
            missing_name = make_new_filename(
                meta,
                path.suffix,
                eo,
                args.max_authors,
                args.unknown_year,
                filename_pattern=args.filename_pattern,
                unknown_year_label=args.unknown_year_text,
            )
            missing_year_rows.append(
                {
                    "original": str(path),
                    "novo_com_sd": str((output_dir / missing_name).resolve()),
                    "status_atual": status,
                    "titulo": meta.title,
                    "autores": "; ".join(meta.authors or []),
                    "fonte": meta.source,
                    "source_failures": json.dumps(getattr(meta, "source_failures", []) or [], ensure_ascii=False),
                    "notas": meta.notes,
                }
            )

    save_json(cache_path, cache)
    save_json(output_dir / "phase_artifacts.json", {"items": phase_artifacts})
    _write_plan_csv(plan_path, rows)
    summary_path = _write_run_summary_md(output_dir, rows, len(rows))
    review_path_out = _write_review_needed_csv(output_dir, args, review_needed_rows)
    deep_review_path = _write_deep_review_csv(output_dir, args, deep_review_rows)
    missing_path = _write_missing_year_csv(output_dir, args, missing_year_rows)

    if getattr(args, "generate_catalog", False):
        for wp in write_catalog_entries(
            output_dir,
            catalog_entries,
            getattr(args, "catalog_format", "json"),
        ):
            if not args.quiet:
                log_info(f"Catalogo salvo em: {wp}")
    if deep_review_path is not None and not args.quiet:
        log_info(f"Revisao profunda (CSV): {deep_review_path}")
    if not args.quiet:
        log_info(f"Resumo da execucao: {summary_path}")

    return len(rows), missing_year_count, plan_path, cache_path, missing_path, review_path_out


def _deep_analysis_fallback_payload(meta: BookMeta) -> dict[str, Any]:
    band = _review_band(meta.match_score)
    return {
        "risk": f"divergencia local/remoto na faixa {band}",
        "likely_cause": "heuristica de merge/parsing em conflito com metadado remoto",
        "action": "revisar autores/titulo manualmente antes de aplicar",
        "confidence": round(float(meta.confidence or 0.0), 3),
        "notes": "fallback_heuristico_local",
    }


def _coerce_deep_analysis_json(payload: Any, meta: BookMeta) -> dict[str, Any]:
    req = ["risk", "likely_cause", "action", "confidence", "notes"]
    if not isinstance(payload, dict):
        return _deep_analysis_fallback_payload(meta)
    out = dict(payload)
    for k in req:
        if k not in out:
            return _deep_analysis_fallback_payload(meta)
    try:
        out["confidence"] = float(out.get("confidence"))
    except Exception:
        out["confidence"] = float(meta.confidence or 0.0)
    for k in ("risk", "likely_cause", "action", "notes"):
        out[k] = compact_spaces(str(out.get(k, "")))
    return out


def _deep_analysis_ai_for_item(
    path: Path, local: BookMeta, meta: BookMeta, should_use_offline: bool
) -> dict[str, Any]:
    """Gera parecer de IA em JSON estrito; se falhar, usa fallback heuristico local."""
    api_key = compact_spaces(os.getenv("DEEP_ANALYSIS_API_KEY", ""))
    api_url = compact_spaces(
        os.getenv("DEEP_ANALYSIS_API_URL", "https://openrouter.ai/api/v1/chat/completions")
    )
    model = compact_spaces(os.getenv("DEEP_ANALYSIS_MODEL", "openai/gpt-4o-mini"))

    prompt = (
        "Analise tecnicamente este e-book para risco de renomeacao incorreta.\n"
        f"Arquivo: {path.name}\n"
        f"Modo offline usado: {should_use_offline}\n"
        f"Local -> titulo={local.title!r}; autores={local.authors or []}; ano={local.year!r}; fonte={local.source!r}\n"
        f"Final -> titulo={meta.title!r}; autores={meta.authors or []}; ano={meta.year!r}; fonte={meta.source!r}\n"
        f"Match score={meta.match_score}; confianca={meta.confidence}; evidencias={meta.evidence}; "
        f"falhas={getattr(meta, 'source_failures', [])}; notas={meta.notes!r}\n"
        "Responda SOMENTE em JSON valido com campos: "
        '{"risk":"...", "likely_cause":"...", "action":"...", "confidence":0.0, "notes":"..."}'
    )

    if not api_key:
        return _deep_analysis_fallback_payload(meta)

    try:
        session = _get_http_session()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Voce e um analista bibliografico estrito e conciso."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        r = session.post(api_url, headers=headers, json=payload, timeout=20)
        r.raise_for_status()
        data = r.json()
        txt_raw = (
            (((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content")
            or ""
        )
        txt = txt_raw.replace("\r\n", "\n").replace("\r", "\n").strip()
        try:
            parsed = json.loads(txt)
        except Exception:
            _register_source_failure(meta.source_failures, "deep_analysis_ai", "invalid_json_payload")
            return _deep_analysis_fallback_payload(meta)
        return _coerce_deep_analysis_json(parsed, meta)
    except Exception as exc:
        _register_source_failure(meta.source_failures, "deep_analysis_ai", f"request_failed: {type(exc).__name__}")
        out = _deep_analysis_fallback_payload(meta)
        out["notes"] = compact_spaces(f"{out.get('notes')} | falha_ia={type(exc).__name__}: {exc}")
        return out


def run_deep_analysis_on_root(folder: Path, args: argparse.Namespace) -> Path:
    """Analisa item a item (com IA) e grava apenas um Markdown final."""
    output_dir = _resolve_output_dir_for_root(folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_path = output_dir / "deep_analysis.md"
    cache_path = output_dir / "metadata_cache.json"
    cache, _overrides, sup_index = _load_root_inputs(folder, args, cache_path)
    local_pairs = _collect_local_pairs_for_root(folder, output_dir, args)

    lines: list[str] = []
    lines.append(f"# Deep Analysis - {folder}")
    lines.append("")
    lines.append(f"- Arquivos analisados: {len(local_pairs)}")
    lines.append(f"- Revisao interativa: {bool(getattr(args, 'deep_analysis_review', False))}")
    lines.append("")

    for idx, (path, local) in enumerate(local_pairs, start=1):
        local = prioritize_triplet_filename_over_local(local, path)
        should_use_offline = _should_use_offline_lookup(local, path, args)
        if should_use_offline:
            meta = local
        else:
            meta = lookup_metadata(
                local,
                args.enabled_remote_sources,
                cache,
                sleep_s=args.effective_sleep,
                prefer_remote_title=args.prefer_remote_title,
                year_strategy=args.year_strategy,
                skip_author_enrich=args.skip_author_enrich,
                remote_merge_fields=args.remote_merge_fields,
                keep_local_metadata=args.keep_local_metadata_fields,
            )
        meta = apply_supplementary_merged(local, meta, sup_index, args)
        meta = patch_meta_from_filename_if_merged_suspect(path, meta)
        ms, evd = compute_match_evidence(local, meta)
        meta.match_score = ms
        meta.evidence = evd

        ai_payload = _deep_analysis_ai_for_item(path, local, meta, should_use_offline)
        review_note = ""
        if getattr(args, "deep_analysis_review", False):
            try:
                review_note = input(f"[deep-analysis-review] Comentario para '{path.name}' (ENTER para vazio): ").strip()
            except EOFError:
                review_note = ""

        lines.append(f"## {idx}. {path.name}")
        lines.append("")
        lines.append(f"- Score: `{meta.match_score}` | Faixa: `{_review_band(meta.match_score)}`")
        lines.append(f"- Local: autores={local.authors or []}; ano={local.year!r}; titulo={local.title!r}")
        lines.append(f"- Final: autores={meta.authors or []}; ano={meta.year!r}; titulo={meta.title!r}")
        lines.append(f"- Fonte: `{meta.source}`")
        lines.append(f"- Falhas externas: `{getattr(meta, 'source_failures', [])}`")
        lines.append("")
        lines.append("### Parecer IA")
        lines.append("")
        lines.append(f"- risk: {ai_payload.get('risk', '')}")
        lines.append(f"- likely_cause: {ai_payload.get('likely_cause', '')}")
        lines.append(f"- action: {ai_payload.get('action', '')}")
        lines.append(f"- confidence: {ai_payload.get('confidence', '')}")
        lines.append(f"- notes: {ai_payload.get('notes', '')}")
        lines.append("")
        if review_note:
            lines.append("### Revisao manual")
            lines.append("")
            lines.append(f"- {review_note}")
            lines.append("")

    analysis_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return analysis_path


def run_planning_on_root(folder: Path, args: argparse.Namespace) -> tuple[Path, Path]:
    """Somente planejamento: classifica risco e recomenda acao; nao gera nomes finais."""
    output_dir = _resolve_output_dir_for_root(folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "metadata_cache.json"
    cache, _overrides, sup_index = _load_root_inputs(folder, args, cache_path)
    author_aliases = _load_author_aliases_for_root(folder, args)
    local_pairs = _collect_local_pairs_for_root(folder, output_dir, args)
    items: list[dict[str, Any]] = []
    md_lines = ["# Planning Only", "", f"- Arquivos analisados: {len(local_pairs)}", ""]
    for idx, (path, local) in enumerate(local_pairs, start=1):
        meta, signals = _extract_signals_for_item(path, local, args, cache, sup_index, author_aliases)
        risk = str(signals.get("risk_level", "medium"))
        rec = str(signals.get("recommendation", "revisar_manual"))
        item = {
            "path": str(path),
            "risk_level": risk,
            "recommendation": rec,
            "item_kind": signals.get("item_kind"),
            "item_kind_confidence": signals.get("item_kind_confidence"),
            "match_score": meta.match_score,
            "confidence": meta.confidence,
            "notes": meta.notes,
            "source_failures": list(getattr(meta, "source_failures", []) or []),
        }
        items.append(item)
        md_lines.append(f"## {idx}. {path.name}")
        md_lines.append("")
        md_lines.append(f"- risco: `{risk}`")
        md_lines.append(f"- recomendacao: `{rec}`")
        md_lines.append(
            f"- score/conf: `{meta.match_score}` / `{round(float(meta.confidence or 0.0), 3)}`"
        )
        md_lines.append(
            f"- kind: `{signals.get('item_kind')}` ({round(float(signals.get('item_kind_confidence') or 0.0), 3)})"
        )
        md_lines.append("")
    md_path = output_dir / "planning_only.md"
    json_path = output_dir / "planning_only.json"
    md_path.write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    save_json(json_path, {"items": items})
    save_json(cache_path, cache)
    return md_path, json_path


def _partial_file_fingerprint(path: Path, head_bytes: int = 65536) -> str:
    try:
        sz = path.stat().st_size
    except OSError:
        return ""
    h = hashlib.sha256()
    h.update(str(sz).encode("ascii", errors="ignore"))
    try:
        with path.open("rb") as f:
            h.update(f.read(head_bytes))
    except OSError:
        return ""
    return h.hexdigest()[:24]


def _parse_prefer_format_csv(raw: str) -> list[str]:
    out: list[str] = []
    for p in (raw or "").split(","):
        e = p.strip().lower()
        if not e:
            continue
        if not e.startswith("."):
            e = "." + e
        out.append(e)
    return out


def _dup_author_title_key(meta: BookMeta) -> str | None:
    t = normalize_for_match(meta.title or "")
    a = normalize_for_match(" ".join(meta.authors or []))
    if len(t) < 5:
        return None
    return f"at:{a}|{t}"


def _dup_isbn_key(meta: BookMeta) -> str | None:
    d = re.sub(r"[^0-9Xx]", "", meta.isbn or "")
    if len(d) < 10:
        return None
    return f"isbn:{d}"


class _UnionFind:
    def __init__(self) -> None:
        self.p: dict[str, str] = {}
        self.r: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self.p:
            self.p[x] = x
            self.r[x] = 0
            return x
        chain: list[str] = []
        cur = x
        while self.p[cur] != cur:
            chain.append(cur)
            cur = self.p[cur]
        for node in chain:
            self.p[node] = cur
        return cur

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.r[ra] < self.r[rb]:
            ra, rb = rb, ra
        self.p[rb] = ra
        if self.r[ra] == self.r[rb]:
            self.r[ra] += 1


def run_find_duplicates(folder: Path, args: argparse.Namespace) -> Path | None:
    """Agrupa duplicados por ISBN, autor+titulo normalizados e fingerprint parcial; relatorio e opcionalmente move."""
    if folder.name.lower() == "renamed":
        output_dir = folder
    else:
        output_dir = (folder / "renamed").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    files = iter_files(
        folder,
        args.recursive,
        exclude_dir=None,
        allowed_exts=args.ext_filter,
    )
    if args.limit and args.limit > 0:
        files = files[: args.limit]

    pairs = build_local_metadata(
        files,
        max_pdf_pages=args.effective_max_pdf_pages,
        year_strategy=args.year_strategy,
        jobs=max(1, args.jobs),
    )

    uf = _UnionFind()
    resolved_strs: dict[Path, str] = {p: str(_resolved_path(p)) for p, _ in pairs}
    path_strs = list(resolved_strs.values())

    def union_str_list(lst: list[str]) -> None:
        if len(lst) < 2:
            return
        b0 = lst[0]
        for x in lst[1:]:
            uf.union(b0, x)

    isbn_b: defaultdict[str, list[str]] = defaultdict(list)
    at_b: defaultdict[str, list[str]] = defaultdict(list)
    for path, meta in pairs:
        ps = resolved_strs[path]
        ik = _dup_isbn_key(meta)
        if ik:
            isbn_b[ik].append(ps)
        ak = _dup_author_title_key(meta)
        if ak:
            at_b[ak].append(ps)

    for lst in isbn_b.values():
        union_str_list(lst)
    for lst in at_b.values():
        union_str_list(lst)

    fp_b: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)
    for path, _meta in pairs:
        ps = resolved_strs[path]
        try:
            sz = path.stat().st_size
        except OSError:
            continue
        if sz < 50_000:
            continue
        fp = _partial_file_fingerprint(path)
        if not fp:
            continue
        fp_b[fp].append((ps, sz))

    for items in fp_b.values():
        lst_ps = [t[0] for t in items]
        if len(lst_ps) < 2:
            continue
        szmap = {t[0]: t[1] for t in items}
        for ia in range(len(lst_ps)):
            for ib in range(ia + 1, len(lst_ps)):
                a, b = lst_ps[ia], lst_ps[ib]
                s1, s2 = szmap[a], szmap[b]
                mx = max(s1, s2, 1)
                if abs(s1 - s2) / mx <= 0.02:
                    uf.union(a, b)

    comp: dict[str, set[str]] = {}
    for ps in path_strs:
        r = uf.find(ps)
        comp.setdefault(r, set()).add(ps)

    clusters = [frozenset(s) for s in comp.values() if len(s) >= 2]
    if not clusters:
        log_info(
            "Nenhum grupo de duplicados encontrado (ISBN, autor+titulo ou fingerprint+ tamanho)."
        )
        return None

    order = _parse_prefer_format_csv(args.prefer_format)

    def fmt_rank(p: Path) -> int:
        suf = p.suffix.lower()
        try:
            return order.index(suf)
        except ValueError:
            return len(order) + 1

    def sort_key(p: Path) -> tuple[int, int]:
        try:
            sz = p.stat().st_size
        except OSError:
            sz = 0
        prefer_big = args.prefer_larger or not args.prefer_smaller
        return (fmt_rank(p), -sz if prefer_big else sz)

    dr = getattr(args, "duplicates_report", "") or ""
    if not dr.strip():
        report_path = output_dir / "duplicates_report.csv"
    else:
        report_path = Path(dr)
        if not report_path.is_absolute():
            report_path = output_dir / report_path

    dup_dir = folder / "duplicates"
    if args.move_duplicates:
        dup_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str]] = []
    for ci, cl in enumerate(clusters, start=1):
        paths_sorted = sorted((Path(s) for s in cl), key=sort_key)
        keep = paths_sorted[0]
        for dup in paths_sorted[1:]:
            rows.append(
                {
                    "grupo": str(ci),
                    "manter": str(keep),
                    "duplicado": str(dup),
                    "acao": "mover" if args.move_duplicates else "apenas_relatorio",
                }
            )
            if args.move_duplicates:
                dest = dup_dir / dup.name
                n = 2
                while dest.exists():
                    dest = dup_dir / f"{dup.stem} ({n}){dup.suffix}"
                    n += 1
                try:
                    dup.rename(dest)
                except OSError as e:
                    log_error(f"Erro ao mover duplicado {dup}: {e}")

    with report_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["grupo", "manter", "duplicado", "acao"])
        w.writeheader()
        for row in rows:
            w.writerow(_csv_safe_row(row))

    log_info(f"Relatorio de duplicados: {report_path} ({len(rows)} linhas).")
    if args.move_duplicates:
        log_info(f"Arquivos movidos para: {_resolved_path(dup_dir)}")
    return report_path


def _configure_runtime_args(args: argparse.Namespace) -> int | None:
    args.execution_profile = compact_spaces(getattr(args, "execution_profile", "balanced") or "balanced")
    if args.execution_profile not in {"safe", "balanced", "aggressive"}:
        args.execution_profile = "balanced"
    args.safe_require_manual = args.execution_profile == "safe"
    args._estimated_cost_spent = 0.0
    if args.max_remote_calls_per_file < 0:
        args.max_remote_calls_per_file = 0
    if args.max_estimated_cost < 0:
        args.max_estimated_cost = 0.0
    if args.item_timeout_s < 0:
        args.item_timeout_s = 0.0

    if args.execution_profile == "safe":
        args.source = "offline"
        args.force_remote = False
        args.search_speed = 5 if args.search_speed is None else args.search_speed
    elif args.execution_profile == "aggressive":
        args.source = "all"
        args.force_remote = True
        if args.search_speed is None and not args.fast and not args.thorough:
            args.search_speed = 1

    if args.omit_console and args.review:
        log_error("--omit-console nao combina com --review (e preciso ver mensagens no terminal).")
        return 2

    log_set_omit_console(bool(args.omit_console))

    if not _HAS_DEFUSED_XML:
        log_warn(
            "defusedxml nao instalado; XML de EPUB e lido com xml.etree (limite de "
            "tamanho aplicado). Instale 'defusedxml' para defesa adicional contra XML-bomb."
        )

    try:
        sources_parsed = (
            parse_remote_sources_csv(args.sources)
            if compact_spaces(args.sources)
            else None
        )
    except ValueError as e:
        log_error(f"Erro em --sources: {e}")
        return 2

    if args.source == "offline" and sources_parsed is not None:
        log_error("--sources nao combina com --source offline.")
        return 2

    if args.source not in ("offline", "all") and (
        sources_parsed is not None or args.search_speed is not None
    ):
        log_error(
            "--sources e --search-speed exigem --source all (fonte unica legacy e incompativel)."
        )
        return 2

    try:
        args.remote_merge_fields = (
            parse_merge_metadata_csv(args.remote_metadata)
            if compact_spaces(args.remote_metadata)
            else MERGE_METADATA_FIELDS
        )
    except ValueError as e:
        log_error(f"Erro em --remote-metadata: {e}")
        return 2

    try:
        args.keep_local_metadata_fields = (
            parse_merge_metadata_csv(args.keep_local_metadata)
            if compact_spaces(args.keep_local_metadata)
            else frozenset()
        )
    except ValueError as e:
        log_error(f"Erro em --keep-local-metadata: {e}")
        return 2

    args.unknown_year_text = compact_spaces(args.unknown_year_text or "")
    if args.unknown_year == "sd" and not args.unknown_year_text:
        args.unknown_year_text = "s.d."

    if args.omit_date_if_missing:
        args.unknown_year = "omit"

    if args.fast:
        args.effective_sleep = 0.0
        args.effective_max_pdf_pages = max(0, min(args.max_pdf_pages, 1))
        args.effective_force_remote = args.force_remote
        args.skip_author_enrich = True
    elif args.thorough:
        args.effective_sleep = max(args.sleep, 0.35)
        args.effective_max_pdf_pages = min(max(args.max_pdf_pages, 5), 15)
        args.effective_force_remote = args.force_remote
        args.skip_author_enrich = False
    elif args.search_speed is not None:
        spd = args.search_speed
        if spd == 5:
            args.effective_sleep = 0.0
            args.effective_max_pdf_pages = max(0, min(args.max_pdf_pages, 1))
            args.effective_force_remote = args.force_remote
            args.skip_author_enrich = True
        elif spd == 4:
            args.effective_sleep = max(args.sleep, 0.08)
            args.effective_max_pdf_pages = max(0, min(args.max_pdf_pages, 2))
            args.effective_force_remote = args.force_remote
            args.skip_author_enrich = True
        elif spd == 3:
            args.effective_sleep = max(args.sleep, 0.15)
            args.effective_max_pdf_pages = max(0, min(args.max_pdf_pages, 3))
            args.effective_force_remote = args.force_remote
            args.skip_author_enrich = True
        elif spd == 2:
            args.effective_sleep = max(args.sleep, 0.22)
            args.effective_max_pdf_pages = args.max_pdf_pages
            args.effective_force_remote = args.force_remote
            args.skip_author_enrich = False
        else:
            args.effective_sleep = max(args.sleep, 0.35)
            args.effective_max_pdf_pages = min(max(args.max_pdf_pages, 5), 15)
            args.effective_force_remote = args.force_remote
            args.skip_author_enrich = False
    else:
        args.effective_sleep = args.sleep
        args.effective_max_pdf_pages = args.max_pdf_pages
        args.effective_force_remote = args.force_remote
        args.skip_author_enrich = False

    if args.source == "offline":
        args.enabled_remote_sources = frozenset()
    elif sources_parsed is not None:
        args.enabled_remote_sources = sources_parsed
    elif args.fast:
        args.enabled_remote_sources = SEARCH_SPEED_TO_SOURCES[5]
    elif args.thorough:
        args.enabled_remote_sources = ALL_REMOTE_SOURCES
    elif args.search_speed is not None:
        args.enabled_remote_sources = SEARCH_SPEED_TO_SOURCES[args.search_speed]
    elif args.source == "all":
        args.enabled_remote_sources = ALL_REMOTE_SOURCES
    else:
        leg = args.source.lower()
        if leg in ("googlebooks", "google"):
            leg = "google"
        args.enabled_remote_sources = frozenset({leg})

    if compact_spaces(args.exts):
        try:
            args.ext_filter = parse_exts_csv(args.exts)
        except ValueError as e:
            log_error(f"Erro em --exts: {e}")
            return 2
    else:
        args.ext_filter = None
    return None


def _validate_main_modes(args: argparse.Namespace, roots: list[Path]) -> int | None:
    for folder in roots:
        if not folder.exists() or not folder.is_dir():
            log_error(f"Pasta invalida: {folder}")
            return 2

    if args.apply and args.review:
        log_error("--apply e --review nao podem ser usados juntos.")
        return 2
    if args.move_duplicates and not args.find_duplicates:
        log_error("--move-duplicates exige --find-duplicates.")
        return 2
    if args.prefer_larger and args.prefer_smaller:
        log_error("Use apenas uma de --prefer-larger ou --prefer-smaller.")
        return 2
    if args.delete_dups and not args.dedup:
        log_error("--delete-dups exige --dedup.")
        return 2
    if args.dedup and args.find_duplicates:
        log_error("--dedup e --find-duplicates sao mutuamente exclusivos.")
        return 2
    if args.generate_catalog and (args.find_duplicates or args.dedup):
        log_error("--generate-catalog nao pode ser usado com --find-duplicates ou --dedup.")
        return 2
    if args.deep_analysis and (args.apply or args.find_duplicates or args.dedup):
        log_error("--deep-analysis nao combina com --apply, --find-duplicates ou --dedup.")
        return 2
    if args.planning_only and (args.apply or args.find_duplicates or args.dedup or args.deep_analysis):
        log_error("--planning-only nao combina com --apply, dedup/duplicates ou --deep-analysis.")
        return 2
    return None


def _execute_main_flow(args: argparse.Namespace, roots: list[Path]) -> int:
    if args.planning_only:
        n_roots_pl = len(roots)
        for folder in roots:
            if n_roots_pl > 1 and not args.quiet:
                log_info(f"\n--- planning-only: {folder} ---")
            mdp, jsp = run_planning_on_root(folder, args)
            log_info(f"Planejamento salvo em: {mdp}")
            log_info(f"Planejamento (JSON) salvo em: {jsp}")
        return 0

    if args.deep_analysis:
        n_roots_da = len(roots)
        for folder in roots:
            if n_roots_da > 1 and not args.quiet:
                log_info(f"\n--- deep-analysis: {folder} ---")
            analysis_path = run_deep_analysis_on_root(folder, args)
            log_info(f"Analise profunda salva em: {analysis_path}")
        return 0

    if args.find_duplicates:
        if args.apply or args.review:
            log_error("--find-duplicates nao combina com --apply nem com --review.")
            return 2
        n_roots_dup = len(roots)
        for folder in roots:
            if n_roots_dup > 1 and not args.quiet:
                log_info(f"\n--- duplicados: {folder} ---")
            run_find_duplicates(folder, args)
        return 0

    if args.dedup:
        if args.apply or args.review:
            log_error("--dedup nao combina com --apply nem com --review.")
            return 2
        n_roots_ded = len(roots)
        for folder in roots:
            if n_roots_ded > 1 and not args.quiet:
                log_info(f"\n--- dedup (hash): {folder} ---")
            run_dedup_hashes(folder, args)
        return 0

    total_analysed = 0
    total_missing = 0
    n_roots = len(roots)
    for folder in roots:
        if n_roots > 1 and not args.quiet:
            log_info(f"\n--- {folder} ---")
        n_rows, miss, plan_path, cache_path, missing_path, review_path = run_on_root(folder, args)
        total_analysed += n_rows
        total_missing += miss
        log_info(f"\nArquivos analisados (esta pasta): {n_rows}")
        log_info(f"Sem ano identificado (esta pasta): {miss}")
        log_info(f"Plano/log salvo em: {plan_path}")
        log_info(f"Cache salvo em: {cache_path}")
        if missing_path is not None:
            log_info(f"Log de sem-data salvo em: {missing_path}")
        if review_path is not None:
            log_info(f"Revisao sugerida (CSV): {review_path}")

    if n_roots > 1:
        log_info(f"\nTotal em {n_roots} pastas: {total_analysed} arquivos, {total_missing} sem ano.")
    if not args.apply:
        log_info("Simulacao apenas. Para renomear de verdade, rode novamente com --apply.")
        if args.source == "offline" and total_missing > 0:
            log_info(
                "Dica: use --source all para tentar completar anos (Open Library, Google Books, "
                "Skoob, catalogs agregados, Wikipedia e fallback web)."
            )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        prog="renomear_ebooks.py",
        formatter_class=_HelpFormatter,
        description=(
            "Renomeia e-books (EPUB, PDF, MOBI, AZW/AZW3, DJVU) para o padrao:\n"
            "  SOBRENOME, Nome - Ano - Titulo.ext (ou --filename-pattern personalizado)\n\n"
            "Por padrao, so arquivos diretamente em cada PASTA sao listados; use --recursive "
            "para incluir subpastas.\n"
            "Os arquivos renomeados vao para a subpasta 'renamed' dentro de cada pasta informada "
            "(exceto se a propria PASTA for uma pasta chamada 'renamed').\n"
            "Sem --apply, apenas simula e grava rename_plan.csv; com --apply, move/renomeia "
            "e grava rename_log.csv."
        ),
        epilog=(
            "Exemplos:\n"
            "  Simulacao rapida (50 arquivos, sem rede extra se ja houver ano local):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --limit 50 --quiet\n\n"
            "  Sem nenhuma mensagem no terminal (so CSV/cache; [FATAL] ainda aparece):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --omit-console --quiet\n\n"
            "  Simulacao completa com busca de ano na rede:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --sleep 0.25\n\n"
            "  Varias pastas (cada uma com seu proprio renamed/):\n"
            "    python renomear_ebooks.py \"D:\\A\" \"D:\\B\" --recursive --quiet\n\n"
            "  So itens sem ano (apos leitura local), com log:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --only-missing-year "
            "--missing-year-log sem_data.csv --quiet\n\n"
            "  Aplicar de verdade:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --apply\n\n"
            "  Sempre ir a rede (revalidar remoto mesmo com ano local) + so ano do remoto:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --force-remote "
            "--remote-metadata year --keep-local-metadata authors,title\n\n"
            "  Modo rapido (menos rede e menos leitura de PDF):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --fast --quiet\n\n"
            "  Modo minucioso (mais PDF + todas as fontes; rede opcional se ja houver ano local):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --thorough\n\n"
            "  Velocidade de busca 1 a 5 (so um de --fast, --thorough ou --search-speed):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --search-speed 3 --quiet\n\n"
            "  Escolher fontes manualmente (com --source all):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all "
            "--sources openlibrary,google,wikipedia --quiet\n\n"
            "  Nome de ficheiro personalizado:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --filename-pattern "
            "\"%DATE%_%AUTHOR% - %TITLE%%FORMAT%\" --quiet\n\n"
            "  Sem segmento de ano quando a data nao existir (AUTOR - Titulo):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --omit-date-if-missing --quiet\n\n"
            "  Revisao interactiva (metadado duvidoso):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --review\n\n"
            "  Detectar duplicados (relatorio em renamed/):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --find-duplicates --recursive\n\n"
            "  Metadado adicional (CSV/JSON junto ao catalogo):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --supplementary-data metadados_extra.csv --quiet\n\n"
            "  Catalogo para Calibre / relatorios (apos simulacao ou apply):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --generate-catalog --catalog-format both --quiet\n\n"
            "  Duplicados por hash de conteudo:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --dedup --dedup-algorithm sha1 --recursive\n\n"
            "Overrides de autores: arquivo JSON (chave = como aparece no metadado/nome; "
            "valor = formato desejado), padrao author_overrides.json na pasta-alvo.\n"
            "Veja README.md na mesma pasta do script para detalhes."
        ),
    )

    ap.add_argument(
        "folders",
        metavar="PASTA",
        nargs="+",
        help=(
            "Uma ou mais pastas com e-books. Sem --recursive, so entram arquivos no nivel "
            "imediatamente dentro de cada PASTA (nao desce em subpastas)."
        ),
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Aplica renomeacoes e move arquivos para PASTA/renamed/ de cada raiz. Sem esta flag, so gera o CSV de plano.",
    )
    ap.add_argument(
        "--review",
        action="store_true",
        help=(
            "Revisao interactiva item a item quando a pontuacao de concordancia local/remoto for "
            "70-89 (revisar) ou <70 (duvidoso): aceitar, editar nome, pular ou gravar override do "
            "autor na sessao. Gera review_needed.csv. Incompativel com --apply."
        ),
    )
    ap.add_argument(
        "--deep-review",
        action="store_true",
        help=(
            "Revisao interactiva aprofundada de TODOS os itens (nao apenas os duvidosos). "
            "Ativa automaticamente --review e gera deep_review.csv com comparativo local vs final."
        ),
    )
    ap.add_argument(
        "--execution-profile",
        choices=["safe", "balanced", "aggressive"],
        default="balanced",
        help=(
            "Perfil pronto de execucao (preset de comportamento): "
            "safe (sem rede e revisao forte), "
            "balanced (equilibrado), aggressive (mais remoto e mais tolerante a fallback)."
        ),
    )
    ap.add_argument(
        "--quarantine",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Quarentena operacional (padrao: ativa): cria originals/, failed/ e converted/ em renamed/; "
            "faz backup pre-renomeio e move falhas para failed/. Use --no-quarantine para desativar."
        ),
    )
    ap.add_argument(
        "--persist-intermediate",
        action="store_true",
        help="Persiste artefato da fase de extracao->decisao em renamed/phase_artifacts.json.",
    )
    ap.add_argument(
        "--max-remote-calls-per-file",
        type=int,
        default=0,
        metavar="N",
        help="Limita quantas fontes remotas podem ser usadas por arquivo (0 = sem limite).",
    )
    ap.add_argument(
        "--max-estimated-cost",
        type=float,
        default=0.0,
        metavar="VALOR",
        help="Limite de custo estimado por lote para consultas remotas/IA (0 = sem limite).",
    )
    ap.add_argument(
        "--item-timeout-s",
        type=float,
        default=0.0,
        metavar="SEGUNDOS",
        help="Timeout total por item; se exceder, cai para decisao conservadora local (0 = sem limite).",
    )
    ap.add_argument(
        "--planning-only",
        action="store_true",
        help="Somente planejamento: classifica risco e recomendacao sem gerar nome final.",
    )
    ap.add_argument(
        "--author-aliases",
        default="",
        metavar="ARQUIVO.json",
        help="JSON opcional de aliases canonicos de autor (chave -> valor canonicizado).",
    )
    ap.add_argument(
        "--deep-analysis",
        action="store_true",
        help=(
            "Executa analise aprofundada item a item (IA quando configurada) e grava apenas "
            "deep_analysis.md ao final."
        ),
    )
    ap.add_argument(
        "--deep-analysis-review",
        action="store_true",
        help=(
            "Com --deep-analysis, pede comentario manual por item e inclui no Markdown final."
        ),
    )
    ap.add_argument(
        "--recursive",
        action="store_true",
        help="Inclui arquivos em todas as subpastas de cada PASTA (sem esta flag, so o nivel raiz).",
    )
    ap.add_argument(
        "--exts",
        default="",
        metavar="EXTS",
        help=(
            "Lista separada por virgula de extensoes a incluir (ex.: pdf,epub ou .PDF,.epub). "
            "Case-insensitive; com ou sem ponto. So tipos suportados entram; sem esta flag, "
            "usa o conjunto padrao e ignora .html."
        ),
    )

    ap.add_argument(
        "--source",
        choices=["offline", "openlibrary", "google", "skoob", "catalogs", "wikipedia", "web", "all"],
        default="all",
        help=(
            "Fonte(s) para completar metadado remoto (principalmente ano). "
            "Padrao: all. "
            "'offline' nao acessa a rede. "
            "'all' tenta Open Library, Google Books, Skoob (DDG site:skoob.com.br), "
            "catalogos agregados (DDG site: worldcat, goodreads, storygraph, librarything, "
            "bookbrowse, bookbrainz, amazon, isbndb), Wikipedia e busca web (fallback). "
            "Se o ano ja foi encontrado na leitura local, a rede e pulada salvo --force-remote "
            "(ou --fetch-remote-always). "
            "Se o nome do ficheiro ja estiver em AUTOR - ANO - TITULO (ou ANO - AUTOR - TITULO, "
            "incl. s.d. no lugar do ano), a rede e igualmente pulada salvo --force-remote. "
            "Combine com --sources ou --search-speed (exige --source all)."
        ),
    )

    ap.add_argument(
        "--sources",
        default="",
        metavar="LISTA",
        help=(
            "Filtra quais fontes remotas usar (virgula). Valores: openlibrary, google, skoob, "
            "catalogs, wikipedia, web. Ex.: openlibrary,google,wikipedia. Exige --source all. "
            "Tem precedencia sobre --search-speed e sobre o subconjunto implicito de --fast."
        ),
    )

    ap.add_argument(
        "--prefer-remote-title",
        action="store_true",
        help="Substitui titulo local pelo titulo retornado pela API (pode divergir da edicao que voce tem).",
    )

    ap.add_argument(
        "--remote-metadata",
        default="",
        metavar="CAMPOS",
        help=(
            "Apos a busca remota, quais campos podem ser atualizados a partir do resultado remoto "
            "(lista separada por virgula). Valores: title, authors, year, isbn, publisher "
            "(aliases: date, ano, author, autor, autores, titulo, editora). "
            "Sem esta flag: todos os campos podem receber remoto. Ver tambem --keep-local-metadata."
        ),
    )
    ap.add_argument(
        "--keep-local-metadata",
        default="",
        metavar="CAMPOS",
        help=(
            "Campos em que preservar o valor local quando ja preenchido (virgula); o remoto nao "
            "substitui. Mesmos nomes que --remote-metadata. Ex.: authors,title"
        ),
    )

    ap.add_argument(
        "--max-authors",
        type=int,
        default=3,
        help="Numero maximo de autores no nome do arquivo; acima disso usa 'et al.'. Use 0 para listar todos.",
    )

    ap.add_argument(
        "--unknown-year",
        choices=["sd", "omit"],
        default="sd",
        help=(
            "Como preencher o ano quando desconhecido: 'sd' insere um placeholder (ver "
            "--unknown-year-text); 'omit' omite o segmento de ano. Ver tambem --omit-date-if-missing."
        ),
    )
    ap.add_argument(
        "--omit-date-if-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Sem ano identificado, nao inclui a parte da data no nome (padrao: ativo; ex.: AUTOR - Titulo.ext). "
            "Com ano, mantem AUTOR - ANO - TITULO. Equivale a --unknown-year omit. "
            "Use --no-omit-date-if-missing para voltar ao placeholder de data desconhecida."
        ),
    )
    ap.add_argument(
        "--unknown-year-text",
        default="s.d.",
        metavar="TEXTO",
        help=(
            "Texto do placeholder de ano quando desconhecido (so com --unknown-year sd). "
            "Padrao: s.d.. Caracteres invalidos para nome de ficheiro sao normalizados. "
            "Ignorado com omit. Se vazio apos limpar, volta a s.d.."
        ),
    )
    ap.add_argument(
        "--filename-pattern",
        default="",
        metavar="PADRAO",
        help=(
            "Modelo do nome do ficheiro em renamed/. Marcadores (case-insensitive): "
            "%%AUTHOR%%, %%DATE%% (ano ou --unknown-year-text se sd), %%TITLE%%, "
            "%%PUBLISHER%% (ex.: metadado EPUB), %%FORMAT%% (extensao com ponto, ex. .epub). "
            "Se nao usar %%FORMAT%%, a extensao e acrescentada no fim. "
            "Sem esta flag: padrao SOBRENOME, Nome - Ano - Titulo.ext"
        ),
    )
    ap.add_argument(
        "--year-strategy",
        choices=["original", "edition"],
        default="original",
        help=(
            "Quando ha varios anos candidatos (APIs, texto): "
            "'original' prefere o mais antigo plausivel; "
            "'edition' prefere o mais recente (ex.: reimpressao)."
        ),
    )

    ap.add_argument(
        "--max-pdf-pages",
        type=int,
        default=3,
        help="Quantas paginas iniciais do PDF extrair para ISBN/ano (custo de CPU).",
    )

    ap.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="Pausa em segundos entre requisicoes HTTP (evita limitar APIs).",
    )

    perf = ap.add_mutually_exclusive_group()
    perf.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Prioriza velocidade: pausa HTTP efetiva 0s; le no maximo 1 pagina de PDF por arquivo; "
            "sem --sources, equivale a --search-speed 5 (Open Library + Google Books; pula Skoob, "
            "catalogs, Wikipedia, web e enriquecimento extra de autores). Com --sources, mantem "
            "pausa/PDF rapidos mas consulta so as fontes listadas."
        ),
    )
    perf.add_argument(
        "--thorough",
        action="store_true",
        help=(
            "Prioriza consistencia: pausa HTTP maior (min. 0.35s); le no minimo 5 paginas de PDF "
            "(ate 15); com --source all usa todas as fontes (Skoob, catalogs, Wikipedia, web). "
            "Nao forca por si a rede se ja houver ano local; use --force-remote para isso. "
            "Equivale a --search-speed 1 no ritmo de rede/PDF (sem alterar --force-remote)."
        ),
    )
    perf.add_argument(
        "--search-speed",
        type=int,
        choices=[1, 2, 3, 4, 5],
        default=None,
        metavar="N",
        help=(
            "Velocidade da busca remota (1 = mais lenta, mais fontes; 5 = mais rapida, so "
            "Open Library + Google Books). Ajusta pausa HTTP, leitura de PDF e enriquecimento "
            "de autores. Nao ativa por si --force-remote. Incompativel com --fast e --thorough."
        ),
    )

    ap.add_argument(
        "--overrides",
        default="author_overrides.json",
        help="JSON de overrides de autor: se caminho relativo, resolve em relacao a cada PASTA.",
    )
    ap.add_argument(
        "--supplementary-data",
        default="",
        metavar="ARQUIVO",
        help=(
            "Ficheiro .json, .csv ou .txt (TSV com cabecalho) com metadado extra por ficheiro. "
            "Identificacao: path, original, filepath, file ou filename (caminho absoluto, relativo "
            "a PASTA, ou so o nome do ficheiro). Campos: title/titulo, authors/autores, year/ano, "
            "isbn, publisher/editora, notes/notas. O caminho do ficheiro resolve no cwd e na PASTA. "
            "JSON: lista de objetos, {\"records\": [...]} ou mapa caminho -> objeto."
        ),
    )
    ap.add_argument(
        "--supplementary-mode",
        choices=["merge", "override"],
        default="merge",
        help=(
            "merge: junta o suplemento como mais uma fonte (respeita --remote-metadata e "
            "--keep-local-metadata). override: cada campo preenchido no suplemento substitui "
            "o metadado ja obtido."
        ),
    )
    ap.add_argument(
        "--missing-year-log",
        nargs="?",
        const="missing_years.csv",
        default="",
        metavar="ARQUIVO.csv",
        help=(
            "Gera CSV apenas dos itens sem ano apos metadado final: colunas original, "
            "novo_com_sd (nome planejado com a mesma regra de --unknown-year / --omit-date-if-missing), "
            "titulo, autores, etc. "
            "Sem nome apos a flag: usa missing_years.csv em cada PASTA/renamed/. "
            "Com varias PASTAs, evite um unico caminho absoluto comum (cada raiz gravaria o mesmo arquivo)."
        ),
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Por pasta: no maximo os N primeiros arquivos da lista (ordem alfabetica). 0 = sem limite.",
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=1,
        metavar="N",
        help="Threads para leitura local paralela (PDF/EPUB). Aumente em SSD/CPU forte; 1 e o mais seguro.",
    )
    ap.add_argument(
        "--only-missing-year",
        action="store_true",
        help="Filtra para arquivos cujo metadado LOCAL nao trouxe ano (antes da etapa remota).",
    )
    ap.add_argument(
        "--only-review-needed",
        action="store_true",
        help=(
            "No console, imprime apenas linhas com status revisao_necessaria (nao altera CSVs "
            "nem o processamento). Respeita --quiet."
        ),
    )
    ap.add_argument(
        "--force-remote",
        "--fetch-remote-always",
        action="store_true",
        dest="force_remote",
        help=(
            "Sempre executa a fase de busca remota (rede), mesmo quando a leitura local ja "
            "trouxer ano ou outros dados uteis, ou quando o nome do ficheiro ja estiver em "
            "AUTOR-ANO-TITULO (ou ANO-AUTOR-TITULO). Sem esta flag, com ano local ou nome "
            "estruturado assim, a rede e ignorada. Independente de --thorough / --search-speed. "
            "Apos a busca, --remote-metadata e --keep-local-metadata controlam o merge no metadado final."
        ),
    )
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Nao imprime linha a linha no console (o CSV e resumo final continuam).",
    )
    ap.add_argument(
        "--omit-console",
        action="store_true",
        help=(
            "Nao escreve mensagens no terminal ([INFO]/[WARN]/[ERROR]); CSVs, caches e ficheiros "
            "gerados mantem-se. [FATAL] ainda e impresso. Incompativel com --review."
        ),
    )
    ap.add_argument(
        "--generate-catalog",
        action="store_true",
        help=(
            "No fim do processamento (simulacao ou --apply), grava catalog.json e/ou catalog.csv "
            "em PASTA/renamed/ com titulo, autores, ano, ISBN, editora, series (ex.: EPUB Calibre), "
            "subjects (Open Library / Google Books / EPUB), caminho original e nome planejado."
        ),
    )
    ap.add_argument(
        "--catalog-format",
        choices=["json", "csv", "both"],
        default="json",
        help="Com --generate-catalog: formato do ficheiro em renamed/.",
    )

    dup = ap.add_argument_group("duplicados")
    dup.add_argument(
        "--find-duplicates",
        action="store_true",
        help=(
            "Apenas detecta grupos de possiveis duplicados (ISBN, autor+titulo normalizado, "
            "fingerprint parcial + tamanho semelhante). Gera CSV; nao renomeia pela logica normal."
        ),
    )
    dup.add_argument(
        "--duplicates-report",
        nargs="?",
        const="duplicates_report.csv",
        default="",
        metavar="ARQUIVO.csv",
        help=(
            "Nome ou caminho do CSV de duplicados. Relativo a PASTA/renamed/ se nao for absoluto. "
            "Com --find-duplicates sem este flag: usa duplicates_report.csv em renamed/."
        ),
    )
    dup.add_argument(
        "--move-duplicates",
        action="store_true",
        help="Com --find-duplicates: move copias nao preferidas para PASTA/duplicates/.",
    )
    dup.add_argument(
        "--prefer-format",
        default="epub,pdf,azw3,azw,mobi,djvu",
        metavar="LISTA",
        help="Ordem de preferencia de extensao para escolher a copia a manter (lista separada por virgula).",
    )
    dup.add_argument(
        "--prefer-larger",
        action="store_true",
        help="Com --find-duplicates: prefere o ficheiro maior (comportamento por defeito se nao usar --prefer-smaller).",
    )
    dup.add_argument(
        "--prefer-smaller",
        action="store_true",
        help="Com --find-duplicates: prefere o ficheiro menor entre candidatos equiparados.",
    )
    dup.add_argument(
        "--dedup",
        action="store_true",
        help=(
            "Apenas detecta ficheiros com o mesmo conteudo (hash MD5 ou SHA1 de ficheiro completo). "
            "Gera renamed/duplicates.csv com grupo, digest, caminhos e similaridade de nomes (difflib). "
            "Distinto de --find-duplicates (metadado / fingerprint parcial)."
        ),
    )
    dup.add_argument(
        "--dedup-algorithm",
        choices=["md5", "sha1"],
        default="sha1",
        help="Algoritmo para --dedup (le o ficheiro inteiro; pode ser lento em bibliotecas grandes).",
    )
    dup.add_argument(
        "--delete-dups",
        action="store_true",
        help=(
            "Com --dedup: por grupo mantem a copia com metadado mais completo e maior tamanho; "
            "move as outras para renamed/duplicates/."
        ),
    )

    args = ap.parse_args()
    args.review_author_lock = {}
    if args.deep_review:
        args.review = True
    if args.deep_analysis_review:
        args.deep_analysis = True
    rc = _configure_runtime_args(args)
    if rc is not None:
        return rc
    roots = [Path(f).expanduser().resolve() for f in args.folders]
    rc = _validate_main_modes(args, roots)
    if rc is not None:
        return rc
    return _execute_main_flow(args, roots)


if __name__ == "__main__":
    raise SystemExit(main())
