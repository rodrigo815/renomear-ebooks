from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import logging
import re
import sys
import time
import urllib.parse
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from rapidfuzz import fuzz

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


SUPPORTED_EXTS = {".epub", ".pdf", ".mobi", ".azw", ".azw3", ".djvu"}
IGNORED_DIR_NAMES = {"livros stalin", "anarquismo"}

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
}

BAD_TITLE_WORDS = {
    "unknown", "untitled", "document", "scan", "scanner", "converted",
}


@dataclass
class BookMeta:
    path: str
    title: str = ""
    authors: list[str] | None = None
    year: str = ""
    isbn: str = ""
    source: str = ""
    confidence: float = 0.0
    notes: str = ""

    def __post_init__(self) -> None:
        if self.authors is None:
            self.authors = []


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
    return s.strip(" .-_")


def append_note(meta: BookMeta, note: str) -> None:
    note = compact_spaces(note)
    if not note:
        return
    if not meta.notes:
        meta.notes = note
    elif note not in meta.notes:
        meta.notes += f" | {note}"


def author_looks_bad(author: str) -> bool:
    a = compact_spaces(author)
    if not a:
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


def authors_list_looks_bad(authors: list[str] | None) -> bool:
    if not authors:
        return True
    return all(author_looks_bad(a) for a in authors)


def dedupe_authors(authors: list[str]) -> list[str]:
    out: list[str] = []
    norms: list[str] = []
    for a in authors:
        a = compact_spaces(a)
        if not a:
            continue
        n = normalize_for_match(a)
        if not n:
            continue
        if any(fuzz.token_set_ratio(n, prev) >= 94 for prev in norms):
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

    s = re.sub(r"[\x00-\x1f]", "", s)
    s = re.sub(r"\s+", " ", s).strip(" .")

    if len(s) > max_len:
        s = s[:max_len].rstrip(" .-_")

    return s or "sem_nome"


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


def split_authors(raw: str | list[str] | None) -> list[str]:
    if not raw:
        return []

    if isinstance(raw, list):
        items = raw
    else:
        text = compact_spaces(raw)
        text = re.sub(r"\s+(?:and|e|&)\s+", ";", text, flags=re.I)
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

    for item in items:
        item = compact_spaces(item)
        if item and item not in out:
            out.append(item)

    return out


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


def parse_filename_fallback(path: Path) -> BookMeta:
    stem = compact_spaces(path.stem)
    year = year_from_string(stem)
    title = stem
    authors: list[str] = []

    m = re.match(
        r"^(.+?)\s+-\s+((?:1[4-9]\d{2}|20\d{2}|s\.d\.))\s+-\s+(.+)$",
        stem,
        re.I,
    )

    if m:
        authors = split_authors(m.group(1))
        year = "" if m.group(2).lower() == "s.d." else m.group(2)
        title = m.group(3)
        return BookMeta(str(path), clean_title(title), authors, year, source="filename", confidence=0.35)

    m = re.match(r"^((?:1[4-9]\d{2}|20\d{2}))\s+-\s+(.+?)\s+-\s+(.+)$", stem)

    if m:
        year = m.group(1)
        authors = split_authors(m.group(2))
        title = m.group(3)
        return BookMeta(str(path), clean_title(title), authors, year, source="filename", confidence=0.35)

    m = re.match(r"^(.+?)\s*\(([^()]+)\)$", stem)

    if m:
        title = m.group(1)
        authors = split_authors(m.group(2))
        return BookMeta(str(path), clean_title(title), authors, year, source="filename", confidence=0.25)

    parts = re.split(r"\s+-\s+", stem, maxsplit=1)

    if len(parts) == 2:
        left, right = parts

        if re.search(r"[A-Za-zÀ-ÿ]", left) and len(left.split()) <= 6:
            authors = split_authors(left)
            title = right

    title = re.sub(r"\b(1[4-9]\d{2}|20\d{2})\b", " ", title)

    return BookMeta(str(path), clean_title(title), authors, year, source="filename", confidence=0.15)


def read_epub_metadata(path: Path) -> BookMeta:
    meta = BookMeta(str(path), source="epub", confidence=0.5)

    try:
        with zipfile.ZipFile(path) as z:
            container_xml = z.read("META-INF/container.xml")
            root = ET.fromstring(container_xml)

            ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
            rootfile = root.find(".//c:rootfile", ns)

            if rootfile is None:
                raise ValueError("rootfile ausente no EPUB")

            opf_path = rootfile.attrib["full-path"]
            opf = ET.fromstring(z.read(opf_path))

            def texts(tag: str) -> list[str]:
                vals = []

                for el in opf.iter():
                    if el.tag.endswith("}" + tag) or el.tag == tag:
                        if el.text and compact_spaces(el.text):
                            vals.append(compact_spaces(el.text))

                return vals

            titles = texts("title")
            creators = texts("creator")
            identifiers = texts("identifier")
            dates = texts("date")

            if titles:
                meta.title = clean_title(titles[0])

            if creators:
                meta.authors = split_authors(creators)

            if identifiers:
                meta.isbn = find_isbn(" ".join(identifiers))

            if dates:
                meta.year = year_from_string(" ".join(dates))

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

        text = ""

        for page in reader.pages[:max_pages]:
            try:
                text += "\n" + (page.extract_text() or "")
            except Exception:
                pass

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
        for a in candidate_authors:
            na = normalize_for_match(a)
            if not any(fuzz.token_set_ratio(na, normalize_for_match(b)) >= 90 for b in merged):
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

    if not meta.year:
        meta.year = fallback.year

    if not meta.source or meta.source == "unsupported":
        meta.source = fallback.source

    meta.confidence = max(meta.confidence, fallback.confidence)

    if fallback.notes and not meta.notes:
        meta.notes = fallback.notes

    return meta


def cache_key(url: str, params: dict[str, Any] | None) -> str:
    raw = url + "?" + urllib.parse.urlencode(params or {}, doseq=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_json(url: str, params: dict[str, Any] | None, cache: dict[str, Any], sleep_s: float) -> Any:
    key = cache_key(url, params)

    if key in cache:
        return cache[key]

    try:
        time.sleep(sleep_s)

        r = requests.get(
            url,
            params=params,
            timeout=20,
            headers={"User-Agent": "ebook-renamer/1.0"},
        )

        r.raise_for_status()
        ctype = (r.headers.get("Content-Type", "") or "").lower()
        if "json" in ctype:
            data = r.json()
        else:
            data = r.text
        cache[key] = data
        return data

    except Exception as e:
        cache[key] = {"_error": str(e)}
        return cache[key]


def best_openlibrary(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
) -> BookMeta | None:
    if meta.isbn:
        data = get_json(f"https://openlibrary.org/isbn/{meta.isbn}.json", None, cache, sleep_s)

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
        data = get_json("https://openlibrary.org/search.json", params, cache, sleep_s)
        if not isinstance(data, dict) or "_error" in data:
            continue
        candidates = data.get("docs", []) or []

        for doc in candidates:
            dt = normalize_for_match(str(doc.get("title", "")))
            da = normalize_for_match(" ".join(doc.get("author_name", [])[:5]))

            t_score = fuzz.token_set_ratio(target_title, dt) if target_title and dt else 0
            a_score = fuzz.token_set_ratio(target_author, da) if target_author and da else 60

            score = 0.78 * t_score + 0.22 * a_score

            if t_score >= 60 and (not target_author or a_score >= 36):
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
        source="openlibrary:search",
        confidence=round(score / 100, 3),
    )


def best_googlebooks(meta: BookMeta, cache: dict[str, Any], sleep_s: float) -> BookMeta | None:
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
        )

        if not isinstance(data, dict) or "_error" in data:
            continue

        items = data.get("items", []) or []
        for item in items:
            info = item.get("volumeInfo", {})
            title = normalize_for_match(info.get("title", ""))
            authors = normalize_for_match(" ".join(info.get("authors", [])[:5]))

            t_score = fuzz.token_set_ratio(target_title, title) if target_title and title else 0
            a_score = fuzz.token_set_ratio(target_author, authors) if target_author and authors else 60

            score = 0.78 * t_score + 0.22 * a_score

            if t_score >= 58 and (not target_author or a_score >= 34):
                if best is None or score > best[0]:
                    best = (score, info)

    if not best:
        return None

    score, info = best
    year = year_from_string(str(info.get("publishedDate", "")))

    return BookMeta(
        meta.path,
        title=clean_title(info.get("title", "") or meta.title),
        authors=split_authors(info.get("authors", [])[:3]) or meta.authors,
        year=year,
        isbn=meta.isbn,
        source="googlebooks",
        confidence=round(score / 100, 3),
    )


def best_wikipedia(
    meta: BookMeta,
    cache: dict[str, Any],
    sleep_s: float,
    year_strategy: str = "original",
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

    def contextual_years(text: str) -> list[int]:
        patterns = [
            r"(?i)(?:publication date|published|released|launch(?:ed)?|copyright|©|data de publica[cç][aã]o|publicado|lan[cç]amento)[^\n]{0,90}\b(1[4-9]\d{2}|20\d{2})\b",
            r"(?i)\b(1[4-9]\d{2}|20\d{2})\b[^\n]{0,90}(?:publication date|published|released|launch(?:ed)?|copyright|©|data de publica[cç][aã]o|publicado|lan[cç]amento)",
        ]
        out: list[int] = []
        for pat in patterns:
            for m in re.finditer(pat, text):
                out.extend(extract_year_candidates(m.group(0)))
        return out

    years: list[int] = []
    for q in probes:
        data = get_json(
            "https://duckduckgo.com/html/",
            {"q": q},
            cache,
            sleep_s,
        )
        if not isinstance(data, str):
            continue
        text = re.sub(r"<[^>]+>", " ", data)
        text = compact_spaces(text)
        years.extend(contextual_years(text))

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


def enrich_weak_authors_from_web(meta: BookMeta, cache: dict[str, Any], sleep_s: float) -> list[str]:
    authors = meta.authors or []
    if not authors or not authors_need_enrichment(authors):
        return authors

    title = title_variants(meta.title)[0] if title_variants(meta.title) else meta.title
    if not title:
        return authors

    query = f"\"{title}\" " + " ".join(f"\"{a}\"" for a in authors[:3])
    data = get_json("https://duckduckgo.com/html/", {"q": query}, cache, sleep_s)
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


def merge_metadata(local: BookMeta, remote: BookMeta | None, prefer_remote_title: bool = False) -> BookMeta:
    if not remote:
        return local

    out = BookMeta(local.path)

    out.title = remote.title if prefer_remote_title and remote.title else (local.title or remote.title)
    local_authors = local.authors or []
    remote_authors = remote.authors or []
    if local_authors and remote_authors and authors_need_enrichment(local_authors):
        if surnames_compatible(local_authors, remote_authors):
            out.authors = remote_authors
        else:
            out.authors = local_authors
    else:
        out.authors = local_authors or remote_authors or []
    out.year = remote.year or local.year
    out.isbn = local.isbn or remote.isbn
    out.source = f"{local.source}+{remote.source}"
    out.confidence = max(local.confidence, remote.confidence)

    notes = []

    if local.notes:
        notes.append(local.notes)

    if remote.notes:
        notes.append(remote.notes)

    out.notes = " | ".join(notes)

    return out


def lookup_metadata(
    meta: BookMeta,
    source: str,
    cache: dict[str, Any],
    sleep_s: float,
    prefer_remote_title: bool,
    year_strategy: str = "original",
) -> BookMeta:
    source = source.lower()
    remote: BookMeta | None = None

    if source in {"openlibrary", "all"}:
        remote = best_openlibrary(meta, cache, sleep_s, year_strategy=year_strategy)

    if (not remote or not remote.year) and source in {"google", "googlebooks", "all"}:
        gb = best_googlebooks(meta, cache, sleep_s)

        if not remote:
            remote = gb
        elif gb and not remote.year:
            remote.year = gb.year

            if not remote.authors:
                remote.authors = gb.authors

    if (not remote or not remote.year) and source in {"wikipedia", "all"}:
        wk = best_wikipedia(meta, cache, sleep_s, year_strategy=year_strategy)
        if not remote:
            remote = wk
        elif wk and not remote.year:
            remote.year = wk.year

    if (not remote or not remote.year) and source in {"web", "all"}:
        web = best_web_year(meta, cache, sleep_s, year_strategy=year_strategy)
        if not remote:
            remote = web
        elif web and not remote.year:
            remote.year = web.year

    merged = merge_metadata(meta, remote, prefer_remote_title=prefer_remote_title)
    if authors_need_enrichment(merged.authors):
        merged.authors = enrich_weak_authors_from_web(merged, cache, sleep_s)
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


def format_one_author(author: str, overrides: dict[str, str]) -> str:
    author = compact_spaces(author)

    override = apply_author_overrides(author, overrides)

    if override:
        return override

    if "," in author:
        before, after = [compact_spaces(x) for x in author.split(",", 1)]
        return f"{before.upper()}, {after}" if after else before.upper()

    tokens = author.split()

    if not tokens:
        return ""

    author_lower = normalize_for_match(author)

    is_institution = (
        len(tokens) > 6
        or any(w in author_lower.split() for w in INSTITUTION_WORDS)
        or is_acronym_token(tokens[0])
    )

    if is_institution:
        return author.upper()

    last_parts = [tokens[-1]]
    i = len(tokens) - 2

    while i >= 0 and tokens[i].lower().strip(".") in PARTICLES:
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


def make_new_filename(
    meta: BookMeta,
    ext: str,
    overrides: dict[str, str],
    max_authors: int,
    unknown_year: str,
) -> str:
    title = safe_filename_part(meta.title or Path(meta.path).stem, max_len=120)
    author_part = safe_filename_part(format_authors(meta.authors or [], overrides, max_authors), max_len=90)
    year = meta.year or ("s.d." if unknown_year == "sd" else "")

    if author_part and year:
        base = f"{author_part} - {year} - {title}"
    elif author_part:
        base = f"{author_part} - {title}"
    elif year:
        base = f"{year} - {title}"
    else:
        base = title

    base = safe_filename_part(base, max_len=190)

    return base + ext.lower()


def unique_target(src: Path, filename: str, target_dir: Path, reserved: set[Path]) -> Path:
    target = (target_dir / filename).resolve()
    src_resolved = src.resolve()

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


def iter_files(folder: Path, recursive: bool, exclude_dir: Path | None = None) -> list[Path]:
    pattern = "**/*" if recursive else "*"
    exclude_dir_resolved = exclude_dir.resolve() if exclude_dir else None

    def should_ignore_path(p: Path) -> bool:
        for parent in p.resolve().parents:
            name_lower = parent.name.lower()
            if name_lower in IGNORED_DIR_NAMES or name_lower.endswith("_files"):
                return True
        return False

    files = [
        p for p in folder.glob(pattern)
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_EXTS
        and p.suffix.lower() != ".html"
        and (
            exclude_dir_resolved is None
            or exclude_dir_resolved not in p.resolve().parents
        )
        and not should_ignore_path(p)
    ]

    return sorted(files, key=lambda p: str(p).lower())


def load_json(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    return {}


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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


def main() -> int:
    ap = argparse.ArgumentParser(
        prog="renomear_ebooks.py",
        formatter_class=_HelpFormatter,
        description=(
            "Renomeia e-books (EPUB, PDF, MOBI, AZW/AZW3, DJVU) para o padrao:\n"
            "  SOBRENOME, Nome - Ano - Titulo.ext\n\n"
            "Os arquivos renomeados vao para a subpasta 'renamed' dentro da pasta informada "
            "(exceto se voce ja apontar diretamente para uma pasta chamada 'renamed').\n"
            "Sem --apply, apenas simula e grava rename_plan.csv; com --apply, move/renomeia "
            "e grava rename_log.csv."
        ),
        epilog=(
            "Exemplos:\n"
            "  Simulacao rapida (50 arquivos, sem rede extra se ja houver ano local):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --limit 50 --quiet\n\n"
            "  Simulacao completa com busca de ano na rede:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --sleep 0.25\n\n"
            "  So itens sem ano (apos leitura local), com log:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --only-missing-year "
            "--missing-year-log sem_data.csv --quiet\n\n"
            "  Aplicar de verdade:\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --apply\n\n"
            "  Forcar rede mesmo quando o PDF ja trouxe um ano (revalidar):\n"
            "    python renomear_ebooks.py \"C:\\Livros\" --source all --force-remote\n\n"
            "Overrides de autores: arquivo JSON (chave = como aparece no metadado/nome; "
            "valor = formato desejado), padrao author_overrides.json na pasta-alvo.\n"
            "Veja README.md na mesma pasta do script para detalhes."
        ),
    )

    ap.add_argument(
        "folder",
        metavar="PASTA",
        help="Pasta raiz que contem os e-books (nao use a subpasta 'renamed' como raiz salvo se quiser ler da biblioteca principal).",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Aplica renomeacoes e move arquivos para PASTA/renamed/. Sem esta flag, so gera o CSV de plano.",
    )
    ap.add_argument(
        "--recursive",
        action="store_true",
        help="Inclui subpastas ao listar arquivos (glob recursivo).",
    )

    ap.add_argument(
        "--source",
        choices=["offline", "openlibrary", "google", "wikipedia", "web", "all"],
        default="all",
        help=(
            "Fonte(s) para completar metadado remoto (principalmente ano). "
            "'offline' nao acessa a rede. "
            "'all' tenta Open Library + Google Books + Wikipedia + busca web (fallback). "
            "Se o ano ja foi encontrado na leitura local, a rede e pulada salvo --force-remote."
        ),
    )

    ap.add_argument(
        "--prefer-remote-title",
        action="store_true",
        help="Substitui titulo local pelo titulo retornado pela API (pode divergir da edicao que voce tem).",
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
        help="Como preencher o ano quando desconhecido: 'sd' insere s.d.; 'omit' omite o segmento de ano.",
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

    ap.add_argument(
        "--overrides",
        default="author_overrides.json",
        help="Caminho para JSON de overrides de autor (relativo a PASTA se nao for absoluto).",
    )
    ap.add_argument(
        "--missing-year-log",
        nargs="?",
        const="missing_years.csv",
        default="",
        metavar="ARQUIVO.csv",
        help=(
            "Gera CSV apenas dos itens sem ano apos metadado final: colunas original, "
            "novo_com_sd (nome planejado forcando s.d.), titulo, autores, etc. "
            "Sem nome apos a flag: usa missing_years.csv em PASTA/renamed/."
        ),
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Processa no maximo os N primeiros arquivos da lista (ordem alfabetica). 0 = sem limite.",
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
        "--force-remote",
        action="store_true",
        help="Sempre chama fontes remotas mesmo se o ano ja existir na leitura local.",
    )
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Nao imprime linha a linha no console (o CSV e resumo final continuam).",
    )

    args = ap.parse_args()

    folder = Path(args.folder).expanduser().resolve()

    if not folder.exists() or not folder.is_dir():
        print(f"Pasta invalida: {folder}", file=sys.stderr)
        return 2

    if folder.name.lower() == "renamed":
        output_dir = folder
    else:
        output_dir = (folder / "renamed").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "metadata_cache.json"
    plan_path = output_dir / ("rename_log.csv" if args.apply else "rename_plan.csv")

    overrides_path = Path(args.overrides)

    if not overrides_path.is_absolute():
        overrides_path = folder / overrides_path

    cache = load_json(cache_path)
    overrides = load_json(overrides_path)

    exclude_dir = output_dir if output_dir != folder else None
    files = iter_files(folder, args.recursive, exclude_dir=exclude_dir)
    if args.limit and args.limit > 0:
        files = files[: args.limit]

    local_pairs = build_local_metadata(
        files,
        max_pdf_pages=args.max_pdf_pages,
        year_strategy=args.year_strategy,
        jobs=max(1, args.jobs),
    )
    if args.only_missing_year:
        local_pairs = [(p, m) for (p, m) in local_pairs if not m.year]

    reserved: set[Path] = set()
    rows: list[dict[str, str]] = []
    missing_year_rows: list[dict[str, str]] = []
    missing_year_count = 0

    for path, local in local_pairs:
        should_use_offline = (
            args.source == "offline"
            or (local.year and not args.force_remote)
        )
        if should_use_offline:
            meta = local
        else:
            meta = lookup_metadata(
                local,
                args.source,
                cache,
                sleep_s=args.sleep,
                prefer_remote_title=args.prefer_remote_title,
                year_strategy=args.year_strategy,
            )

        new_name = make_new_filename(
            meta,
            path.suffix,
            overrides,
            args.max_authors,
            args.unknown_year,
        )

        target = unique_target(path, new_name, output_dir, reserved)

        status = "igual" if target.resolve() == path.resolve() else "planejado"

        if args.apply and status != "igual":
            try:
                path.rename(target)
                status = "renomeado"
            except Exception as e:
                status = f"erro: {e}"

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
                "notas": meta.notes,
            }
        )

        line = f"{status}: {path.name} -> {target.name}"
        if not args.quiet:
            try:
                print(line)
            except UnicodeEncodeError:
                safe_line = line.encode("cp1252", errors="replace").decode("cp1252", errors="replace")
                print(safe_line)

        if not meta.year:
            missing_year_count += 1
            missing_name = make_new_filename(
                meta,
                path.suffix,
                overrides,
                args.max_authors,
                "sd",
            )
            missing_year_rows.append(
                {
                    "original": str(path),
                    "novo_com_sd": str((output_dir / missing_name).resolve()),
                    "status_atual": status,
                    "titulo": meta.title,
                    "autores": "; ".join(meta.authors or []),
                    "fonte": meta.source,
                    "notas": meta.notes,
                }
            )

    save_json(cache_path, cache)

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
                "notas",
            ],
        )

        writer.writeheader()
        writer.writerows(rows)

    if args.missing_year_log:
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
                    "notas",
                ],
            )
            writer.writeheader()
            writer.writerows(missing_year_rows)

    print(f"\nArquivos analisados: {len(rows)}")
    print(f"Sem ano identificado: {missing_year_count}")
    print(f"Plano/log salvo em: {plan_path}")
    print(f"Cache salvo em: {cache_path}")
    if args.missing_year_log:
        print(f"Log de sem-data salvo em: {missing_path}")

    if not args.apply:
        print("Simulacao apenas. Para renomear de verdade, rode novamente com --apply.")
        if args.source == "offline" and missing_year_count > 0:
            print(
                "Dica: use --source all para tentar completar anos (Open Library, Google Books, "
                "Wikipedia e fallback web)."
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())