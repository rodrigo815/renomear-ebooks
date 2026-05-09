"""Regressao: livros nao podem ser classificados como periodico por 'vol' ou pela palavra 'no'."""
from __future__ import annotations

from pathlib import Path

import renomear_ebooks as re


def test_parse_grundrisse_boitempo_parenthetical_is_publisher_not_author() -> None:
    p = Path("Karl Marx - Grundrisse (Boitempo).pdf")
    m = re.parse_filename_fallback(p)
    joined_a = " ".join(a.lower() for a in (m.authors or []))
    assert "marx" in joined_a
    assert "boitempo" not in joined_a
    assert (m.publisher or "").lower() == "boitempo"
    assert "grundrisse" in (m.title or "").lower()


def test_classify_cadernos_volume_is_book_not_magazine() -> None:
    p = Path("Antonio Gramsci - Cadernos do cárcere - vol 1.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2012",
        source="test",
    )
    kind, _conf = re.classify_item_kind(p, fb, meta)
    assert kind == "book"


def test_make_new_filename_cadernos_includes_author() -> None:
    p = Path("Antonio Gramsci - Cadernos do cárcere - vol 2.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2015",
        source="test",
        filename_extra_suffix=fb.filename_extra_suffix or "",
    )
    kind, _ = re.classify_item_kind(p, fb, meta)
    out = re.make_new_filename(meta, ".pdf", {}, 3, "omit", item_kind=kind)
    assert "GRAMSCI" in out.upper()


def test_classify_florestan_no_brasil_is_book_not_magazine() -> None:
    p = Path("Florestan Fernandes - A Revolução Burguesa no Brasil.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2013",
        source="test",
    )
    kind, _conf = re.classify_item_kind(p, fb, meta)
    assert kind == "book"


def test_make_new_filename_florestan_includes_author() -> None:
    p = Path("Florestan Fernandes - A Revolução Burguesa no Brasil.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2013",
        source="test",
    )
    kind, _ = re.classify_item_kind(p, fb, meta)
    out = re.make_new_filename(meta, ".pdf", {}, 3, "omit", item_kind=kind)
    assert "FERNANDES" in out.upper()


def test_classify_gramsci_conceicao_is_book() -> None:
    p = Path("Gramsci - Concepção Dialética da História.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2012",
        source="test",
    )
    kind, _conf = re.classify_item_kind(p, fb, meta)
    assert kind == "book"


def test_make_new_filename_gramsci_conceicao_author_first() -> None:
    p = Path("Gramsci - Concepção Dialética da História.pdf")
    fb = re.parse_filename_fallback(p)
    meta = re.BookMeta(
        str(p),
        title=fb.title or "",
        authors=list(fb.authors or []),
        year="2012",
        source="test",
    )
    kind, _ = re.classify_item_kind(p, fb, meta)
    out = re.make_new_filename(meta, ".pdf", {}, 3, "omit", item_kind=kind)
    assert out.upper().startswith("GRAMSCI")


def test_classify_true_magazine_still_magazine() -> None:
    p = Path("Revista Veja - issue 2400 - 2020.pdf")
    local = re.BookMeta(str(p), title="Veja", authors=["Redação"], year="2020")
    meta = re.BookMeta(str(p), title="Veja", authors=["Redação"], year="2020")
    kind, _conf = re.classify_item_kind(p, local, meta)
    assert kind == "magazine"
