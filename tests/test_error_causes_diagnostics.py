from __future__ import annotations

from pathlib import Path

import pytest

import renomear_ebooks as re


def _fb(name: str) -> re.BookMeta:
    return re.parse_filename_fallback(Path(name))


def test_c2_parenthetical_language_is_not_author() -> None:
    m = _fb("Michael Tomasello - Por qué cooperamos (Espanhol).pdf")
    joined = " ".join(m.authors or []).lower()
    assert "espanhol" not in joined


def test_c2_parenthetical_traduzido_is_not_author() -> None:
    m = _fb("Dan Barker - Godless (Traduzido).pdf")
    joined = " ".join(m.authors or []).lower()
    assert "traduzido" not in joined


def test_c2_parenthetical_political_group_not_author() -> None:
    m = _fb("O marxismo ... (nazbols e afins).pdf")
    joined = " ".join(m.authors or []).lower()
    assert "nazbol" not in joined
    assert "afins" not in joined


def test_c3_prefer_left_author_in_author_dash_title_pattern() -> None:
    m = _fb("Paul Burkett - Marx and Nature.pdf")
    joined = " ".join(m.authors or []).lower()
    assert "burkett" in joined


def test_c3_english_title_not_promoted_to_author() -> None:
    m = _fb("Lewis Mumford - Technics And Civilization.pdf")
    joined = " ".join(m.authors or []).lower()
    assert "civilization" not in joined


def test_c4_etc_token_is_bad_author_word() -> None:
    assert re.author_looks_bad("etc")
    assert re.author_looks_bad("etc.")


def test_c7_volume_suffix_is_detected_as_suffix_not_author() -> None:
    m = _fb("José Goldemberg - Física Geral e Experimental (Vol.1 3a.ed).pdf")
    assert m.filename_extra_suffix
    assert "vol" in m.filename_extra_suffix.lower()


def test_c1_regression_outlier_year_should_be_blocked_for_cruzadas() -> None:
    local = re.BookMeta("x.pdf", title="As Cruzadas vistas pelos árabes", authors=["Amin Maalouf"], year="1988")
    remote = re.BookMeta("x.pdf", title="As Cruzadas vistas pelos árabes", authors=["Amin Maalouf"], year="1601")
    merged = re.merge_metadata(local, remote)
    assert merged.year == "1988"


def test_c4_regression_duplicate_author_variants_should_collapse() -> None:
    merged = re.merge_metadata(
        re.BookMeta("x.pdf", title="The Laws of Thermodynamics", authors=["Peter Atkins"], year=""),
        re.BookMeta("x.pdf", title="The Laws of Thermodynamics", authors=["P. W. Atkins"], year="2011"),
    )
    names = " ; ".join(merged.authors or [])
    assert names.count("Atkins") <= 1


def test_c6_regression_periodical_should_not_force_person_author() -> None:
    m = _fb("Soviet Cybernetics Review - Vol. 4 no1.pdf")
    assert not m.authors
    assert "review" in (m.title or "").lower()


@pytest.mark.xfail(reason="C5 pendente: fallback sem autor ainda depende de remoto fraco", strict=False)
def test_c5_regression_where_human_rights_should_recover_author() -> None:
    m = _fb("Where Human Rights Are Real.pdf")
    assert m.authors


@pytest.mark.xfail(reason="C3/C4 pendente: autor composto de particulas ainda inconsistente", strict=False)
def test_c3_c4_regression_compound_surname_engel_di_mauro() -> None:
    out = re.format_one_author("Salvatore Engel-Di Mauro", {})
    assert out.startswith("ENGEL-DI MAURO")
