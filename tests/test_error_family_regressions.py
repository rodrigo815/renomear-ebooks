from __future__ import annotations

from pathlib import Path

import pytest

import renomear_ebooks as re


@pytest.mark.parametrize(
    "name,expected_author",
    [
        ("Peter Atkins - The Laws of Thermodynamics.pdf", "Atkins"),
        ("R. Medeiros - Nietzsche e o Socialismo_ uma coletânea.pdf", "Medeiros"),
    ],
)
def test_family_false_author(name: str, expected_author: str) -> None:
    m = re.parse_filename_fallback(Path(name))
    joined = " ".join(m.authors or [])
    assert expected_author.lower() in joined.lower()


@pytest.mark.parametrize(
    "local_year,remote_year,expected",
    [
        ("2010", "1601", "2010"),
        ("1999", "1999", "1999"),
    ],
)
def test_family_false_year_guardrail(local_year: str, remote_year: str, expected: str) -> None:
    local = re.BookMeta("x.pdf", title="As Cruzadas", authors=["Amin Maalouf"], year=local_year)
    remote = re.BookMeta("x.pdf", title="As Cruzadas", authors=["Amin Maalouf"], year=remote_year)
    merged = re.merge_metadata(local, remote)
    assert merged.year == expected


def test_family_editorial_noise_not_author() -> None:
    m = re.parse_filename_fallback(Path("Godless (Traduzido).pdf"))
    assert not m.authors


def test_family_multi_author_split() -> None:
    parts = re.split_authors("James F. Kasting, Robert G. Crane & Lee R. Kump")
    assert len(parts) >= 3


def test_family_volume_edition_normalization() -> None:
    m = re.BookMeta("x.pdf", title="Cadernos do Cárcere", authors=["Antonio Gramsci"], year="2012")
    m.filename_extra_suffix = "Vol.1 3a.ed"
    nm = re.make_new_filename(m, ".pdf", {}, 3, "sd")
    assert "Vol" in nm or "vol" in nm.lower()


def test_family_transliteration_alias_safe() -> None:
    meta = re.BookMeta("x.pdf", title="Historia", authors=["Georg Lukacs"], year="2021", confidence=0.5)
    local = re.BookMeta("x.pdf", title="Historia", authors=[], year="2021", confidence=0.2)
    ali = {"georg lukacs": "György Lukács"}
    out = re._apply_author_aliases(meta, local, ali)
    assert out.authors and "György" in out.authors[0]
