from __future__ import annotations

import argparse
from pathlib import Path

import renomear_ebooks as re


def _build_args(**overrides) -> argparse.Namespace:
    base = {
        "omit_console": False,
        "review": False,
        "sources": "",
        "source": "all",
        "search_speed": None,
        "remote_metadata": "",
        "keep_local_metadata": "",
        "unknown_year": "sd",
        "unknown_year_text": "s.d.",
        "omit_date_if_missing": False,
        "fast": False,
        "thorough": False,
        "max_pdf_pages": 3,
        "sleep": 0.25,
        "force_remote": False,
        "exts": "",
        "find_duplicates": False,
        "dedup": False,
        "generate_catalog": False,
        "apply": False,
        "move_duplicates": False,
        "prefer_larger": False,
        "prefer_smaller": False,
        "delete_dups": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_structural_parse_route_author_year_title() -> None:
    p = Path("Autor X - 2001 - Livro Y.pdf")
    m = re.parse_filename_fallback(p)
    routed = re._parse_filename_triplet_author_year_title(p, re._normalize_filename_hyphens(p.stem))
    assert routed is not None
    assert m.title == routed.title
    assert m.authors == routed.authors
    assert m.year == routed.year


def test_structural_parse_route_year_author_title() -> None:
    p = Path("2002 - Autor Y - Livro Z.pdf")
    m = re.parse_filename_fallback(p)
    routed = re._parse_filename_triplet_year_author_title(p, re._normalize_filename_hyphens(p.stem))
    assert routed is not None
    assert m.title == routed.title
    assert m.authors == routed.authors
    assert m.year == routed.year


def test_structural_parse_route_nested_editor_parenthetical() -> None:
    p = Path("Freud Evaluated The Completed Arc (Malcolm Macmillan (Eds.)).pdf")
    stem = re._normalize_filename_hyphens(re.compact_spaces(p.stem))
    m = re.parse_filename_fallback(p)
    routed = re._parse_filename_nested_editor_parenthetical(p, stem, m.year, m.filename_paren_year)
    assert routed is not None
    assert m.title == routed.title
    assert m.authors == routed.authors


def test_structural_parse_route_simple_parenthetical_editorial() -> None:
    p = Path("Godless (Traduzido).pdf")
    stem_raw = re.compact_spaces(p.stem)
    stem_hyp = re._normalize_filename_hyphens(stem_raw)
    stem, _ = re.strip_trailing_volume_edition_parenthetical(stem_hyp)
    m = re.parse_filename_fallback(p)
    routed = re._parse_filename_simple_parenthetical(p, stem, m.year, m.filename_paren_year)
    assert routed is not None
    assert m.title == routed.title
    assert m.authors == routed.authors


def test_structural_parse_route_bipartite_fallback() -> None:
    p = Path("Marx - O Capital.pdf")
    stem_raw = re.compact_spaces(p.stem)
    stem_raw = re._sanitize_filename_stem_noise(stem_raw)
    stem_hyp = re._normalize_filename_hyphens(stem_raw)
    stem, _ = re.strip_trailing_volume_edition_parenthetical(stem_hyp)
    stem, paren_yr = re._strip_trailing_paren_publication_year(stem)
    filename_paren_year = bool(paren_yr)
    year = paren_yr or re.year_from_string(stem) or re.year_from_string(stem_hyp) or re.year_from_string(stem_raw)

    m = re.parse_filename_fallback(p)
    routed = re._parse_filename_bipartite_fallback(p, stem, year, filename_paren_year)
    assert m.title == routed.title
    assert m.authors == routed.authors
    assert m.year == routed.year


def test_structural_configure_runtime_args_defaults() -> None:
    args = _build_args()
    rc = re._configure_runtime_args(args)
    assert rc is None
    assert args.enabled_remote_sources == re.ALL_REMOTE_SOURCES
    assert args.effective_sleep == 0.25
    assert args.effective_max_pdf_pages == 3
    assert args.ext_filter is None


def test_structural_configure_runtime_args_fast_profile() -> None:
    args = _build_args(fast=True, max_pdf_pages=9, sleep=1.0)
    rc = re._configure_runtime_args(args)
    assert rc is None
    assert args.effective_sleep == 0.0
    assert args.effective_max_pdf_pages == 1
    assert args.skip_author_enrich is True
    assert args.enabled_remote_sources == re.SEARCH_SPEED_TO_SOURCES[5]


def test_structural_validate_main_modes_conflict() -> None:
    args = _build_args(apply=True, review=True)
    rc = re._validate_main_modes(args, [Path(".").resolve()])
    assert rc == 2
