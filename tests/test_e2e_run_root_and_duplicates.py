from __future__ import annotations

import argparse
import csv
import json
import re as regex
import subprocess
import sys
from pathlib import Path

import renomear_ebooks as re

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _args_for_run_on_root(**overrides) -> argparse.Namespace:
    base = {
        "apply": False,
        "review": False,
        "recursive": False,
        "ext_filter": None,
        "limit": 0,
        "jobs": 1,
        "effective_max_pdf_pages": 1,
        "year_strategy": "original",
        "source": "all",
        "enabled_remote_sources": frozenset({"openlibrary"}),
        "effective_force_remote": False,
        "effective_sleep": 0.0,
        "prefer_remote_title": False,
        "skip_author_enrich": True,
        "remote_merge_fields": re.MERGE_METADATA_FIELDS,
        "keep_local_metadata_fields": frozenset(),
        "max_authors": 3,
        "unknown_year": "sd",
        "unknown_year_text": "s.d.",
        "filename_pattern": "",
        "quiet": True,
        "overrides": "author_overrides.json",
        "supplementary_data": "",
        "supplementary_mode": "merge",
        "missing_year_log": "",
        "generate_catalog": False,
        "catalog_format": "json",
        "review_author_lock": {},
        "only_missing_year": False,
        "only_review_needed": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_run_on_root_marks_revisao_necessaria_on_failure_and_low_score(
    tmp_path: Path, monkeypatch
) -> None:
    src = tmp_path / "Arquivo Original.pdf"
    src.write_bytes(b"x")

    local = re.BookMeta(str(src), title="Titulo Local", authors=["Autor Local"], year="", source="filename")

    def _fake_build_local_metadata(*args, **kwargs):  # noqa: ANN002, ANN003
        return [(src, local)]

    def _fake_lookup_metadata(*args, **kwargs):  # noqa: ANN002, ANN003
        return re.BookMeta(
            str(src),
            title="Outro Titulo",
            authors=["Outro Autor"],
            year="2001",
            source="openlibrary",
            confidence=0.55,
            source_failures=[{"source": "openlibrary", "reason": "timeout", "action": "ignored_source_and_continued"}],
        )

    monkeypatch.setattr(re, "build_local_metadata", _fake_build_local_metadata)
    monkeypatch.setattr(re, "lookup_metadata", _fake_lookup_metadata)

    args = _args_for_run_on_root()
    n_rows, _, plan_path, _, _, _ = re.run_on_root(tmp_path, args)

    assert n_rows == 1
    assert plan_path.exists()

    with plan_path.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 1
    assert rows[0]["status"] == "revisao_necessaria"
    failures = json.loads(rows[0]["source_failures"])
    assert failures and failures[0]["source"] == "openlibrary"


def _install_two_file_mocks_for_console_filter(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    bad = tmp_path / "bad.pdf"
    good = tmp_path / "Autor X - 2001 - Livro Y.pdf"
    bad.write_bytes(b"b")
    good.write_bytes(b"g")

    def _fake_build_local_metadata(files, **kwargs):  # noqa: ANN002, ANN003
        out = []
        for p in sorted(files, key=lambda x: x.name):
            if p.name == "bad.pdf":
                out.append(
                    (
                        p,
                        re.BookMeta(
                            str(p),
                            title="Titulo Local",
                            authors=["Autor Local"],
                            year="",
                            source="filename",
                        ),
                    )
                )
            else:
                out.append(
                    (
                        p,
                        re.BookMeta(
                            str(p),
                            title="Livro Y",
                            authors=["Autor X"],
                            year="2001",
                            source="filename",
                        ),
                    )
                )
        return out

    def _fake_lookup_metadata(meta, *args, **kwargs):  # noqa: ANN002, ANN003
        if Path(meta.path).name == "bad.pdf":
            return re.BookMeta(
                str(meta.path),
                title="Outro Titulo",
                authors=["Outro Autor"],
                year="2001",
                source="openlibrary",
                confidence=0.55,
                source_failures=[
                    {"source": "openlibrary", "reason": "timeout", "action": "ignored_source_and_continued"}
                ],
            )
        return meta

    monkeypatch.setattr(re, "build_local_metadata", _fake_build_local_metadata)
    monkeypatch.setattr(re, "lookup_metadata", _fake_lookup_metadata)


def test_only_review_needed_filters_per_file_console_lines(tmp_path: Path, monkeypatch) -> None:
    _install_two_file_mocks_for_console_filter(tmp_path, monkeypatch)
    logged: list[str] = []
    monkeypatch.setattr(re, "log_info", lambda m: logged.append(m))
    args = _args_for_run_on_root(quiet=False, only_review_needed=True)
    re.run_on_root(tmp_path, args)
    per_file = [m for m in logged if " -> " in m]
    assert len(per_file) == 1
    assert "revisao_necessaria" in per_file[0]
    assert "bad.pdf" in per_file[0]


def test_without_only_review_needed_logs_all_per_file_lines(tmp_path: Path, monkeypatch) -> None:
    _install_two_file_mocks_for_console_filter(tmp_path, monkeypatch)
    logged: list[str] = []
    monkeypatch.setattr(re, "log_info", lambda m: logged.append(m))
    args = _args_for_run_on_root(quiet=False, only_review_needed=False)
    re.run_on_root(tmp_path, args)
    per_file = [m for m in logged if " -> " in m]
    assert len(per_file) == 2


def test_help_lists_only_review_needed_flag() -> None:
    proc = subprocess.run(
        [sys.executable, str(_REPO_ROOT / "renomear_ebooks.py"), "--help"],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(_REPO_ROOT),
    )
    assert proc.returncode == 0
    assert "--only-review-needed" in proc.stdout


def test_help_lists_all_long_flags_from_parser_source() -> None:
    source = (_REPO_ROOT / "renomear_ebooks.py").read_text(encoding="utf-8")
    expected_flags = sorted(set(regex.findall(r"\"(--[a-z0-9][a-z0-9\\-]*)\"", source)))
    proc = subprocess.run(
        [sys.executable, str(_REPO_ROOT / "renomear_ebooks.py"), "--help"],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(_REPO_ROOT),
    )
    assert proc.returncode == 0
    # Cada flag de linha de comando declarada no parser precisa aparecer no help.
    missing = [flag for flag in expected_flags if flag not in proc.stdout]
    assert not missing, f"Flags ausentes no --help: {missing}"


def test_unique_target_handles_multiple_collisions(tmp_path: Path) -> None:
    target_dir = tmp_path / "renamed"
    target_dir.mkdir()
    (target_dir / "Livro.pdf").write_text("a", encoding="utf-8")
    (target_dir / "Livro (2).pdf").write_text("b", encoding="utf-8")

    src1 = tmp_path / "s1.pdf"
    src2 = tmp_path / "s2.pdf"
    src3 = tmp_path / "s3.pdf"
    src1.write_text("1", encoding="utf-8")
    src2.write_text("2", encoding="utf-8")
    src3.write_text("3", encoding="utf-8")

    reserved: set[Path] = set()
    t1 = re.unique_target(src1, "Livro.pdf", target_dir, reserved)
    t2 = re.unique_target(src2, "Livro.pdf", target_dir, reserved)
    t3 = re.unique_target(src3, "Livro.pdf", target_dir, reserved)

    assert t1.name == "Livro (3).pdf"
    assert t2.name == "Livro (4).pdf"
    assert t3.name == "Livro (5).pdf"


def test_filename_pattern_handles_empty_placeholders_without_dangling_dash() -> None:
    m1 = re.BookMeta("x.pdf", title="Only Title", authors=[], year="")
    out1 = re.make_new_filename(
        m1,
        ".pdf",
        overrides={},
        max_authors=3,
        unknown_year="sd",
        filename_pattern="%AUTHOR% - %TITLE%%FORMAT%",
        unknown_year_label="s.d.",
    )
    assert out1 == "Only Title.pdf"

    m2 = re.BookMeta("x.pdf", title="Godless", authors=["Dan Barker"], year="")
    out2 = re.make_new_filename(
        m2,
        ".pdf",
        overrides={},
        max_authors=3,
        unknown_year="omit",
        filename_pattern="%AUTHOR% - %DATE% - %TITLE%%FORMAT%",
        unknown_year_label="s.d.",
    )
    assert out2 == "BARKER, Dan - Godless.pdf"
    assert " -  - " not in out2


def _args_for_duplicates(**overrides) -> argparse.Namespace:
    base = {
        "recursive": False,
        "ext_filter": frozenset({".pdf"}),
        "limit": 0,
        "effective_max_pdf_pages": 1,
        "year_strategy": "original",
        "jobs": 1,
        "prefer_format": "pdf,epub",
        "prefer_larger": True,
        "prefer_smaller": False,
        "duplicates_report": "",
        "move_duplicates": False,
        "dedup_algorithm": "sha1",
        "delete_dups": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_find_duplicates_generates_report_for_isbn_cluster(tmp_path: Path, monkeypatch) -> None:
    f1 = tmp_path / "A.pdf"
    f2 = tmp_path / "B.pdf"
    f1.write_bytes(b"a")
    f2.write_bytes(b"b")

    def _fake_build_local_metadata(files, **kwargs):  # noqa: ANN001
        out = []
        for p in files:
            out.append(
                (
                    p,
                    re.BookMeta(
                        str(p),
                        title="Mesmo Livro",
                        authors=["Autor"],
                        year="2000",
                        isbn="9780306406157",
                    ),
                )
            )
        return out

    monkeypatch.setattr(re, "build_local_metadata", _fake_build_local_metadata)

    report = re.run_find_duplicates(tmp_path, _args_for_duplicates())
    assert report is not None
    assert report.exists()

    with report.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    assert len(rows) == 1
    assert rows[0]["acao"] == "apenas_relatorio"
    assert rows[0]["manter"].endswith(".pdf")
    assert rows[0]["duplicado"].endswith(".pdf")


def test_dedup_hash_mode_reports_keeper_and_duplicate(tmp_path: Path) -> None:
    same = b"same-content-for-hash"
    (tmp_path / "dup1.pdf").write_bytes(same)
    (tmp_path / "dup2.pdf").write_bytes(same)
    (tmp_path / "other.pdf").write_bytes(b"other-content")

    report = re.run_dedup_hashes(tmp_path, _args_for_duplicates())
    assert report.exists()

    with report.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    roles = {r["funcao"] for r in rows}
    assert "manter" in roles
    assert "duplicado" in roles
    assert any(r["algoritmo"] == "sha1" for r in rows)
