"""Testes do parse de nomes de ficheiro (autor/titulo, separadores, triplete)."""
from __future__ import annotations

from pathlib import Path

import renomear_ebooks as re


def _fb(name: str) -> re.BookMeta:
    return re.parse_filename_fallback(Path(name))


class TestParseFilenameKarlMarxStyle:
    def test_mixed_underscore_hyphen_triple(self) -> None:
        m = _fb("Karl_Marx_-_O_Capital.pdf")
        assert m.authors
        assert any("Karl" in a for a in m.authors)
        assert "capital" in (m.title or "").lower()

    def test_spaced_hyphen_with_underscores_in_words(self) -> None:
        m = _fb("Karl_Marx - O_Capital.pdf")
        assert m.authors
        assert any("Karl" in a for a in m.authors)
        assert "capital" in (m.title or "").lower()


class TestParseFilenameSeparators:
    def test_unicode_en_dash_author_title(self) -> None:
        m = _fb("Luc Boltanski \u0026 Eve Chiapello \u2013 O novo espirito.pdf")
        assert len(m.authors) >= 1
        assert "novo" in (m.title or "").lower() or "espirito" in (m.title or "").lower()

    def test_title_then_authors_hyphen(self) -> None:
        m = _fb("O novo espirito do capitalismo - Luc Boltanski & Eve Chiapello.pdf")
        assert len(m.authors) >= 2
        assert "espirito" in (m.title or "").lower() or "capitalismo" in (m.title or "").lower()

    def test_double_underscore_separator(self) -> None:
        m = _fb("O_novo_espirito__Luc_Boltanski.pdf")
        assert m.authors
        assert "boltanski" in " ".join(m.authors).lower()

    def test_tight_hyphen_author_on_right(self) -> None:
        m = _fb("Marxismo-Lucien Seve.pdf")
        assert m.authors
        assert "seve" in " ".join(m.authors).lower()
        assert "marxismo" in (m.title or "").lower()


class TestParseFilenameTriplet:
    def test_author_year_title(self) -> None:
        m = _fb("SOBRENOME, Nome - 2001 - Um Titulo Qualquer.pdf")
        assert m.year == "2001"
        assert m.authors
        assert m.confidence >= 0.35

    def test_triplet_stem_detection(self) -> None:
        assert re.filename_triplet_structured_stem(Path("A - 2000 - B.pdf"))
        assert not re.filename_triplet_structured_stem(Path("Only Title.pdf"))


class TestParseFilenameVolumeSuffix:
    def test_volume_in_parens_not_author(self) -> None:
        m = _fb("Jose Goldemberg - Fisica Geral e Experimental (Vol.1 3a.ed).pdf")
        assert m.filename_extra_suffix
        assert "vol" in m.filename_extra_suffix.lower()
        assert "goldemberg" in " ".join(m.authors).lower() or "goldemberg" in (m.title or "").lower()


class TestInternalIdAndSanitize:
    def test_internal_id_title_detected(self) -> None:
        assert re._looks_like_internal_id_title("4d3d21f432f123a8f1327c80fed7038f")
        assert not re._looks_like_internal_id_title("Marxismo e a Teoria")

    def test_expand_bipartite_sanitizes_mixed(self) -> None:
        out = re._expand_filename_separators_for_bipartite("Karl_Marx_-_O_Capital")
        assert " - " in out
        assert "_" not in out or out.count("_") == 0


class TestFailsafePatchMeta:
    def test_hash_title_replaced_from_filename(self) -> None:
        # Segmento curto a direita ajuda o parse (autor | titulo) a ficar estavel.
        p = Path("Lucien Seve - Marxismo.epub")
        meta = re.BookMeta(
            str(p),
            title="4d3d21f432f123a8f1327c80fed7038f",
            authors=["UNKNOWN"],
            year="2002",
            source="epub",
        )
        patched = re.patch_meta_from_filename_if_merged_suspect(p, meta)
        assert not re._looks_like_internal_id_title(patched.title or "")
        assert patched.authors
        assert "unknown" not in " ".join(patched.authors).lower()
        assert "marxismo" in (patched.title or "").lower()


class TestDefaultFilenameStem:
    def test_extra_suffix_appended(self) -> None:
        m = re.BookMeta(
            str(Path("x.pdf")),
            title="Titulo",
            authors=["Um Autor"],
            year="2000",
            filename_extra_suffix="Vol.1 3ªed",
        )
        stem = re.default_filename_stem(m, {}, 3, "sd", unknown_year_label="s.d.")
        assert "Vol" in stem or "vol" in stem.lower()
        assert "2000" in stem
