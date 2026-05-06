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


class TestParseFilenameAlanBarnardStyle:
    def test_alan_barnard_social_anthropology(self) -> None:
        m = _fb("Alan Barnard - Social Anthropology and Human Origins.pdf")
        assert "barnard" in " ".join(m.authors).lower()
        assert "anthropology" in (m.title or "").lower()


class TestAuthorsSuspiciousHeuristic:
    def test_article_start_fragments_are_suspicious(self) -> None:
        assert re._authors_look_suspicious(["O gesto e a palavra", "Memória e ritmos"])

    def test_single_real_author_not_suspicious(self) -> None:
        assert not re._authors_look_suspicious(["Alan Barnard"])


class TestFilenameUnderscoreAndLongHyphen:
    def test_underscore_replaces_colon_before_subtitle(self) -> None:
        m = _fb("David Harvey – O neoliberalismo_ história e implicações.pdf")
        assert "harvey" in " ".join(m.authors).lower()
        assert "neoliberalismo" in (m.title or "").lower()

    def test_underscore_subtitle_before_author_suffix(self) -> None:
        m = _fb("Art of Game Design_ A Book - Jesse Schell.pdf")
        assert "schell" in " ".join(m.authors).lower()
        assert "game design" in (m.title or "").lower()

    def test_underscore_in_subtitle_clark(self) -> None:
        m = _fb("Andy Clark - Being There_ Putting Brain, Body, and World Together Again.pdf")
        assert "clark" in " ".join(m.authors).lower()
        assert "being there" in (m.title or "").lower()


class TestCatalogAuthorLifeDates:
    def test_paren_bunge_strips_birth_year(self) -> None:
        m = _fb(
            "Scientific research. 2, The search for truth "
            "(Bunge, Mario, 1919-) (z-library.sk, 1lib.sk, z-lib.sk).pdf"
        )
        assert m.authors
        joined = " ".join(m.authors).lower()
        assert "1919" not in joined
        assert "bunge" in joined and "mario" in joined

    def test_format_one_author_strips_trailing_lifespan(self) -> None:
        out = re.format_one_author("Bunge, Mario, 1919-", {})
        assert "1919" not in out
        assert "BUNGE" in out and "Mario" in out


class TestYearTokenAndBadAuthor:
    def test_is_year_token(self) -> None:
        assert re.is_year_token("1867")
        assert not re.is_year_token("Marx")

    def test_year_is_bad_author(self) -> None:
        assert re.author_looks_bad("1867")
        assert re.author_looks_bad("1917")


class TestSplitAuthorsConjunction:
    def test_split_marx_e_engels(self) -> None:
        assert re.split_authors("Marx e Engels") == ["Marx", "Engels"]

    def test_split_karl_marx_friedrich_engels(self) -> None:
        assert re.split_authors("Karl Marx e Friedrich Engels") == ["Karl Marx", "Friedrich Engels"]

    def test_split_marx_and_engels(self) -> None:
        assert re.split_authors("Marx and Engels") == ["Marx", "Engels"]

    def test_do_not_split_title_crime_e_castigo(self) -> None:
        assert re.split_authors("Crime e Castigo") == ["Crime e Castigo"]

    def test_do_not_split_title_guerra_e_paz(self) -> None:
        assert re.split_authors("Guerra e Paz") == ["Guerra e Paz"]

    def test_do_not_split_title_work_and_energy(self) -> None:
        assert re.split_authors("Work and Energy") == ["Work and Energy"]


class TestParentheticalEditorialNotes:
    def test_2nd_edition(self) -> None:
        assert re._parenthetical_is_editorial_note("2nd edition")

    def test_book_club_edition(self) -> None:
        assert re._parenthetical_is_editorial_note("book club edition")

    def test_penguin_classics(self) -> None:
        assert re._parenthetical_is_editorial_note("Penguin Classics")

    def test_oxford_worlds_classics(self) -> None:
        assert re._parenthetical_is_editorial_note("Oxford World's Classics")


class TestResolveTwoSegmentsGuardrails:
    def test_marx_o_capital(self) -> None:
        authors, title = re._resolve_two_segments_to_authors_and_title("Marx", "O Capital")
        assert authors and "marx" in " ".join(authors).lower()
        assert "capital" in title.lower()

    def test_lenin_estado_revolucao(self) -> None:
        authors, title = re._resolve_two_segments_to_authors_and_title("Lenin", "Estado e Revolução")
        assert authors and "lenin" in " ".join(authors).lower()
        assert "estado" in title.lower()

    def test_year_left_never_author(self) -> None:
        authors, title = re._resolve_two_segments_to_authors_and_title("1917", "Estado e Revolução")
        assert not authors
        assert "1917" in title


class TestTrailingParenPublicationYear:
    def test_year_after_title_in_filename(self) -> None:
        m = _fb("Yrjö Engeström - Learning by Expanding (1999).pdf")
        assert m.year == "1999"
        assert m.filename_paren_year is True
        assert "expanding" in (m.title or "").lower()
        assert m.authors
        assert any("engestr" in a.lower() for a in m.authors)


class TestPortalNoiseAndParentheticals:
    def test_strip_zlibrary_and_take_parens_author(self) -> None:
        m = re.parse_filename_fallback(
            Path("As Cruzadas (Amin Maalouf) (z-library.sk, 1lib.sk, z-lib.sk).pdf")
        )
        assert any("maalouf" in a.lower() for a in (m.authors or []))

    def test_cia_registry_tail_removed(self) -> None:
        m = re.parse_filename_fallback(
            Path("CIA Information Report - Stalin leadership - CIA-RDP80-00810A006000360009-0.pdf")
        )
        assert m.title is not None and "CIA-RDP" not in m.title

    def test_traduzido_in_parens_is_not_lone_author(self) -> None:
        m = re.parse_filename_fallback(Path("Godless (Traduzido).pdf"))
        assert not m.authors


class TestFormatRussianStyleInitials:
    def test_batischev_g_s(self) -> None:
        out = re.format_one_author("Batischev G. S.", {})
        assert "BATISCHEV" in out and "G." in out

    def test_sanitize_residual_punctuation(self) -> None:
        out = re.format_one_author("Jameson;, Fredric", {})
        assert ";" not in out
        assert "JAMESON" in out


class TestSplitAuthorsCommaWithE:
    def test_hegel_marx_e_tradicao_single_title(self) -> None:
        parts = re.split_authors("Hegel, Marx e a Tradição Liberal")
        assert len(parts) == 1


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


class TestMergeGuardrails:
    def test_remote_incompatible_author_is_blocked(self) -> None:
        local = re.BookMeta("x.pdf", title="Godless", authors=["Dan Barker"], year="2016", source="filename")
        remote = re.BookMeta("x.pdf", title="Godless", authors=["Gabriel Bresque"], year="2016", source="googlebooks")
        merged = re.merge_metadata(local, remote)
        assert merged.authors == ["Dan Barker"]

    def test_remote_year_outlier_blocked(self) -> None:
        local = re.BookMeta("x.pdf", title="As Cruzadas", authors=["Amin Maalouf"], year="2010", source="filename")
        remote = re.BookMeta("x.pdf", title="As Cruzadas", authors=["Amin Maalouf"], year="1601", source="openlibrary")
        merged = re.merge_metadata(local, remote)
        assert merged.year == "2010"


class TestFallbackNestedEditorialParens:
    def test_author_from_nested_eds_parenthesis(self) -> None:
        m = _fb("Freud Evaluated The Completed Arc (Malcolm Macmillan (Eds.)).pdf")
        assert m.authors
        assert any("macmillan" in a.lower() for a in m.authors)
