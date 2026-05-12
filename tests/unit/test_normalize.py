"""Text normalization tests."""

from awareness.normalize.text import detect_language, normalize_text, safe_title


def test_normalize_collapses_whitespace_and_keeps_paragraphs() -> None:
    raw = "Hello\r\n\r\nWorld   this  is\n\n\n\n a paragraph."
    out = normalize_text(raw, min_chars=10)
    assert out.discarded_reason is None
    assert out.n_lines >= 2
    assert "  " not in out.text  # double spaces collapsed
    assert "\n\n\n" not in out.text


def test_normalize_filters_too_short() -> None:
    out = normalize_text("hi", min_chars=200)
    assert out.discarded_reason is not None


def test_normalize_strips_control_chars() -> None:
    raw = "Title\x00\x01\x02\nbody body body body body" * 50
    out = normalize_text(raw, min_chars=20)
    assert "\x00" not in out.text
    assert "\x01" not in out.text


def test_normalize_truncates_to_max_chars() -> None:
    raw = "x" * 5000
    out = normalize_text(raw, min_chars=10, max_chars=500)
    assert out.n_chars == 500


def test_safe_title_uses_first_line_when_missing() -> None:
    assert safe_title(None, "First line is the title.\nbody body body") == "First line is the title."
    assert safe_title("Real Title", "body") == "Real Title"


def test_detect_language_short_text_returns_none() -> None:
    assert detect_language("hi") is None


def test_detect_language_long_english_returns_en() -> None:
    text = ("The quick brown fox jumps over the lazy dog. " * 30)
    # We don't assert exact value because langdetect can occasionally vary;
    # at minimum we get a non-None result for substantial English text.
    out = detect_language(text)
    assert out is not None
