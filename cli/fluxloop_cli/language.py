"""Shared language utilities for FluxLoop CLI."""

DEFAULT_LANGUAGE = "en"


def normalize_language_token(value: str | None) -> str | None:
    """Normalize language code to lowercase 2-letter token.

    Examples: "EN-US" -> "en", "ko_KR" -> "ko", "ja" -> "ja"
    """
    if not value:
        return None
    raw = value.strip().lower()
    if not raw:
        return None
    token = raw.split("-", 1)[0].split("_", 1)[0].strip()
    return token or None
