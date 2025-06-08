import re
import pycountry

def strip_html_tags(text: str) -> str:
    return " ".join(re.sub(r"<[^>]+>", "", text).split())


def non_empty_str(v: str, field_name: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError(f"{field_name!r} must be non-empty")
    return s


def map_country_code(name: str) -> str:
    overrides = {"Turkey": "TR", "Türkiye": "TR", "European Union": "EU"}
    try:
        return pycountry.countries.search_fuzzy(name)[0].alpha_2
    except Exception:
        pass
    code = overrides.get(name.strip())
    if code:
        return code
    country = pycountry.countries.get(name=name.strip())
    if country:
        return country.alpha_2
    logger.error("Invalid country name: %s", name)
    raise ValueError(f"Invalid country name: {name}")