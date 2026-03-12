"""
schools.py
══════════════════════════════════════════════════════════════════════════════
Shared school registry for the entire Nutmeg Sports project.

This module is the single source of truth for:
  - Canonical school names  (what appears in master_games.csv / Google Sheets)
  - Short display names     (website game cards, Instagram captions)
  - Abbreviations           (3-letter codes for tight layouts)
  - CIAC school IDs         (used by scrapeTEAMS / updateRecords to hit the
                             CIAC schedule page for each school)
  - Sport classifications   (boys CIAC class, girls division)
  - Conference
  - Mascot / town

All other scripts should import from here instead of maintaining their own
SCHOOLS lists or normalize() helpers.

Usage
─────
    from schools import SCHOOLS, short_name, abbrev_name, normalize, find_school

    # SCHOOLS is the drop-in replacement for the (name, school_id) list in
    # scrapeTEAMS.py — same format:
    for name, school_id in SCHOOLS:
        ...

    # Short name for display (website, Instagram captions):
    short_name("Notre Dame-West Haven")   # → "ND West Haven"
    short_name("Norwich Free Academy")    # → "Norwich FA"

    # 3-letter abbreviation:
    abbrev_name("Northwest Catholic")     # → "NWC"

    # Normalize for fuzzy matching (used internally, also exported):
    normalize("Notre Dame-West Haven")    # → "notredamewesthaven"

    # Full registry row as a dict:
    find_school("NW Catholic")            # → { canonical, short, abbrev, ... }
"""

import re
from typing import Optional

# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY
# Each entry: (canonical_name, ciac_school_id, short_name, abbrev,
#              mascot, town, ciac_class_boys, ciac_div_girls, conference)
# ══════════════════════════════════════════════════════════════════════════════

# Format: (Official Name, ID, Shortname, Abbreviation)
_REGISTRY = [
    ("Prince Tech", 6, "Prince Tech", "PRNC"),
    ("Comp Sci", 127, "Comp Sci", "ACSE"),
    ("Lauralton Hall", 3, "Lauralton Hall", "LAUR"),
    ("Achievement First", 5, "Achievement First", "AFH"),
    ("Amistad", 7, "Amistad", "AMST"),
    ("Amity", 8, "Amity", "AMIT"),
    ("Ansonia", 9, "Ansonia", "ANS"),
    ("Avon", 10, "Avon", "AVON"),
    ("Bacon Academy", 11, "Bacon Academy", "BACN"),
    ("Bassick", 12, "Bassick", "BSSK"),
    ("Berlin", 13, "Berlin", "BERL"),
    ("Bethel", 14, "Bethel", "BTHL"),
    ("Bloomfield", 15, "Bloomfield", "BLMF"),
    ("Bolton", 16, "Bolton", "BOLT"),
    ("Branford", 17, "Branford", "BRNF"),
    ("Brien McMahon", 19, "McMahon", "BRMC"),
    ("Bristol Central", 20, "Bristol Central", "BCEN"),
    ("Bristol Eastern", 21, "Bristol Eastern", "BEAS"),
    ("Brookfield", 22, "Brookfield", "BKFD"),
    ("Bulkeley", 23, "Bulkeley", "BULK"),
    ("Bullard Havens Tech", 24, "Bullard-Havens", "BULL"),
    ("Bunnell", 25, "Bunnell", "BUNN"),
    ("Canton", 26, "Canton", "CANT"),
    ("Capital Prep", 27, "Capital Prep", "CPRP"),
    ("Bridgeport Central", 18, "Bdpt. Central", "BCEN"),
    ("Cheshire", 29, "Cheshire", "CHSH"),
    ("Classical Magnet", 61, "Classical", "CLAS"),
    ("Coginchaug", 30, "Coginchaug", "COGI"),
    ("Conard", 31, "Conard", "CONR"),
    ("Coventry", 32, "Coventry", "COV"),
    ("Cromwell", 33, "Cromwell", "CROM"),
    ("Crosby", 34, "Crosby", "CRSB"),
    ("Danbury", 35, "Danbury", "DANB"),
    ("Daniel Hand", 36, "Daniel Hand", "HAND"),
    ("Darien", 37, "Darien", "DARN"),
    ("Derby", 38, "Derby", "DRBY"),
    ("Goodwin Tech", 46, "Goodwin Tech", "GOOD"),
    ("E.O. Smith", 51, "E.O. Smith", "EOS"),
    ("East Catholic", 39, "E Catholic", "ECAT"),
    ("East Granby", 40, "E Granby", "EGRB"),
    ("East Hampton", 41, "E Hampton", "EHMP"),
    ("East Hartford", 42, "E Hartford", "EHRT"),
    ("East Haven", 43, "E Haven", "EHVN"),
    ("East Lyme", 44, "E Lyme", "ELYM"),
    ("East Windsor", 45, "E Windsor", "EWND"),
    ("Whitney Tech", 47, "Whitney Tech", "WHIT"),
    ("Grasso Tech", 60, "Grasso Tech", "GRAS"),
    ("Ellington", 48, "Ellington", "ELLN"),
    ("O'Brien Tech", 49, "O'Brien Tech", "OBRN"),
    ("Enfield", 50, "Enfield", "ENFD"),
    ("Fairfield Prep", 52, "Fairfield Prep", "FPREP"),
    ("Fairfield Ludlowe", 53, "Ludlowe", "FLUD"),
    ("Fairfield Warde", 54, "Warde", "FWRD"),
    ("Farmington", 55, "Farmington", "FARM"),
    ("Maloney", 89, "Maloney", "MAL"),
    ("Glastonbury", 58, "Glastonbury", "GLAS"),
    ("Granby", 59, "Granby Memorial", "GRBY"),
    ("Greenwich", 62, "Greenwich", "GRNW"),
    ("Griswold", 63, "Griswold", "GRIS"),
    ("Guilford", 64, "Guilford", "GUIL"),
    ("Wilcox Tech", 69, "Wilcox Tech", "WLCX"),
    ("Haddam-Killingworth", 65, "H-K", "HK"),
    ("Hamden", 66, "Hamden", "HAMD"),
    ("Hartford Public", 67, "Hartford Public", "HPHS"),
    ("Ellis Tech", 68, "Ellis Tech", "ELLS"),
    ("Abbott Tech", 70, "Abbott Tech", "ABBT"),
    ("Career Magnet", 28, "Career", "CARE"),
    ("Holy Cross", 71, "Holy Cross", "HC"),
    ("Housatonic Regional", 72, "Housatonic", "HOUS"),
    ("Cheney Tech", 73, "Cheney Tech", "CHNY"),
    ("Immaculate", 74, "Immaculate", "IMMC"),
    ("Wright Tech", 77, "Wright Tech", "WRGT"),
    ("Hillhouse", 75, "Hillhouse", "HILL"),
    ("Joel Barlow", 78, "Joel Barlow", "BRLW"),
    ("Kennedy", 76, "Kennedy", "KENN"),
    ("Jonathan Law", 79, "Jonathan Law", "LAW"),
    ("Foran", 80, "Foran", "FOR"),
    ("Killingly", 81, "Killingly", "KILL"),
    ("Kolbe Cathedral", 82, "Kolbe", "KLBE"),
    ("Ledyard", 83, "Ledyard", "LEDY"),
    ("Lewis Mills", 84, "Lewis Mills", "LMIL"),
    ("Lyman Hall", 86, "Lyman Hall", "LYMH"),
    ("Lyman Memorial", 87, "Lyman Memorial", "LYMM"),
    ("Lyme-Old Lyme", 88, "Old Lyme", "LOL"),
    ("Manchester", 90, "Manchester", "MANC"),
    ("Masuk", 91, "Masuk", "MSUK"),
    ("Mercy", 93, "Mercy", "MRCY"),
    ("Middletown", 94, "Middletown", "MIDD"),
    ("Montville", 96, "Montville", "MONT"),
    ("Hale Ray", 98, "Hale Ray", "HRAY"),
    ("Naugatuck", 99, "Naugatuck", "NAUG"),
    ("New Britain", 100, "New Britain", "NBRI"),
    ("New Canaan", 101, "New Canaan", "NCAN"),
    ("New Fairfield", 102, "New Fairfield", "NFAI"),
    ("New London", 104, "New London", "NLON"),
    ("New Milford", 105, "New Milford", "NMIL"),
    ("Newington", 103, "Newington", "NWGT"),
    ("Newtown", 106, "Newtown", "NEWT"),
    ("Nonnewaug", 107, "Nonnewaug", "NONN"),
    ("North Branford", 108, "N Branford", "NBRN"),
    ("North Haven", 109, "N Haven", "NHVN"),
    ("Northwest Catholic", 110, "NW Catholic", "NWC"),
    ("Northwestern", 111, "Northwestern", "NWST"),
    ("Norwalk", 112, "Norwalk", "NORW"),
    ("NFA", 113, "NFA", "NFA"),
    ("Norwich Tech", 114, "Norwich Tech", "NORR"),
    ("Notre Dame-West Haven", 115, "ND West Haven", "NDWH"),
    ("Notre Dame Prep", 116, "ND Fairfield", "NDF"),
    ("Platt", 117, "Platt", "PLAT"),
    ("Old Saybrook", 118, "Old Saybrook", "OSAY"),
    ("Wolcott Tech", 119, "Wolcott Tech", "WOLC"),
    ("Oxford", 120, "Oxford", "OXFD"),
    ("Plainfield", 122, "Plainfield", "PLFD"),
    ("Plainville", 123, "Plainville", "PLNV"),
    ("Platt Tech", 124, "Platt Tech", "PLTT"),
    ("Pomperaug", 125, "Pomperaug", "POMP"),
    ("Portland", 126, "Portland", "PORT"),
    ("Putnam", 128, "Putnam", "PUTN"),
    ("RHAM", 129, "RHAM", "RHAM"),
    ("Ridgefield", 130, "Ridgefield", "RIDG"),
    ("Fitch", 56, "Fitch", "FICH"),
    ("Rockville", 131, "Rockville", "ROCK"),
    ("Rocky Hill", 132, "Rocky Hill", "RHIL"),
    ("Sacred Heart Academy", 133, "Sacred Heart", "SHA"),
    ("Seymour", 134, "Seymour", "SEYM"),
    ("Sheehan", 135, "Sheehan", "SHEE"),
    ("Shelton", 136, "Shelton", "SHEL"),
    ("Shepaug Valley", 137, "Shepaug Valley", "SHEP"),
    ("Simsbury", 138, "Simsbury", "SIMS"),
    ("Somers", 139, "Somers", "SOM"),
    ("South Windsor", 141, "S Windsor", "SWND"),
    ("Southington", 140, "Southington", "STHN"),
    ("SMSA", 142, "SMSA", "SMSA"),
    ("St. Joseph", 147, "St Joseph", "STJO"),
    ("St. Paul", 149, "St Paul", "STP"),
    ("Stafford", 143, "Stafford", "STAF"),
    ("Stamford", 144, "Stamford", "STMF"),
    ("Staples", 145, "Staples", "STAP"),
    ("Stonington", 148, "Stonington", "STON"),
    ("Stratford", 150, "Stratford", "STRT"),
    ("Suffield", 151, "Suffield", "SUFF"),
    ("Terryville", 152, "Terryville", "TVIL"),
    ("Gilbert", 57, "Gilbert", "GILB"),
    ("Morgan", 97, "Morgan", "MORG"),
    ("Woodstock Academy", 186, "Woodstock Academy", "WOOD"),
    ("Tolland", 155, "Tolland", "TOLL"),
    ("Torrington", 156, "Torrington", "TORR"),
    ("Tourtellotte", 157, "Tourtellotte", "TOUR"),
    ("Trumbull", 159, "Trumbull", "TRUM"),
    ("University", 160, "University", "UHS"),
    ("Valley Regional", 161, "Valley Regional", "VALR"),
    ("Vinal Tech", 162, "Vinal Tech", "VINL"),
    ("Kaynor Tech", 174, "Kaynor Tech", "KAYN"),
    ("Harding", 164, "Harding", "HRDG"),
    ("WCA", 165, "WCA", "WCA"),
    ("Waterford", 166, "Waterford", "WTFD"),
    ("Watertown", 167, "Watertown", "WTRT"),
    ("Weaver", 168, "Weaver", "WVR"),
    ("West Haven", 170, "WHaven", "WHVN"),
    ("Westbrook", 169, "Westbrook", "WSTB"),
    ("Westhill", 171, "Westhill", "WSTL"),
    ("Weston", 172, "Weston", "WSTN"),
    ("Wethersfield", 173, "Wethersfield", "WETH"),
    ("Wilbur Cross", 176, "Wilbur Cross", "WCRO"),
    ("Wilby", 177, "Wilby", "WLBY"),
    ("Hall", 183, "Hall", "HALL"),
    ("Wilton", 178, "Wilton", "WILT"),
    ("Windham", 179, "Windham", "WNDH"),
    ("Windham Tech", 180, "Windham Tech", "WTEC"),
    ("Windsor", 181, "Windsor", "WNDS"),
    ("Windsor Locks", 182, "Windsor Locks", "WLCK"),
    ("Wolcott", 184, "Wolcott", "WOLC"),
    ("Woodland", 185, "Woodland", "WDLD"),
    ("Xavier", 187, "Xavier", "XVRE")
]

# ── Field names for structured access ────────────────────────────────────────
_FIELDS = ("canonical", "ciac_id", "short_name", "abbrev")

# ── Build lookup indexes ──────────────────────────────────────────────────────
_by_norm:   dict[str, dict] = {}
_all_rows:  list[dict]      = []

for _row in _REGISTRY:
    _d = dict(zip(_FIELDS, _row))
    _all_rows.append(_d)
    _key = re.sub(r"[^a-z0-9]", "", _d["canonical"].lower())
    _by_norm[_key] = _d
    # Also index the short_name so lookups work from either name
    _skey = re.sub(r"[^a-z0-9]", "", _d["short_name"].lower())
    if _skey not in _by_norm:
        _by_norm[_skey] = _d


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def normalize(name: str) -> str:
    """Lowercase, strip non-alphanumeric. Used for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def find_school(name: str) -> Optional[dict]:
    """
    Return the registry dict for a school name, or None if not found.
    Tries exact normalized match first, then partial containment.
    """
    if not name:
        return None
    norm = normalize(name)
    if norm in _by_norm:
        return _by_norm[norm]
    # Partial match fallback
    for key, row in _by_norm.items():
        if norm in key or key in norm:
            return row
    return None


def short_name(name: str) -> str:
    """
    Return the short display name for a school.
    Falls back to the original name if not found.

    Examples:
        short_name("Notre Dame-West Haven") → "ND West Haven"
        short_name("Norwich Free Academy")  → "Norwich FA"  (via NFA entry)
        short_name("NFA")                   → "Norwich FA"
    """
    row = find_school(name)
    return row["short_name"] if row else name


def abbrev_name(name: str) -> str:
    """
    Return the 3-letter abbreviation for a school.
    Falls back to the first 3 characters uppercased if not found.
    """
    row = find_school(name)
    return row["abbrev"] if row else name[:3].upper()


def canonical_name(name: str) -> str:
    """
    Return the canonical name for a school (useful for normalising
    names that come in via Google Sheets or scraped data).
    Falls back to the original name if not found.
    """
    row = find_school(name)
    return row["canonical"] if row else name


def ciac_id(name: str) -> Optional[int]:
    """Return the CIAC school ID integer, or None if not a CT school / not found."""
    row = find_school(name)
    if row and row["ciac_id"] is not None:
        return row["ciac_id"]
    return None


def is_ct_school(name: str) -> bool:
    """Return True if the school is in the CIAC registry (i.e. a CT school)."""
    row = find_school(name)
    return row is not None and row["ciac_id"] is not None


# ── SCHOOLS: drop-in replacement for scrapeTEAMS.py's SCHOOLS list ───────────
# Format: list of (canonical_name, ciac_school_id) — same as before.
# Schools without a CIAC ID (out-of-state / girls-only) are excluded.
SCHOOLS: list[tuple[str, int]] = [
    (row["canonical"], row["ciac_id"])
    for row in _all_rows
    if row["ciac_id"] is not None
]


# ── Convenience: all canonical names ─────────────────────────────────────────
ALL_SCHOOL_NAMES: list[str] = [row["canonical"] for row in _all_rows]