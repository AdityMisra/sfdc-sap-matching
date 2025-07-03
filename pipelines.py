import pandas as pd
import re
from difflib import SequenceMatcher
import tldextract
import usaddress

# Default corporate suffixes for canonicalisation
CORP_SUFFIXES = [
    r"\bINC\b", r"\bINC\.\b", r"\bCORP\b", r"\bCORPORATION\b",
    r"\bLLC\b", r"\bL\.L\.C\.\b", r"\bLTD\b", r"\bLIMITED\b",
    r"\bCOMPANY\b", r"\bCO\b", r"\bCO\.\b"
]


def canonicalise(name: str) -> str:
    s = str(name).upper()
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s]", " ", s)
    for suf in CORP_SUFFIXES:
        s = re.sub(suf, " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_english_row(row) -> bool:
    english_pattern = r'^[\x00-\x7F]+$'
    for val in row:
        if isinstance(val, str) and not re.match(english_pattern, val):
            return False
    return True


def extract_domain(url: str) -> str:
    if isinstance(url, str):
        return tldextract.extract(url).domain
    return ""


def addr_score(addr1: str, addr2: str) -> float:
    a1, a2 = addr1.upper(), addr2.upper()
    try:
        p1 = usaddress.tag(a1)[0]
        p2 = usaddress.tag(a2)[0]
    except Exception:
        # fallback to token-based Jaccard
        t1 = set(a1.replace(",", " ").split())
        t2 = set(a2.replace(",", " ").split())
        if not t1 or not t2:
            return 0.0
        return round(len(t1 & t2) / len(t1 | t2), 3)

    score = 0.0
    # Street fuzzy (40%)
    parts1 = " ".join(filter(None, [
        p1.get("AddressNumber", ""),
        p1.get("StreetNamePreDirectional", ""),
        p1.get("StreetName", ""),
        p1.get("StreetNamePostType", ""),
        p1.get("StreetNamePostDirectional", "")
    ]))
    parts2 = " ".join(filter(None, [
        p2.get("AddressNumber", ""),
        p2.get("StreetNamePreDirectional", ""),
        p2.get("StreetName", ""),
        p2.get("StreetNamePostType", ""),
        p2.get("StreetNamePostDirectional", "")
    ]))
    street_sim = SequenceMatcher(None, parts1, parts2).ratio()
    score += 0.4 * street_sim

    # City fuzzy (30%)
    city_sim = SequenceMatcher(None, p1.get("PlaceName", ""), p2.get("PlaceName", "")).ratio()
    score += 0.3 * city_sim

    # State exact (20%)
    if p1.get("StateName") == p2.get("StateName"):
        score += 0.2

    # ZIP exact (10%)
    if p1.get("ZipCode") == p2.get("ZipCode"):
        score += 0.1

    return round(min(score, 1.0), 3)


def run_top_vs_sfdc(
    top_stream,
    sfdc_stream,
    threshold_top_sf: float = 0.85,
    header_row: int = 2,
    top_col: str = "End Customer",
    sfdc_cols: dict = None
) -> pd.DataFrame:
    """
    Step 1 matching: Top list vs SFDC accounts.
    Parameters:
      - top_stream: file-like for top.xlsx
      - sfdc_stream: file-like for SFDC data
      - threshold_top_sf: similarity cutoff
      - header_row: row index for top file header
      - top_col: column name for End Customer in top file
      - sfdc_cols: dict mapping {
            'id','name','website','street','state','parent'
        } to actual SFDC columns
    """
    # default SFDC column mapping
    if sfdc_cols is None:
        sfdc_cols = {
            'id':      'Account ID',
            'name':    'Account Name',
            'website': 'Website',
            'street':  'Billing Street',
            'state':   'Billing State/Province',
            'parent':  'Parent Account'
        }

    # Load and canonicalise top list
    top_df = pd.read_excel(top_stream, header=header_row, usecols=[top_col], engine="openpyxl")
    top_list = top_df[top_col].dropna().unique().tolist()
    top_canon = {cust: canonicalise(cust) for cust in top_list}
    top_canon_set = set(top_canon.values())

    # Load SFDC data
    df = pd.read_excel(sfdc_stream, engine="openpyxl")
    df = df[df.apply(is_english_row, axis=1)]

    df["canon_name"]   = df[sfdc_cols['name']].apply(canonicalise)
    df["block_letter"] = df["canon_name"].str[:1]
    df["domain"]       = df[sfdc_cols['website']].apply(extract_domain)

    matches = []
    for cust, cust_canon in top_canon.items():
        block = df[df["block_letter"] == cust_canon[:1]]
        for _, row in block.iterrows():
            sim = SequenceMatcher(None, cust_canon, row["canon_name"]).ratio()
            exact_name   = (cust_canon == row["canon_name"])
            exact_domain = False  # no top-side URL to compare

            parent = row.get(sfdc_cols['parent'], "")
            parent_canon = canonicalise(parent) if pd.notna(parent) else ""
            child = parent_canon in top_canon_set

            score = sim
            if exact_name or exact_domain:
                score = 1.0
            score = min(score, 1.0)

            if score >= threshold_top_sf:
                addr = (
                    (str(row[sfdc_cols['street']]) if pd.notna(row[sfdc_cols['street']]) else "") +
                    ", " +
                    (str(row[sfdc_cols['state']]) if pd.notna(row[sfdc_cols['state']]) else "")
                ).strip(", ")
                matches.append({
                    "End_Customer": cust,
                    "Acct_SFDC_ID": row[sfdc_cols['id']],
                    "SFDC_Name":    row[sfdc_cols['name']],
                    "Address":      addr,
                    "Similarity":   round(sim, 3),
                    "Exact_Name":   exact_name,
                    "Exact_Domain": exact_domain,
                    "Child":        child,
                    "Score":        round(score, 5)
                })

    return pd.DataFrame(matches)


def run_sfdc_vs_sap(
    sfdc_matches_df: pd.DataFrame,
    sap_stream,
    threshold_sf_sap: float = 0.85,
    sap_cols: dict = None
) -> (pd.DataFrame, pd.DataFrame):
    """
    Step 2 matching: SFDC matches vs SAP data.
    Returns (auto_matches_df, manual_review_df)
    """
    # default SAP column mapping
    if sap_cols is None:
        sap_cols = {
            'customer': 'Customer',
            'name1':    'Name 1',
            'name2':    'Name 2',
            'street':   'Street',
            'city':     'City',
            'region':   'Rg',
            'postal':   'PostalCode'
        }

    sap = pd.read_excel(sap_stream, engine="openpyxl")
    sap = sap[sap.apply(is_english_row, axis=1)]
    sap = sap.drop_duplicates(subset=[sap_cols['customer']])

    sap["Name 1"] = (
        sap[sap_cols['name1']].fillna("").astype(str) + " " +
        sap[sap_cols['name2']].fillna("").astype(str)
    ).str.strip()
    sap["Address"] = (
        sap[sap_cols['street']].fillna("").astype(str) + ", " +
        sap[sap_cols['city']].fillna("").astype(str) + ", " +
        sap[sap_cols['region']].fillna("").astype(str) + " " +
        sap[sap_cols['postal']].fillna("").astype(str)
    ).str.strip(", ")

    sap["canon_name"]   = sap["Name 1"].apply(canonicalise)
    sap["block_letter"] = sap["canon_name"].str[:1]

    auto_matches = []
    manual_review = []

    for _, sf in sfdc_matches_df.iterrows():
        cust = sf["SFDC_Name"]
        acc_id = sf["Acct_SFDC_ID"]
        cust_canon = canonicalise(cust)
        block = sap[sap["block_letter"] == cust_canon[:1]]
        candidates = []

        # gather name-sim candidates
        for _, row in block.iterrows():
            sim = SequenceMatcher(None, cust_canon, row["canon_name"]).ratio()
            if sim >= threshold_sf_sap:
                candidates.append({
                    "SAP_ID":     row[sap_cols['customer']],
                    "SAP_Name":   row["Name 1"],
                    "Address":    row["Address"],
                    "Name_Score": round(sim, 3)
                })

        if not candidates:
            continue

        perfects = [c for c in candidates if c["Name_Score"] == 1.0]

        if len(perfects) > 1:
            # address tie-break among perfects
            sf_addr = sf["Address"]
            for c in perfects:
                c["Addr_Score"] = addr_score(sf_addr, c["Address"])

            perfect_addr = [c for c in perfects if c["Addr_Score"] == 1.0]
            if perfect_addr:
                for pick in perfect_addr:
                    auto_matches.append({
                        "SFDC ID":      acc_id,
                        "SAP ID":       pick["SAP_ID"],
                        "SFDC Name":    cust,
                        "SAP Name":     pick["SAP_Name"],
                        "SFDC Address": sf_addr,
                        "SAP Address":  pick["Address"],
                        "Name Score":   pick["Name_Score"],
                        "Address Score": pick["Addr_Score"],
                        "Decision":     "PERFECT_NAME_AND_ADDRESS_MATCH"
                    })
            else:
                for c in perfects:
                    manual_review.append({
                        "SFDC ID":      acc_id,
                        "SAP ID":       c["SAP_ID"],
                        "SFDC Name":    cust,
                        "SAP Name":     c["SAP_Name"],
                        "SFDC Address": sf_addr,
                        "SAP Address":  c["Address"],
                        "Name Score":   c["Name_Score"],
                        "Address Score": c.get("Addr_Score", ""),
                        "Decision":     "REVIEW_NAME_MATCH_ADDRESS_MISMATCH"
                    })
        else:
            for c in candidates:
                if c["Name_Score"] == 1.0:
                    auto_matches.append({
                        "SFDC ID":      acc_id,
                        "SAP ID":       c["SAP_ID"],
                        "SFDC Name":    cust,
                        "SAP Name":     c["SAP_Name"],
                        "SFDC Address": sf["Address"],
                        "SAP Address":  c["Address"],
                        "Name Score":   c["Name_Score"],
                        "Address Score": "Name matches (no need of address match)",
                        "Decision":     "PERFECT_NAME_SINGLE_MATCH"
                    })
                else:
                    manual_review.append({
                        "SFDC ID":      acc_id,
                        "SAP ID":       c["SAP_ID"],
                        "SFDC Name":    cust,
                        "SAP Name":     c["SAP_Name"],
                        "SFDC Address": sf["Address"],
                        "SAP Address":  c["Address"],
                        "Name Score":   c["Name_Score"],
                        "Address Score": "Name doesn't match, address score not calculated",
                        "Decision":     "NEED_REVIEW_NAME_MISMATCH"
                    })

    auto_df   = pd.DataFrame(auto_matches)
    manual_df = pd.DataFrame(manual_review)
    return auto_df, manual_df