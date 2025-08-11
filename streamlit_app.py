import io
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st

def read_any_table(upload):
    """Robust reader for CSV/TSV/TXT/XLS/XLSX with fallbacks."""
    if upload is None:
        return None
    name = upload.name.lower()
    raw = upload.getvalue()

    # Prefer explicit engines by extension
    if name.endswith(".xlsx"):
        try:
            return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        except Exception:
            pass
    if name.endswith(".xls"):
        try:
            return pd.read_excel(io.BytesIO(raw), engine="xlrd")
        except Exception:
            pass

    # Try Excel sniff (in case the extension is wrong)
    try:
        return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    except Exception:
        pass

    # Try CSV/TSV with common delimiters
    for sep in [",", "\t", ";", "|"]:
        for eng in ["c", "python"]:
            try:
                return pd.read_csv(io.BytesIO(raw), sep=sep, engine=eng, on_bad_lines="skip")
            except Exception:
                continue

    return None  # unreadable

def normalize_apn(apn: str) -> str:
    s = str(apn or "").strip()
    for ch in [" ", "-", "_", "/", "."]:
        s = s.replace(ch, "")
    return s.lower()

def addr_query(address, city, state, postal):
    parts = [str(address or "").strip(), str(city or "").strip(), str(state or "").strip(), str(postal or "").strip()]
    parts = [p for p in parts if p]
    return " ".join(parts)

def build_gis_links(row):
    county = str(row.get("County Finder", "") or "")
    state = str(row.get("State", "") or "")
    apn   = str(row.get("APN", "") or "")
    addrq = str(row.get("addr_query", "") or "")
    google_q = f"{county} {state} GIS parcel {apn}".strip()
    bing_q   = google_q
    appraiser_q = f"{county} {state} property appraiser {apn}".strip()
    return pd.Series({
        "GIS_Google": "https://www.google.com/search?q=" + quote_plus(google_q),
        "GIS_Bing":   "https://www.bing.com/search?q=" + quote_plus(bing_q),
        "Appraiser_Search": "https://www.google.com/search?q=" + quote_plus(appraiser_q),
        "GIS_By_Address": "https://www.google.com/search?q=" + quote_plus(f"{county} {state} GIS {addrq}"),
    })

def build_people_osint_links(row):
    addrq = str(row.get("addr_query", "") or "")
    return pd.Series({
        "OSINT_Google_Addr": "https://www.google.com/search?q=" + quote_plus(addrq),
        "OSINT_Bing_Addr":   "https://www.bing.com/search?q=" + quote_plus(addrq),
        "Whitepages":        "https://www.whitepages.com/address/" + addrq.replace(" ", "-"),
        "FastPeopleSearch":  "https://www.fastpeoplesearch.com/address/" + addrq.replace(" ", "-"),
        "BeenVerified":      "https://www.beenverified.com/people/search/?n=&citystatezip=" + quote_plus(addrq),
    })

def build_social_links(row):
    addrq = str(row.get("addr_query", "") or "")
    return pd.Series({
        "Facebook_Search": "https://www.facebook.com/search/top/?q=" + quote_plus(addrq),
        "LinkedIn_Search": "https://www.linkedin.com/search/results/all/?keywords=" + quote_plus(addrq),
        "X_Search":        "https://x.com/search?q=" + quote_plus(addrq) + "&src=typed_query",
    })

def merge_on_apn(base: pd.DataFrame, other: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if other is None or len(other) == 0:
        return base
    df = base.copy()
    pw = other.copy()
    for c in ["APN", "Parcel", "Parcel Number", "parcel", "parcel number"]:
        if c in pw.columns:
            pw.rename(columns={c: "APN"}, inplace=True)
            break
    if "APN" not in pw.columns:
        pw["APN"] = ""
    df["APN_key"] = df["APN"].astype(str).str.replace(r"\W+", "", regex=True).str.lower()
    pw["APN_key"] = pw["APN"].astype(str).str.replace(r"\W+", "", regex=True).str.lower()
    merged = df.merge(pw, how="left", on="APN_key", suffixes=("", suffix))
    return merged.drop(columns=["APN_key"], errors="ignore")

def to_excel_bytes(main_df: pd.DataFrame, meta: dict, batch_name: str) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        main_df.to_excel(writer, sheet_name="enriched", index=False)
        pd.DataFrame([meta]).to_excel(writer, sheet_name="meta", index=False)
        dict_rows = [{"column": c, "example": str(main_df[c].dropna().astype(str).head(1).values[0]) if c in main_df.columns and main_df[c].notna().any() else ""} for c in main_df.columns]
        pd.DataFrame(dict_rows).to_excel(writer, sheet_name="columns", index=False)
    bio.seek(0)
    return bio.read()

st.set_page_config(page_title="Surplus Funds OSINT", page_icon="💰", layout="wide")
st.markdown("# 🎮 Surplus Funds OSINT — Research Console")
st.caption("Upload → Enrich → Download. No logins, no scraping headaches.")

with st.sidebar:
    st.markdown("### ⚙️ Settings")
    batch_name = st.text_input("Batch name", value=f"batch_{datetime.now().strftime('%Y%m%d_%H%M')}")
    st.markdown("### 📥 Upload your lead file")
    lead_file = st.file_uploader("CSV / Excel (columns like APN, County, Address, City, State, Zip)", type=["csv","xlsx","xls","tsv","txt"])
    st.markdown("### ➕ Optional: Upload Propwire/PropertyRadar exports")
    propwire_file = st.file_uploader("Propwire export (CSV/XLSX)", type=["csv","xlsx","xls"], key="propwire")
    pradar_file   = st.file_uploader("PropertyRadar export (CSV/XLSX)", type=["csv","xlsx","xls"], key="propertyradar")
    st.markdown("### 🔎 Enrichment toggles")
    use_county = st.checkbox("Generate County GIS lookups", value=True)
    use_osint  = st.checkbox("Generate OSINT people-search links", value=True)
    use_social = st.checkbox("Generate social media dorks", value=True)
    run_btn = st.button("🚀 Run Enrichment", use_container_width=True)

col_main, col_prev = st.columns([3,2], gap="large")

with col_prev:
    st.markdown("### 👀 Preview")
    if lead_file:
        leads_df = read_any_table(lead_file)
        if leads_df is None or not isinstance(leads_df, pd.DataFrame) or leads_df.empty:
    st.error("File looks empty/unreadable. Use CSV or XLSX (if .xls, re-save as .xlsx).")
    st.stop()
if leads_df is None or not isinstance(leads_df, pd.DataFrame) or leads_df.empty:
            st.error("That file looks empty or unreadable. Re-export as CSV/XLSX and re-upload.")
        else:
            st.dataframe(leads_df.head(25), use_container_width=True)
            st.info(f"Detected {len(leads_df):,} rows · {len(leads_df.columns)} columns.")
    else:
        st.warning("Upload your leads file to begin.")

with col_main:
    st.markdown("### 🧠 Enrichment")
    if run_btn and lead_file:
        leads_df = read_any_table(lead_file)
        df = leads_df.copy()
        rename_map = {}
        for c in df.columns:
            lc = c.strip().lower()
            if lc in ["apn","parcel","parcel number","parcel_number"]:
                rename_map[c] = "APN"
            elif lc in ["address","property address","site address"]:
                rename_map[c] = "Property Address"
            elif lc in ["county","county finder","county_name"]:
                rename_map[c] = "County Finder"
            elif lc in ["city","town"]:
                rename_map[c] = "City"
            elif lc in ["state","st"]:
                rename_map[c] = "State"
            elif lc in ["zip","zipcode","postal code"]:
                rename_map[c] = "Zip"
        if rename_map:
            df = df.rename(columns=rename_map)
        for col in ["APN","Property Address","City","State","Zip","County Finder"]:
            if col not in df.columns:
                df[col] = ""
        df["APN_norm"] = df["APN"].astype(str).apply(normalize_apn)
        df["addr_query"] = df.apply(lambda r: addr_query(
            r.get("Property Address",""), r.get("City",""), r.get("State",""), r.get("Zip","")
        ), axis=1)
        propwire_df = read_any_table(propwire_file) if propwire_file else None
        pradar_df   = read_any_table(pradar_file) if pradar_file else None
        if propwire_df is not None and len(propwire_df)>0:
            df = merge_on_apn(df, propwire_df, suffix="_pw")
        if pradar_df is not None and len(pradar_df)>0:
            df = merge_on_apn(df, pradar_df, suffix="_pr")
        if use_county:
            df = pd.concat([df, df.apply(build_gis_links, axis=1)], axis=1)
        if use_osint:
            df = pd.concat([df, df.apply(build_people_osint_links, axis=1)], axis=1)
        if use_social:
            df = pd.concat([df, df.apply(build_social_links, axis=1)], axis=1)
        df["confidence_score"] = (
            df["APN"].astype(str).str.len().clip(upper=12).fillna(0).astype(float)/12
            + (df["Property Address"].astype(str).str.len()>0).astype(int)*0.5
            + (df["City"].astype(str).str.len()>0).astype(int)*0.1
            + (df["State"].astype(str).str.len()>0).astype(int)*0.1
        ).clip(upper=1.0).round(2)
        meta = {
            "rows_in": len(leads_df),
            "rows_out": len(df),
            "toggles": dict(county=use_county, osint=use_osint, social=use_social)
        }
        xls_bytes = to_excel_bytes(df, meta, batch_name)
        st.success("Enrichment complete.")
        st.download_button(
            "⬇️ Download Workbook",
            data=xls_bytes,
            file_name=f"{batch_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.markdown("#### 🔍 Result sample")
        st.dataframe(df.head(50), use_container_width=True)
    elif run_btn and not lead_file:
        st.error("Please upload a leads file first.")
