"""
Local dashboard for browsing PDAC trial results.

Run:
    streamlit run frontend/dashboard.py
"""

from pathlib import Path
import math
import sqlite3
import shlex

import altair as alt
import pandas as pd
import streamlit as st

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "pdac_trials.db"


def split_tags(tags: str) -> list[str]:
    if not tags:
        return []
    return [t.strip() for t in tags.split(",") if t.strip()]


def split_csv_values(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip() and item.strip() != "NA"]


def first_pubmed_link(value: str) -> str:
    if not value or str(value).strip() == "NA":
        return "NA"
    links = [item.strip() for item in str(value).split("|") if item.strip()]
    return links[0] if links else "NA"


def _year_from_date(value: str) -> str:
    if not value:
        return ""
    value = str(value).strip()
    if len(value) >= 4 and value[:4].isdigit():
        return value[:4]
    return ""


def _build_query_mask(df: pd.DataFrame, query: str) -> pd.Series:
    """
    Lightweight boolean query:
    - OR broadens results
    - AND restricts results
    - quoted phrases are supported: "after progression"
    Example: kras AND metastatic OR "phase 3"
    """
    query = (query or "").strip()
    if not query:
        return pd.Series(True, index=df.index)

    normalized = query.replace(",", " OR ")
    try:
        tokens = shlex.split(normalized)
    except Exception:
        tokens = normalized.split()

    # Build OR groups containing AND terms.
    groups: list[list[str]] = [[]]
    for token in tokens:
        upper = token.upper()
        if upper == "OR":
            if groups[-1]:
                groups.append([])
        elif upper == "AND":
            continue
        else:
            groups[-1].append(token)

    groups = [g for g in groups if g]
    if not groups:
        return pd.Series(True, index=df.index)

    row_text = df.fillna("").astype(str).agg(" | ".join, axis=1)
    final_mask = pd.Series(False, index=df.index)
    for and_terms in groups:
        group_mask = pd.Series(True, index=df.index)
        for term in and_terms:
            group_mask = group_mask & row_text.str.contains(
                term,
                case=False,
                regex=False,
                na=False,
            )
        final_mask = final_mask | group_mask
    return final_mask


def build_display_df(filtered: pd.DataFrame) -> pd.DataFrame:
    display_src = filtered.copy()
    display_src["nct_ref_id"] = display_src.apply(
        lambda row: (
            row["nct_id"]
            if str(row.get("nct_id", "")).startswith("NCT")
            else next(
                (
                    part.strip()
                    for part in str(row.get("secondary_id", "")).split(",")
                    if part.strip().startswith("NCT")
                ),
                "NA",
            )
        ),
        axis=1,
    )

    return (
        display_src.sort_values("nct_id", kind="stable")
        [
            [
                "nct_id",
                "nct_ref_id",
                "source",
                "trial_link",
                "title",
                "study_type",
                "study_design",
                "phase",
                "status",
                "sponsor",
                "therapeutic_class",
                "admission_date",
                "last_update_date",
                "has_results",
                "results_last_update",
                "pubmed_links",
                "conditions",
                "interventions",
                "intervention_types",
                "primary_outcomes",
                "secondary_outcomes",
                "inclusion_criteria",
                "exclusion_criteria",
                "locations",
                "brief_summary",
                "detailed_description",
                "focus_tags",
                "pdac_match_reason",
            ]
        ]
        .copy()
        .rename(
            columns={
                "nct_id": "Trial ID",
                "nct_ref_id": "NCT ID",
                "source": "Source",
                "trial_link": "Trial Link",
                "title": "Title",
                "study_type": "Study Type",
                "study_design": "Study Design",
                "phase": "Phase",
                "status": "Status",
                "sponsor": "Sponsor",
                "therapeutic_class": "Therapeutic Class",
                "admission_date": "Admission Date",
                "last_update_date": "Last Update",
                "has_results": "Results",
                "results_last_update": "Results Update",
                "pubmed_links": "Paper Link",
                "conditions": "Conditions",
                "interventions": "Interventions",
                "intervention_types": "Intervention Types",
                "primary_outcomes": "Primary Outcomes",
                "secondary_outcomes": "Secondary Outcomes",
                "inclusion_criteria": "Inclusion Criteria",
                "exclusion_criteria": "Exclusion Criteria",
                "locations": "Locations",
                "brief_summary": "Brief Summary",
                "detailed_description": "Detailed Description",
                "focus_tags": "Tags",
                "pdac_match_reason": "Match Reason",
            }
        )
    )


@st.cache_data(show_spinner=False)
def load_trials(cache_buster: float = 0.0) -> pd.DataFrame:
    _ = cache_buster
    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        try:
            df = pd.read_sql_query(
                """
                SELECT
                    c.nct_id,
                    c.source,
                    c.secondary_id,
                    c.trial_link,
                    c.title,
                    c.study_type,
                    c.study_design,
                    c.phase,
                    c.status,
                    c.sponsor,
                    c.admission_date,
                    c.last_update_date,
                    c.has_results,
                    c.results_last_update,
                    c.pubmed_links,
                    c.intervention_types,
                    c.therapeutic_class,
                    c.focus_tags,
                    c.pdac_match_reason,
                    d.conditions,
                    d.interventions,
                    d.primary_outcomes,
                    d.secondary_outcomes,
                    d.inclusion_criteria,
                    d.exclusion_criteria,
                    d.locations,
                    d.brief_summary,
                    d.detailed_description
                FROM clinical_trials c
                LEFT JOIN clinical_trial_details d ON d.nct_id = c.nct_id
                ORDER BY c.nct_id
                """,
                conn,
            )
        except Exception as exc:
            if "pubmed_links" not in str(exc).lower():
                raise
            df = pd.read_sql_query(
                """
                SELECT
                    c.nct_id,
                    c.source,
                    c.secondary_id,
                    c.trial_link,
                    c.title,
                    c.study_type,
                    c.study_design,
                    c.phase,
                    c.status,
                    c.sponsor,
                    c.admission_date,
                    c.last_update_date,
                    c.has_results,
                    c.results_last_update,
                    c.intervention_types,
                    c.therapeutic_class,
                    c.focus_tags,
                    c.pdac_match_reason,
                    d.conditions,
                    d.interventions,
                    d.primary_outcomes,
                    d.secondary_outcomes,
                    d.inclusion_criteria,
                    d.exclusion_criteria,
                    d.locations,
                    d.brief_summary,
                    d.detailed_description
                FROM clinical_trials c
                LEFT JOIN clinical_trial_details d ON d.nct_id = c.nct_id
                ORDER BY c.nct_id
                """,
                conn,
            )
            df["pubmed_links"] = ""
    finally:
        conn.close()

    expected_cols = [
        "nct_id",
        "source",
        "secondary_id",
        "trial_link",
        "title",
        "study_type",
        "study_design",
        "phase",
        "status",
        "sponsor",
        "admission_date",
        "last_update_date",
        "has_results",
        "results_last_update",
        "pubmed_links",
        "conditions",
        "interventions",
        "intervention_types",
        "primary_outcomes",
        "secondary_outcomes",
        "inclusion_criteria",
        "exclusion_criteria",
        "locations",
        "brief_summary",
        "detailed_description",
        "therapeutic_class",
        "focus_tags",
        "pdac_match_reason",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    # Backfill has_results when source does not explicitly provide it.
    df["has_results"] = df["has_results"].astype(str)
    inferred = (
        (df["has_results"].str.strip() == "")
        & (df["results_last_update"].astype(str).str.strip() != "")
    )
    df.loc[inferred, "has_results"] = "yes"
    df.loc[df["has_results"].str.strip() == "", "has_results"] = "no"
    df["pubmed_links"] = df["pubmed_links"].fillna("").astype(str).str.strip()
    df.loc[df["pubmed_links"] == "", "pubmed_links"] = "NA"
    df.loc[
        (df["pubmed_links"] != "NA")
        & (~df["has_results"].str.strip().str.lower().eq("yes")),
        "has_results",
    ] = "yes"

    df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
    df.loc[df["source"] == "", "source"] = df["nct_id"].apply(
        lambda value: "clinicaltrials.gov" if str(value).startswith("NCT") else "ctis"
    )
    df["secondary_id"] = df["secondary_id"].fillna("").astype(str).str.strip()
    df["trial_link"] = df["trial_link"].fillna("").astype(str).str.strip()
    df.loc[
        (df["trial_link"] == "") & (df["nct_id"].astype(str).str.startswith("NCT")),
        "trial_link",
    ] = df["nct_id"].apply(lambda value: f"https://clinicaltrials.gov/study/{value}")
    df.loc[
        (df["trial_link"] == "") & (~df["nct_id"].astype(str).str.startswith("NCT")),
        "trial_link",
    ] = df["nct_id"].apply(
        lambda value: f"https://euclinicaltrials.eu/search-for-clinical-trials/?lang=en&EUCT={value}"
    )

    df = df[expected_cols]
    return df.fillna("")


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Quick filters")
    st.sidebar.caption("Fast, convenient filters (you can also filter directly in the table).")
    query = st.session_state.get("global_query", "")

    class_options = sorted([x for x in df["therapeutic_class"].unique() if x])
    selected_classes = st.sidebar.multiselect("Therapeutic class", class_options)

    design_options = sorted([x for x in df["study_design"].unique() if x])
    selected_designs = st.sidebar.multiselect("Study design", design_options)

    type_options = sorted([x for x in df["study_type"].unique() if x])
    selected_types = st.sidebar.multiselect("Study type", type_options)

    phase_options = sorted([x for x in df["phase"].unique() if x])
    selected_phases = st.sidebar.multiselect("Phase", phase_options)

    status_options = sorted([x for x in df["status"].unique() if x])
    selected_statuses = st.sidebar.multiselect("Status", status_options)

    sponsor_options = sorted([x for x in df["sponsor"].unique() if x])
    selected_sponsors = st.sidebar.multiselect("Sponsor", sponsor_options)

    source_options = sorted([x for x in df["source"].unique() if x])
    selected_sources = st.sidebar.multiselect("Origin", source_options)

    intervention_type_options = sorted(
        {
            item
            for raw in df["intervention_types"].tolist()
            for item in split_csv_values(raw)
        }
    )
    selected_intervention_types = st.sidebar.multiselect(
        "Intervention type",
        intervention_type_options,
    )

    results_options = sorted([x for x in df["has_results"].unique() if x])
    selected_results = st.sidebar.multiselect("Results", results_options)

    admission_years = sorted({_year_from_date(x) for x in df["admission_date"] if _year_from_date(x)})
    selected_admission_years = st.sidebar.multiselect("Admission year", admission_years)

    update_years = sorted({_year_from_date(x) for x in df["last_update_date"] if _year_from_date(x)})
    selected_update_years = st.sidebar.multiselect("Last update year", update_years)

    all_tags = sorted({tag for tags in df["focus_tags"] for tag in split_tags(tags)})
    selected_tags = st.sidebar.multiselect("Focus tags", all_tags)

    out = df.copy()
    if query:
        out = out[_build_query_mask(out, query)]
    if selected_classes:
        out = out[out["therapeutic_class"].isin(selected_classes)]
    if selected_designs:
        out = out[out["study_design"].isin(selected_designs)]
    if selected_types:
        out = out[out["study_type"].isin(selected_types)]
    if selected_phases:
        out = out[out["phase"].isin(selected_phases)]
    if selected_statuses:
        out = out[out["status"].isin(selected_statuses)]
    if selected_sponsors:
        out = out[out["sponsor"].isin(selected_sponsors)]
    if selected_sources:
        out = out[out["source"].isin(selected_sources)]
    if selected_intervention_types:
        selected_set = set(selected_intervention_types)
        out = out[
            out["intervention_types"].apply(
                lambda raw: bool(selected_set.intersection(split_csv_values(raw)))
            )
        ]
    if selected_results:
        out = out[out["has_results"].isin(selected_results)]
    if selected_admission_years:
        out = out[out["admission_date"].apply(lambda x: _year_from_date(x) in selected_admission_years)]
    if selected_update_years:
        out = out[out["last_update_date"].apply(lambda x: _year_from_date(x) in selected_update_years)]
    if selected_tags:
        out = out[
            out["focus_tags"].apply(
                lambda tags: all(tag in split_tags(tags) for tag in selected_tags)
            )
        ]

    st.sidebar.markdown("---")
    st.sidebar.markdown("<div style='height:0.35rem;'></div>", unsafe_allow_html=True)
    st.sidebar.markdown(
        "<div style='display:flex; gap:0.25rem; align-items:center;'>"
        "<span class='sidebar-version-footer'>v1.3</span>"
        "<span class='sidebar-version-footer'>MIT License</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    return out


def metrics_row(total_df: pd.DataFrame, filtered_df: pd.DataFrame):
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">Filtered Trials</div>'
            f'<div class="metric-value">{len(filtered_df):,}</div></div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">Total Trials</div>'
            f'<div class="metric-value">{len(total_df):,}</div></div>',
            unsafe_allow_html=True,
        )
    with c3:
        statuses = filtered_df["status"].replace("", pd.NA).dropna().nunique()
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">Statuses</div>'
            f'<div class="metric-value">{statuses}</div></div>',
            unsafe_allow_html=True,
        )
    with c4:
        sponsors = filtered_df["sponsor"].replace("", pd.NA).dropna().nunique()
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">Sponsors</div>'
            f'<div class="metric-value">{sponsors}</div></div>',
            unsafe_allow_html=True,
        )
    with c5:
        with_results = (
            filtered_df["has_results"]
            .astype(str)
            .str.strip()
            .str.lower()
            .eq("yes")
            .sum()
        )
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">With Results</div>'
            f'<div class="metric-value">{int(with_results):,}</div></div>',
            unsafe_allow_html=True,
        )
    with c6:
        intervention_types = filtered_df["intervention_types"].replace("", pd.NA).dropna().nunique()
        st.markdown(
            f'<div class="metric-card"><div class="metric-label">Intervention Types</div>'
            f'<div class="metric-value">{intervention_types}</div></div>',
            unsafe_allow_html=True,
        )


def render_explorer(filtered: pd.DataFrame):
    theme_mode = st.session_state.get("theme_mode", "Normal")
    is_dark = theme_mode == "Dark"
    page_size = 25
    table_height = 980

    full_display_df = build_display_df(filtered)

    if full_display_df.empty:
        st.info("No trials match the current filters.")
        return

    hidden_columns = {"Trial Link"}
    all_columns = [c for c in full_display_df.columns if c not in hidden_columns]
    default_columns = [
        "Trial ID",
        "NCT ID",
        "Source",
        "Title",
        "Study Type",
        "Phase",
        "Status",
        "Sponsor",
        "Therapeutic Class",
        "Paper Link",
        "Intervention Types",
        "Admission Date",
        "Last Update",
        "Results",
        "Tags",
    ]
    default_columns = [c for c in default_columns if c in all_columns]
    with st.container(key="explorer_controls_banner_block"):
        st.markdown("<div style='height:0.35rem;'></div>", unsafe_allow_html=True)
        cols_pick_col, export_col = st.columns([8.8, 1.2], gap="small")
        with cols_pick_col:
            selected_columns = st.multiselect(
                "Columns to show",
                options=all_columns,
                default=default_columns,
            )
        if not selected_columns:
            selected_columns = default_columns
        if "Trial ID" not in selected_columns:
            selected_columns = ["Trial ID"] + selected_columns
        display_df = full_display_df[selected_columns + ["Trial Link"]].copy()
        if "Paper Link" in display_df.columns:
            display_df["Paper Link"] = display_df["Paper Link"].apply(first_pubmed_link)
        with export_col:
            st.markdown("<div style='height:1.95rem;'></div>", unsafe_allow_html=True)
            st.download_button(
                "Export filtered CSV",
                data=display_df.to_csv(index=False).encode("utf-8"),
                file_name="pdac_trials_filtered.csv",
                mime="text/csv",
                width="stretch",
                key="table_export_filtered_csv",
            )

        query_col = st.columns([1])[0]
        with query_col:
            st.markdown("<div style='height:0.25rem;'></div>", unsafe_allow_html=True)
            st.markdown(
                "<div class='query-hint'>Query mode: use <code>AND</code> to restrict, <code>OR</code> to broaden.</div>",
                unsafe_allow_html=True,
            )
            st.text_input(
                "Global text match (AND / OR)",
                key="global_query",
                label_visibility="collapsed",
                placeholder='Examples: kras AND metastatic OR "phase 3"',
            )
    st.markdown("<div style='margin-bottom:-0.75rem;'></div>", unsafe_allow_html=True)

    if HAS_AGGRID:
        try:
            gb = GridOptionsBuilder.from_dataframe(display_df)
            gb.configure_default_column(
                sortable=True,
                filter=True,
                resizable=True,
                flex=1,
                minWidth=115,
                wrapText=False,
                autoHeight=False,
                tooltipValueGetter=JsCode(
                    """
                    function(params) {
                        return params.value == null ? "" : String(params.value);
                    }
                    """
                ),
                cellStyle={
                    "whiteSpace": "nowrap",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                },
            )
            column_help = {
                "Trial ID": "Trial identifier from the original source (opens source record).",
                "NCT ID": "ClinicalTrials.gov NCT identifier when available (NA otherwise).",
                "Source": "Registry source for this trial row.",
                "Trial Link": "Canonical URL for opening the trial in its source registry.",
                "Title": "Official brief trial title.",
                "Study Type": "Interventional / Observational / Expanded access.",
                "Study Design": "Normalized design classification.",
                "Phase": "Clinical phase as reported by source.",
                "Status": "Current recruitment/overall status.",
                "Sponsor": "Lead sponsor organization.",
                "Therapeutic Class": "Normalized therapy strategy class.",
                "Admission Date": "Initial registration/posting date.",
                "Last Update": "Latest update date reported.",
                "Results": "Whether source indicates result availability.",
                "Results Update": "Date associated with results publication/update.",
                "Paper Link": "First linked PubMed paper found by NCT.",
                "Conditions": "Reported study conditions.",
                "Interventions": "Interventions with type and name.",
                "Intervention Types": "Unique intervention type(s) only.",
                "Primary Outcomes": "Primary endpoint definitions.",
                "Secondary Outcomes": "Secondary endpoint definitions.",
                "Inclusion Criteria": "Eligibility inclusion text.",
                "Exclusion Criteria": "Eligibility exclusion text.",
                "Locations": "Sites/locations from source.",
                "Brief Summary": "Short study description from source.",
                "Detailed Description": "Long study description from source.",
                "Tags": "Normalized focus tags.",
                "Match Reason": "Why trial was matched as PDAC-relevant.",
            }
            for col in display_df.columns:
                if col in column_help:
                    gb.configure_column(col, headerTooltip=column_help[col])
            gb.configure_pagination(
                enabled=True,
                paginationAutoPageSize=False,
                paginationPageSize=page_size,
            )
            gb.configure_column(
                "Trial ID",
                minWidth=130,
                maxWidth=170,
                pinned="left",
                cellStyle={"color": "#2f7a66", "textDecoration": "underline", "fontWeight": 600},
            )
            if "Trial Link" in display_df.columns:
                gb.configure_column("Trial Link", hide=True)
            if "Title" in display_df.columns:
                gb.configure_column("Title", minWidth=260, flex=2.2)
            if "Source" in display_df.columns:
                gb.configure_column("Source", minWidth=115, maxWidth=170)
            if "NCT ID" in display_df.columns:
                gb.configure_column(
                    "NCT ID",
                    minWidth=130,
                    maxWidth=180,
                    cellStyle={"color": "#2f7a66", "textDecoration": "underline", "fontWeight": 600},
                )
            if "Admission Date" in display_df.columns:
                gb.configure_column("Admission Date", minWidth=125, maxWidth=170)
            if "Last Update" in display_df.columns:
                gb.configure_column("Last Update", minWidth=125, maxWidth=170)
            if "Results" in display_df.columns:
                gb.configure_column("Results", minWidth=90, maxWidth=115)
            if "Results Update" in display_df.columns:
                gb.configure_column("Results Update", minWidth=130, maxWidth=180)
            if "Paper Link" in display_df.columns:
                gb.configure_column(
                    "Paper Link",
                    minWidth=190,
                    flex=1.35,
                    cellStyle={"color": "#2f7a66", "textDecoration": "underline"},
                )
            if "Intervention Types" in display_df.columns:
                gb.configure_column("Intervention Types", minWidth=145, maxWidth=210)
            if "Conditions" in display_df.columns:
                gb.configure_column("Conditions", minWidth=200, flex=1.4)
            if "Interventions" in display_df.columns:
                gb.configure_column("Interventions", minWidth=220, flex=1.5)
            if "Primary Outcomes" in display_df.columns:
                gb.configure_column("Primary Outcomes", minWidth=230, flex=1.6)
            if "Secondary Outcomes" in display_df.columns:
                gb.configure_column("Secondary Outcomes", minWidth=230, flex=1.6)
            if "Inclusion Criteria" in display_df.columns:
                gb.configure_column("Inclusion Criteria", minWidth=240, flex=1.7)
            if "Exclusion Criteria" in display_df.columns:
                gb.configure_column("Exclusion Criteria", minWidth=240, flex=1.7)
            if "Locations" in display_df.columns:
                gb.configure_column("Locations", minWidth=210, flex=1.3)
            if "Brief Summary" in display_df.columns:
                gb.configure_column("Brief Summary", minWidth=240, flex=1.8)
            if "Detailed Description" in display_df.columns:
                gb.configure_column("Detailed Description", minWidth=240, flex=1.8)
            if "Tags" in display_df.columns:
                gb.configure_column("Tags", minWidth=170, flex=1.2)
            if "Match Reason" in display_df.columns:
                gb.configure_column("Match Reason", minWidth=150, maxWidth=230)
            gb.configure_grid_options(
                rowHeight=34,
                tooltipShowDelay=100,
                onCellClicked=JsCode(
                    """
                    function(e) {
                        if (e.colDef.field === "Trial ID" && e.value) {
                            const rawLink = e.data && e.data["Trial Link"] ? String(e.data["Trial Link"]) : "";
                            const links = rawLink.includes("|")
                                ? rawLink.split("|").map(x => x.trim()).filter(Boolean)
                                : (rawLink.trim() ? [rawLink.trim()] : []);
                            const source = e.data && e.data["Source"] ? String(e.data["Source"]).toLowerCase() : "";
                            let trialLink = "";
                            // For merged rows show the non-NCT source from Trial ID, keeping NCT link in NCT ID.
                            if (source === "clinicaltrials.gov+ctis" && links.length > 1) {
                                trialLink = links[1];
                            } else if (links.length > 0) {
                                trialLink = links[0];
                            }
                            if (trialLink) {
                                window.open(trialLink, "_blank");
                            } else {
                                const trialId = String(e.value || "");
                                const fallback = trialId.startsWith("NCT")
                                    ? "https://clinicaltrials.gov/study/" + trialId
                                    : "https://euclinicaltrials.eu/search-for-clinical-trials/?lang=en&EUCT=" + encodeURIComponent(trialId);
                                window.open(fallback, "_blank");
                            }
                        }
                        if (e.colDef.field === "NCT ID" && e.value && e.value !== "NA") {
                            window.open("https://clinicaltrials.gov/study/" + String(e.value), "_blank");
                        }
                        if (e.colDef.field === "Paper Link" && e.value && e.value !== "NA") {
                            window.open(e.value, "_blank");
                        }
                    }
                    """
                ),
                onFirstDataRendered=JsCode(
                    """
                    function(params) {
                        params.api.sizeColumnsToFit();
                    }
                    """
                ),
                onGridSizeChanged=JsCode(
                    """
                    function(params) {
                        params.api.sizeColumnsToFit();
                    }
                    """
                ),
                enableCellTextSelection=True,
                ensureDomOrder=True,
            )

            grid_css = {
                ".ag-root-wrapper": {
                    "background-color": ("#111827 !important" if is_dark else "#ffffff !important"),
                    "border": ("1px solid #64748b !important" if is_dark else "1px solid #cbd5e1 !important"),
                    "border-radius": "6px !important",
                    "overflow": "hidden !important",
                },
                ".ag-root, .ag-body-viewport, .ag-center-cols-viewport": {
                    "background-color": ("#111827 !important" if is_dark else "#ffffff !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-header, .ag-header-viewport, .ag-header-container, .ag-pinned-left-header": {
                    "background-color": ("#020617 !important" if is_dark else "#eef2f7 !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                    "border-top": "none !important",
                },
                ".ag-header-row, .ag-header-cell": {
                    "border-bottom": ("3px solid #64748b !important" if is_dark else "2px solid #cbd5e1 !important"),
                    "border-top": "none !important",
                },
                ".ag-header-cell": {
                    "border-right": ("1px solid #64748b !important" if is_dark else "1px solid #cbd5e1 !important"),
                },
                ".ag-header-cell-label, .ag-header-cell-text": {
                    "font-weight": "700 !important",
                    "letter-spacing": "0.015em !important",
                    "text-transform": "none !important",
                    "color": ("#f8fafc !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-header-cell, .ag-cell, .ag-row, .ag-row-odd, .ag-row-even": {
                    "background-color": ("#111827 !important" if is_dark else "#ffffff !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                    "border-color": ("#374151 !important" if is_dark else "#e5e7eb !important"),
                },
                ".ag-row-odd .ag-cell": {
                    "background-color": ("#111827 !important" if is_dark else "#ffffff !important"),
                },
                ".ag-row-even .ag-cell": {
                    "background-color": ("#0b1220 !important" if is_dark else "#f8fafc !important"),
                },
                ".ag-row-hover .ag-cell, .ag-row-hover.ag-row-even .ag-cell, .ag-row-hover.ag-row-odd .ag-cell": {
                    "background-color": ("#1f2937 !important" if is_dark else "#eef2f7 !important"),
                },
                ".ag-paging-panel": {
                    "background-color": ("#111827 !important" if is_dark else "#ffffff !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                    "border-top": ("1px solid #374151 !important" if is_dark else "1px solid #e5e7eb !important"),
                },
                ".ag-paging-panel .ag-picker-field-wrapper, .ag-paging-panel .ag-input-field-input": {
                    "background-color": ("#0f172a !important" if is_dark else "#ffffff !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                    "border": ("1px solid #374151 !important" if is_dark else "1px solid #d1d5db !important"),
                },
                ".ag-paging-panel .ag-picker-field-display, .ag-paging-panel .ag-picker-field-icon": {
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-picker-field-popup, .ag-list, .ag-select-list": {
                    "background-color": ("#0f172a !important" if is_dark else "#ffffff !important"),
                    "border": ("1px solid #374151 !important" if is_dark else "1px solid #d1d5db !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-picker-field-popup .ag-list-item, .ag-select-list .ag-list-item": {
                    "background-color": ("#0f172a !important" if is_dark else "#ffffff !important"),
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-picker-field-popup .ag-list-item:hover, .ag-select-list .ag-list-item:hover": {
                    "background-color": ("#1f2937 !important" if is_dark else "#f3f4f6 !important"),
                },
                ".ag-paging-panel .ag-icon, .ag-paging-panel .ag-paging-row-summary-panel, .ag-paging-panel .ag-label": {
                    "color": ("#e5e7eb !important" if is_dark else "#1f2937 !important"),
                },
                ".ag-tooltip": {
                    "background-color": ("#1f2937 !important" if is_dark else "#f8fafc !important"),
                    "color": ("#f9fafb !important" if is_dark else "#1f2937 !important"),
                    "border": ("1px solid #374151 !important" if is_dark else "1px solid #dbe4f0 !important"),
                    "border-radius": "10px !important",
                    "box-shadow": "0 8px 24px rgba(15, 23, 42, 0.12) !important",
                    "padding": "8px 10px !important",
                    "font-size": "0.83rem !important",
                    "line-height": "1.35 !important",
                }
            }

            AgGrid(
                display_df,
                gridOptions=gb.build(),
                allow_unsafe_jscode=True,
                custom_css=grid_css,
                update_mode="NO_UPDATE",
                theme="streamlit",
                fit_columns_on_grid_load=True,
                height=table_height,
                key=f"aggrid_{theme_mode}",
            )
            return
        except Exception:
            st.warning("AgGrid failed to render. Using fallback pagination.")

    st.warning("`streamlit-aggrid` is not available. Using fallback pagination.")
    total_rows = len(display_df)
    total_pages = max(1, math.ceil(total_rows / page_size))
    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
    start = (page - 1) * page_size
    end = min(start + page_size, total_rows)

    page_df = display_df.iloc[start:end].copy()
    page_df["Trial ID"] = page_df.apply(
        lambda row: (
            str(row["Trial Link"]).split("|")[0].strip()
            if row.get("Trial Link")
            else (
                f"https://clinicaltrials.gov/study/{row['Trial ID']}"
                if str(row["Trial ID"]).startswith("NCT")
                else f"https://euclinicaltrials.eu/search-for-clinical-trials/?lang=en&EUCT={row['Trial ID']}"
            )
        ),
        axis=1,
    )
    if "Trial Link" in page_df.columns:
        page_df = page_df.drop(columns=["Trial Link"])
    if "NCT ID" in page_df.columns:
        page_df["NCT ID"] = page_df["NCT ID"].apply(
            lambda v: f"https://clinicaltrials.gov/study/{v}" if v and v != "NA" else ""
        )
    if "Paper Link" in page_df.columns:
        page_df["Paper Link"] = page_df["Paper Link"].apply(
            lambda link: link if link and link != "NA" else ""
        )
    st.caption(f"Showing rows {start + 1:,}-{end:,} of {total_rows:,}")
    column_cfg = {
        "Trial ID": st.column_config.LinkColumn("Trial ID", display_text="Open trial"),
    }
    if "NCT ID" in page_df.columns:
        column_cfg["NCT ID"] = st.column_config.LinkColumn("NCT ID", display_text="NCT")
    if "Paper Link" in page_df.columns:
        column_cfg["Paper Link"] = st.column_config.LinkColumn("Paper Link", display_text="PubMed")
    st.dataframe(
        page_df,
        width="stretch",
        height=table_height,
        hide_index=True,
        column_config=column_cfg,
    )


def render_analytics(filtered: pd.DataFrame):
    is_dark = st.session_state.get("theme_mode", "Normal") == "Dark"
    chart_bg = "#0f172a" if is_dark else "#ffffff"
    chart_text = "#e5e7eb" if is_dark else "#1f2937"
    chart_grid = "#334155" if is_dark else "#e5e7eb"

    def themed_chart(chart: alt.Chart) -> alt.Chart:
        return (
            chart.properties(background=chart_bg)
            .configure_axis(
                labelColor=chart_text,
                titleColor=chart_text,
                gridColor=chart_grid,
                domainColor=chart_grid,
                tickColor=chart_grid,
            )
            .configure_title(color=chart_text)
            .configure_view(stroke=chart_grid)
            .configure_legend(labelColor=chart_text, titleColor=chart_text)
        )

    left, right = st.columns([1.3, 1])
    with left:
        class_df = (
            filtered["therapeutic_class"]
            .replace("", "missing")
            .value_counts()
            .rename_axis("therapeutic_class")
            .reset_index(name="count")
        )
        class_df["therapeutic_class"] = class_df["therapeutic_class"].astype(str)
        class_df["count"] = class_df["count"].astype(int)
        class_chart = (
            alt.Chart(class_df)
            .mark_bar()
            .encode(
                x=alt.X("therapeutic_class:N", sort="-y", title="Therapeutic class"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["therapeutic_class:N", "count:Q"],
            )
            .properties(
                title="Therapeutic Class Distribution",
                height=390,
            )
        )
        st.altair_chart(themed_chart(class_chart), width="stretch")

    with right:
        sponsor_df = (
            filtered["sponsor"]
            .replace("", pd.NA)
            .dropna()
            .value_counts()
            .head(15)
            .rename_axis("sponsor")
            .reset_index(name="count")
        )
        sponsor_df["sponsor"] = sponsor_df["sponsor"].astype(str)
        sponsor_df["count"] = sponsor_df["count"].astype(int)
        sponsor_chart = (
            alt.Chart(sponsor_df)
            .mark_bar()
            .encode(
                x=alt.X("sponsor:N", sort="-y", title="Sponsor"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["sponsor:N", "count:Q"],
            )
            .properties(
                title="Top Sponsors (Filtered)",
                height=390,
            )
        )
        st.altair_chart(themed_chart(sponsor_chart), width="stretch")

    st.markdown("")
    phase_df = (
        filtered["phase"]
        .replace("", "NA")
        .value_counts()
        .rename_axis("phase")
        .reset_index(name="count")
    )
    phase_df["phase"] = phase_df["phase"].astype(str)
    phase_df["count"] = phase_df["count"].astype(int)
    phase_chart = (
        alt.Chart(phase_df)
        .mark_bar()
        .encode(
            x=alt.X("phase:N", sort="-y", title="Phase"),
            y=alt.Y("count:Q", title="Count"),
            tooltip=["phase:N", "count:Q"],
        )
        .properties(
            title="Phase Distribution",
            height=330,
        )
    )
    st.altair_chart(themed_chart(phase_chart), width="stretch")

    st.markdown("")
    study_type_df = (
        filtered["study_type"]
        .replace("", "Unknown")
        .value_counts()
        .rename_axis("study_type")
        .reset_index(name="count")
    )
    study_type_df["study_type"] = study_type_df["study_type"].astype(str)
    study_type_df["count"] = study_type_df["count"].astype(int)
    study_type_chart = (
        alt.Chart(study_type_df)
        .mark_bar()
        .encode(
            x=alt.X("study_type:N", sort="-y", title="Study type"),
            y=alt.Y("count:Q", title="Count"),
            tooltip=["study_type:N", "count:Q"],
        )
        .properties(
            title="Study Type Distribution",
            height=330,
        )
    )
    st.altair_chart(themed_chart(study_type_chart), width="stretch")

    st.markdown("")
    l2, r2 = st.columns([1, 1])
    with l2:
        status_df = (
            filtered["status"]
            .replace("", "NA")
            .value_counts()
            .rename_axis("status")
            .reset_index(name="count")
        )
        status_chart = (
            alt.Chart(status_df)
            .mark_bar()
            .encode(
                x=alt.X("status:N", sort="-y", title="Status"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["status:N", "count:Q"],
            )
            .properties(
                title="Status Distribution",
                height=330,
            )
        )
        st.altair_chart(themed_chart(status_chart), width="stretch")
    with r2:
        results_df = (
            filtered["has_results"]
            .replace("", "NA")
            .value_counts()
            .rename_axis("has_results")
            .reset_index(name="count")
        )
        results_chart = (
            alt.Chart(results_df)
            .mark_bar()
            .encode(
                x=alt.X("has_results:N", sort="-y", title="Results"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["has_results:N", "count:Q"],
            )
            .properties(
                title="Results Availability",
                height=330,
            )
        )
        st.altair_chart(themed_chart(results_chart), width="stretch")

    st.markdown("")
    l3, r3 = st.columns([1, 1])
    with l3:
        intervention_series = (
            filtered["intervention_types"]
            .apply(split_csv_values)
            .explode()
            .dropna()
        )
        if intervention_series.empty:
            intervention_df = pd.DataFrame(
                {"intervention_types": ["NA"], "count": [0]}
            )
        else:
            intervention_df = (
                intervention_series.value_counts()
                .head(12)
                .rename_axis("intervention_types")
                .reset_index(name="count")
            )
        intervention_chart = (
            alt.Chart(intervention_df)
            .mark_bar()
            .encode(
                x=alt.X("intervention_types:N", sort="-y", title="Intervention type"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["intervention_types:N", "count:Q"],
            )
            .properties(
                title="Intervention Type Distribution",
                height=330,
            )
        )
        st.altair_chart(themed_chart(intervention_chart), width="stretch")
    with r3:
        design_df = (
            filtered["study_design"]
            .replace("", "NA")
            .value_counts()
            .rename_axis("study_design")
            .reset_index(name="count")
        )
        design_chart = (
            alt.Chart(design_df)
            .mark_bar()
            .encode(
                x=alt.X("study_design:N", sort="-y", title="Study design"),
                y=alt.Y("count:Q", title="Count"),
                tooltip=["study_design:N", "count:Q"],
            )
            .properties(
                title="Study Design Distribution",
                height=330,
            )
        )
        st.altair_chart(themed_chart(design_chart), width="stretch")


def main():
    st.set_page_config(
        page_title="PDAC Trial Atlas",
        page_icon="🧬",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    if "theme_mode" not in st.session_state:
        st.session_state["theme_mode"] = "Normal"
    theme_mode = st.session_state["theme_mode"]

    if theme_mode == "Dark":
        colors = {
            "bg": "radial-gradient(circle at 2% 2%, #111827 0%, #0f172a 45%, #111827 100%)",
            "heading": "#e5e7eb",
            "card_bg": "rgba(17, 24, 39, 0.9)",
            "card_border": "#374151",
            "label": "#9ca3af",
            "value": "#f9fafb",
            "tab_bg": "#111827",
            "tab_border": "#374151",
            "tab_text": "#e5e7eb",
            "tab_active_bg": "#f9fafb",
            "tab_active_text": "#111827",
            "version_bg": "#1f2937",
            "version_border": "#374151",
            "license_bg": "#111827",
            "license_border": "#374151",
            "license_text": "#d1d5db",
            "grid_bg": "#111827",
            "grid_header_bg": "#1f2937",
            "grid_text": "#e5e7eb",
            "grid_border": "#374151",
            "sidebar_bg": "#0f172a",
            "sidebar_border": "#334155",
            "sidebar_input_bg": "#111827",
            "toggle_off_bg": "#334155",
            "multiselect_bg": "#0f172a",
            "multiselect_text": "#e5e7eb",
            "multiselect_tag_bg": "#1f2937",
            "multiselect_tag_text": "#e5e7eb",
            "multiselect_menu_bg": "#0f172a",
            "multiselect_clear_icon": "#cbd5e1",
            "multiselect_placeholder": "#cbd5e1",
        }
    else:
        colors = {
            "bg": "radial-gradient(circle at 2% 2%, #fff5e6 0%, #f2f6ff 42%, #eefaf4 100%)",
            "heading": "#1b2440",
            "card_bg": "rgba(255, 255, 255, 0.9)",
            "card_border": "#e6eaf3",
            "label": "#5f6d89",
            "value": "#1b2440",
            "tab_bg": "#f8fbff",
            "tab_border": "#cfd8ea",
            "tab_text": "#1b2440",
            "tab_active_bg": "#1b2440",
            "tab_active_text": "#ffffff",
            "version_bg": "#f3f4f6",
            "version_border": "#d1d5db",
            "license_bg": "#f3f4f6",
            "license_border": "#d1d5db",
            "license_text": "#1f2937",
            "grid_bg": "#ffffff",
            "grid_header_bg": "#f8fafc",
            "grid_text": "#1f2937",
            "grid_border": "#e5e7eb",
            "sidebar_bg": "#f8fafc",
            "sidebar_border": "#dbe3ee",
            "sidebar_input_bg": "#edf2f7",
            "toggle_off_bg": "#111827",
            "multiselect_bg": "#eef2f7",
            "multiselect_text": "#1f2937",
            "multiselect_tag_bg": "#e2e8f0",
            "multiselect_tag_text": "#1f2937",
            "multiselect_menu_bg": "#ffffff",
            "multiselect_clear_icon": "#64748b",
            "multiselect_placeholder": "#64748b",
        }

    st.markdown(
        f"""
        <style>
            .stApp {{ background: {colors["bg"]}; }}
            h1, h2, h3 {{ color: {colors["heading"]}; }}
            [data-testid="stSidebar"] > div:first-child {{
                background: {colors["sidebar_bg"]};
                border-right: 1px solid {colors["sidebar_border"]};
            }}
            [data-testid="stSidebar"] label,
            [data-testid="stSidebar"] .stMarkdown,
            [data-testid="stSidebar"] h1,
            [data-testid="stSidebar"] h2,
            [data-testid="stSidebar"] h3 {{
                color: {colors["heading"]} !important;
            }}
            [data-testid="stSidebar"] [data-baseweb="input"] > div {{
                background: {colors["sidebar_input_bg"]} !important;
                border: 1px solid {colors["card_border"]} !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            [data-testid="stSidebar"] [data-baseweb="input"] > div:focus-within {{
                border: 1px solid {colors["card_border"]} !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            [data-testid="stSidebar"] input,
            [data-testid="stSidebar"] textarea {{
                color: {colors["heading"]} !important;
                caret-color: {colors["heading"]} !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            [data-testid="stSidebar"] input::placeholder,
            [data-testid="stSidebar"] textarea::placeholder {{
                color: {colors["label"]} !important;
                opacity: 1 !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] [data-baseweb="input"] {{
                background: {colors["multiselect_bg"]} !important;
                border: 1px solid {colors["tab_border"]} !important;
                border-radius: 8px !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] [data-baseweb="input"] > div {{
                background: transparent !important;
                border: 0 !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {{
                border: 1px solid {colors["tab_border"]} !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] [data-baseweb="input"] > div:focus-within {{
                border: 0 !important;
                border-radius: 8px !important;
                box-shadow: none !important;
                outline: none !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] input {{
                color: {colors["heading"]} !important;
                caret-color: {colors["heading"]} !important;
            }}
            .st-key-explorer_controls_banner_block [data-testid="stTextInput"] input::placeholder {{
                color: {colors["label"]} !important;
                opacity: 1 !important;
            }}
            .st-key-explorer_controls_banner_block .query-hint {{
                color: {colors["label"]};
                font-size: 0.78rem;
                margin: 0.08rem 0 0.2rem 0;
                display: inline-flex;
                align-items: center;
                gap: 0.2rem;
                background: {colors["multiselect_bg"]} !important;
                border: 1px solid {colors["tab_border"]} !important;
                border-radius: 8px;
                padding: 0.22rem 0.42rem;
            }}
            .st-key-explorer_controls_banner_block .query-hint code {{
                color: {colors["heading"]};
                background: {colors["multiselect_bg"]} !important;
                border: 1px solid {colors["card_border"]} !important;
                border-radius: 6px;
                padding: 0 0.25rem;
            }}
            [data-testid="stToggle"] label,
            [data-testid="stToggle"] span {{
                color: {colors["heading"]} !important;
            }}
            [data-testid="stToggle"] [role="switch"] {{
                border: 1px solid {colors["card_border"]} !important;
                background: {colors["toggle_off_bg"]} !important;
            }}
            [data-testid="stToggle"] [role="switch"]:hover,
            [data-testid="stToggle"] [role="switch"]:focus {{
                border: 1px solid {colors["card_border"]} !important;
                background: {colors["toggle_off_bg"]} !important;
                box-shadow: none !important;
            }}
            [data-testid="stToggle"] [role="switch"][aria-checked="true"] {{
                background: #0f766e !important;
                border: 1px solid #0f766e !important;
            }}
            [data-testid="stToggle"] [role="switch"][aria-checked="true"]:hover,
            [data-testid="stToggle"] [role="switch"][aria-checked="true"]:focus {{
                background: #0f766e !important;
                border: 1px solid #0f766e !important;
                box-shadow: none !important;
            }}
            [data-testid="stToggle"] [data-baseweb="toggle"] {{
                background: {colors["toggle_off_bg"]} !important;
                border: 1px solid {colors["card_border"]} !important;
            }}
            [data-testid="stToggle"] [data-baseweb="toggle"][aria-checked="true"] {{
                background: #0f766e !important;
                border: 1px solid #0f766e !important;
            }}
            [data-testid="stToggle"] [role="switch"] > div,
            [data-testid="stToggle"] [data-baseweb="toggle"] > div {{
                background: #ffffff !important;
                border: 1px solid #475569 !important;
            }}
            .metric-card {{
                background: {colors["card_bg"]};
                border: 1px solid {colors["card_border"]};
                border-radius: 14px;
                padding: 12px 16px;
                box-shadow: 0 8px 18px rgba(27, 36, 64, 0.08);
            }}
            .metric-label {{ color: {colors["label"]}; font-size: 0.86rem; }}
            .metric-value {{ color: {colors["value"]}; font-size: 1.48rem; font-weight: 700; }}
            a {{ color: #2f7a66 !important; }}
            [data-testid="stDownloadButton"] button {{
                background: linear-gradient(135deg, #0f766e 0%, #115e59 100%) !important;
                color: #ffffff !important;
                border: 1px solid #0f766e !important;
                border-radius: 8px !important;
                box-shadow: 0 4px 12px rgba(15, 118, 110, 0.24) !important;
                font-size: 0.72rem !important;
                font-weight: 700 !important;
                min-height: 1.9rem !important;
                line-height: 1 !important;
                padding: 0.12rem 0.55rem !important;
            }}
            [data-testid="stDownloadButton"] button:hover {{
                background: linear-gradient(135deg, #0b5f58 0%, #0f4f4a 100%) !important;
                color: #ffffff !important;
                border: 1px solid #0b5f58 !important;
                box-shadow: 0 5px 14px rgba(15, 118, 110, 0.28) !important;
            }}
            .stButton > button {{
                font-size: 0.5rem;
                min-height: 0.95rem;
                padding: 0.01rem 0.16rem;
            }}
            button[data-testid="stBaseButton-secondary"] {{
                background: #e5e7eb !important;
                color: #111827 !important;
                border: 1px solid #d1d5db !important;
            }}
            button[data-testid="stBaseButton-secondary"]:hover {{
                background: #dbe2ea !important;
                color: #111827 !important;
                border: 1px solid #c8d1db !important;
            }}
            button[data-testid="stBaseButton-primary"] {{
                background: #020617 !important;
                color: #ffffff !important;
                border: 1px solid #020617 !important;
            }}
            button[data-testid="stBaseButton-primary"]:hover {{
                background: #000000 !important;
                color: #ffffff !important;
                border: 1px solid #000000 !important;
            }}
            .theme-label-inline {{
                color: {colors["heading"]};
                font-size: 0.76rem;
                font-weight: 700;
                text-align: right;
                padding-top: 0.28rem;
            }}
            .header-title-compact {{
                color: {"#ffffff" if theme_mode == "Dark" else colors["heading"]} !important;
                font-size: 1.7rem;
                font-weight: 700;
                margin: 0;
                line-height: 1.05;
            }}
            .subtitle-strong {{
                color: {colors["heading"]};
                font-size: 0.86rem;
                font-weight: 600;
                margin-top: -0.12rem;
                margin-bottom: 0.22rem;
                line-height: 1.2;
            }}
            .st-key-header_banner_block,
            .st-key-explorer_controls_banner_block {{
                background: {colors["card_bg"]};
                border: 1px solid {colors["card_border"]};
                border-radius: 14px;
                padding: 0.5rem 0.8rem 0.45rem 0.8rem;
                box-shadow: 0 8px 18px rgba(27, 36, 64, 0.08);
            }}
            .st-key-header_banner_block {{
                margin-bottom: 0.3rem;
                padding: 0.44rem 0.76rem 1.05rem 0.76rem;
                position: sticky;
                top: 0rem;
                z-index: 940;
            }}
            .st-key-header_banner_block [data-testid="stToggle"] {{
                display: flex;
                justify-content: flex-end;
                align-items: center;
                margin-top: 0.03rem;
            }}
            .st-key-header_banner_block [data-testid="stToggle"] label,
            .st-key-header_banner_block [data-testid="stToggle"] span {{
                color: {"#ffffff" if theme_mode == "Dark" else colors["heading"]} !important;
                margin-bottom: 0 !important;
                font-size: 0.82rem !important;
            }}
            .st-key-explorer_controls_banner_block {{
                margin-top: 0.25rem;
                position: sticky;
                top: 14.4rem;
                z-index: 930;
            }}
            @media (max-width: 760px) {{
                .header-title-compact {{
                    font-size: 1.45rem;
                }}
                .stButton > button {{
                    font-size: 0.58rem;
                    min-height: 1.1rem;
                }}
            }}
            /* Columns to show (multiselect) theming */
            [data-testid="stMultiSelect"] label {{
                color: {colors["heading"]} !important;
                font-weight: 600 !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="select"] > div {{
                background: {colors["multiselect_bg"]} !important;
                border: 1px solid {colors["card_border"]} !important;
                color: {colors["multiselect_text"]} !important;
            }}
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] input {{
                color: {colors["multiselect_placeholder"]} !important;
                -webkit-text-fill-color: {colors["multiselect_placeholder"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] input::placeholder,
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] input::-webkit-input-placeholder {{
                color: {colors["multiselect_placeholder"]} !important;
                -webkit-text-fill-color: {colors["multiselect_placeholder"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [aria-live="polite"] {{
                color: {colors["multiselect_placeholder"]} !important;
            }}
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [role="combobox"],
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [role="combobox"] * {{
                color: {colors["multiselect_placeholder"]} !important;
                -webkit-text-fill-color: {colors["multiselect_placeholder"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [class*="placeholder"],
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [class*="Placeholder"],
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [class*="singleValue"],
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] [class*="SingleValue"],
            [data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-baseweb="select"] div[aria-hidden="true"] {{
                color: {colors["multiselect_placeholder"]} !important;
                -webkit-text-fill-color: {colors["multiselect_placeholder"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
                background: {colors["multiselect_tag_bg"]} !important;
                border: 1px solid {colors["tab_border"]} !important;
                color: {colors["multiselect_tag_text"]} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] svg,
            [data-testid="stMultiSelect"] [data-baseweb="tag"] [role="button"],
            [data-testid="stMultiSelect"] [data-baseweb="tag"] button {{
                color: {colors["multiselect_tag_text"]} !important;
                fill: {colors["multiselect_tag_text"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] svg path {{
                fill: {colors["multiselect_tag_text"]} !important;
                stroke: {colors["multiselect_tag_text"]} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="tag"] [role="button"]:hover,
            [data-testid="stMultiSelect"] [data-baseweb="tag"] button:hover {{
                color: {colors["multiselect_tag_text"]} !important;
                fill: {colors["multiselect_tag_text"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stMultiSelect"] input {{
                color: {colors["multiselect_text"]} !important;
            }}
            div[data-baseweb="popover"] ul {{
                background: {colors["multiselect_menu_bg"]} !important;
                color: {colors["multiselect_text"]} !important;
            }}
            div[data-baseweb="popover"] li {{
                color: {colors["multiselect_text"]} !important;
            }}
            [data-testid="stMultiSelect"] [aria-label*="Clear"],
            [data-testid="stMultiSelect"] [title*="Clear"] {{
                color: {colors["multiselect_clear_icon"]} !important;
                fill: {colors["multiselect_clear_icon"]} !important;
                opacity: 1 !important;
            }}
            [data-testid="stMultiSelect"] [aria-label*="Clear"] svg,
            [data-testid="stMultiSelect"] [title*="Clear"] svg,
            [data-testid="stMultiSelect"] [aria-label*="Clear"] svg path,
            [data-testid="stMultiSelect"] [title*="Clear"] svg path {{
                color: {colors["multiselect_clear_icon"]} !important;
                fill: {colors["multiselect_clear_icon"]} !important;
                stroke: {colors["multiselect_clear_icon"]} !important;
            }}
            [data-testid="stMultiSelect"] [data-baseweb="select"] svg,
            [data-testid="stMultiSelect"] [data-baseweb="select"] svg path {{
                color: {colors["multiselect_clear_icon"]} !important;
                fill: {colors["multiselect_clear_icon"]} !important;
                stroke: {colors["multiselect_clear_icon"]} !important;
                opacity: 1 !important;
            }}
            .stTabs [data-baseweb="tab-list"] {{
                gap: 0.45rem;
                margin-top: 0.3rem;
                border-bottom: 2px solid {colors["tab_border"]};
                padding-bottom: 0.2rem;
                position: sticky;
                top: 5.6rem;
                z-index: 935;
                background: {colors["bg"]};
            }}
            .stTabs [data-baseweb="tab"] {{
                height: 2.4rem;
                background: {colors["tab_bg"]};
                border: 1px solid {colors["tab_border"]};
                border-radius: 10px 10px 0 0;
                color: {colors["tab_text"]};
                font-weight: 700;
                padding: 0 1rem;
            }}
            .stTabs [aria-selected="true"] {{
                background: {colors["tab_active_bg"]} !important;
                color: {colors["tab_active_text"]} !important;
                border: 1px solid {colors["tab_active_bg"]} !important;
            }}
            .st-key-metrics_banner_block {{
                position: sticky;
                top: 8.8rem;
                z-index: 932;
                background: {colors["bg"]};
                padding-top: 0.18rem;
                padding-bottom: 0.18rem;
            }}
            @media (max-width: 980px) {{
                .stTabs [data-baseweb="tab-list"] {{ top: 6.1rem; }}
                .st-key-metrics_banner_block {{ top: 9.5rem; }}
                .st-key-explorer_controls_banner_block {{ top: 17.7rem; }}
            }}
            .sidebar-version-footer {{
                color: {colors["heading"]};
                font-weight: 600;
                font-size: 0.72rem;
                background: {colors["version_bg"]};
                border: 1px solid {colors["version_border"]};
                border-radius: 999px;
                padding: 2px 8px;
                display: inline-block;
            }}
            .ag-theme-streamlit,
            .ag-theme-streamlit .ag-root-wrapper,
            .ag-theme-streamlit .ag-root,
            .ag-theme-streamlit .ag-body-viewport {{
                background-color: {colors["grid_bg"]} !important;
                color: {colors["grid_text"]} !important;
            }}
            .ag-theme-streamlit .ag-header {{
                background-color: {colors["grid_header_bg"]} !important;
            }}
            .ag-theme-streamlit .ag-row,
            .ag-theme-streamlit .ag-row .ag-cell {{
                background-color: {colors["grid_bg"]} !important;
                color: {colors["grid_text"]} !important;
            }}
            .ag-theme-streamlit .ag-header-cell-label,
            .ag-theme-streamlit .ag-header-cell-text,
            .ag-theme-streamlit .ag-cell-value {{
                color: {colors["grid_text"]} !important;
            }}
            .ag-theme-streamlit .ag-header-cell,
            .ag-theme-streamlit .ag-cell {{
                border-color: {colors["grid_border"]} !important;
            }}
            .main .block-container {{ padding-top: 0.22rem; padding-bottom: 1rem; }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    db_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else 0.0
    df = load_trials(db_mtime)
    if df.empty:
        st.warning("No local dataset found yet.")
        st.caption(
            "For Streamlit Cloud, use the button below to initialize data from ClinicalTrials.gov and CTIS."
        )
        if st.button("Initialize dataset", type="primary", width="content"):
            with st.spinner("Fetching and building local dataset. This can take a minute..."):
                try:
                    from scripts.ingest_clinicaltrials import run as ingest_run

                    ingest_run()
                    load_trials.clear()
                    st.rerun()
                except Exception as exc:
                    st.error(f"Dataset initialization failed: {exc}")
        return

    with st.container(key="header_banner_block"):
        header_title_col, mode_toggle_col = st.columns([8.05, 1.95], gap="small", vertical_alignment="center")
        with header_title_col:
            st.markdown("<h1 class='header-title-compact'>🧬 PDAC Trial Atlas</h1>", unsafe_allow_html=True)
        with mode_toggle_col:
            dark_mode_enabled = st.toggle(
                "Dark mode",
                value=theme_mode == "Dark",
                key="theme_mode_toggle_top",
            )
            requested_mode = "Dark" if dark_mode_enabled else "Normal"
            if st.session_state.get("theme_mode") != requested_mode:
                st.session_state["theme_mode"] = requested_mode
                st.rerun()

        st.markdown(
            "<div class='subtitle-strong'>Explore trials and analytics from the current filtered dataset.</div>",
            unsafe_allow_html=True,
        )
    filtered = apply_filters(df)

    tab1, tab2 = st.tabs(["Explorer", "Analytics"])

    with tab1:
        with st.container(key="metrics_banner_block"):
            metrics_row(df, filtered)
        render_explorer(filtered)
    with tab2:
        render_analytics(filtered)

if __name__ == "__main__":
    main()
