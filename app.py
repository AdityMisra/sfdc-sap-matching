import streamlit as st
import pandas as pd
from pipelines import run_top_vs_sfdc, run_sfdc_vs_sap

st.set_page_config(page_title="Matcher App", layout="wide")
st.title("🔗 Configurable Top → SFDC → SAP Matcher")

# File uploads
top_file  = st.file_uploader("Upload TOP file (xlsx)", type=["xlsx"])
sfdc_file = st.file_uploader("Upload SFDC file (xlsx)", type=["xlsx"])
sap_file  = st.file_uploader("Upload SAP file (xlsx)", type=["xlsx"])

# Threshold inputs
threshold_top_sf = st.slider("Threshold for Top→SFDC", 0.0, 1.0, 0.85)
threshold_sf_sap = st.slider("Threshold for SFDC→SAP", 0.0, 1.0, 0.85)

# Column mappings
with st.expander("TOP → SFDC column settings"):
    header_row = st.number_input("Header row index for TOP file", value=2, min_value=0)
    top_col    = st.text_input("TOP End-Customer column name", "End Customer")
    sfdc_cols = {
        'id':      st.text_input("SFDC Account ID column", "Account ID"),
        'name':    st.text_input("SFDC Account Name column", "Account Name"),
        'website': st.text_input("SFDC Website column", "Website"),
        'street':  st.text_input("SFDC Billing Street column", "Billing Street"),
        'state':   st.text_input("SFDC Billing State/Province column", "Billing State/Province"),
        'parent':  st.text_input("SFDC Parent Account column", "Parent Account")
    }

with st.expander("SAP column settings"):
    sap_cols = {
        'customer': st.text_input("SAP Customer column", "Customer"),
        'name1':    st.text_input("SAP Name1 column", "Name 1"),
        'name2':    st.text_input("SAP Name2 column", "Name 2"),
        'street':   st.text_input("SAP Street column", "Street"),
        'city':     st.text_input("SAP City column", "City"),
        'region':   st.text_input("SAP Region column", "Rg"),
        'postal':   st.text_input("SAP PostalCode column", "PostalCode")
    }

# Run matching
if st.button("Run Matching"):
    if not (top_file and sfdc_file and sap_file):
        st.error("Please upload all three Excel files.")
    else:
        with st.spinner("Running Top→SFDC matching…"):
            sfdf = run_top_vs_sfdc(
                top_file,
                sfdc_file,
                threshold_top_sf=threshold_top_sf,
                header_row=header_row,
                top_col=top_col,
                sfdc_cols=sfdc_cols
            )
        st.success(f"Found {len(sfdf)} potential SFDC matches.")
        st.dataframe(sfdf)

        with st.spinner("Running SFDC→SAP matching…"):
            auto_df, manual_df = run_sfdc_vs_sap(
                sfdf,
                sap_file,
                threshold_sf_sap=threshold_sf_sap,
                sap_cols=sap_cols
            )
        st.success(f"Auto-matched: {len(auto_df)}, manual review needed: {len(manual_df)}.")

        st.subheader("✅ Auto-matched accounts")
        st.dataframe(auto_df)
        st.download_button("Download Auto CSV", data=auto_df.to_csv(index=False), file_name="auto_matches.csv")

        st.subheader("⚠️ Manual review required")
        st.dataframe(manual_df)
        st.download_button("Download Manual CSV", data=manual_df.to_csv(index=False), file_name="manual_review.csv")