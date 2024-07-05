import streamlit as st


def page_link() -> None:
    """Display the page links in the sidebar."""
    with st.sidebar:
        st.page_link("home.py", label="ホーム", icon="🏠")
        st.page_link("pages/csv_upload.py", label="CSVアップロード", icon="📄")
        st.page_link("pages/s3_file_analyze.py", label="分析", icon="📊")
