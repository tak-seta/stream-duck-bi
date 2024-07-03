import streamlit as st  # noqa: N999


def home() -> None:
    """Display the home page of the Streamlit app."""
    # Streamlitアプリの設定
    st.title("DuckDBとStreamlitによるBIツール")


if __name__ == "__main__":
    home()
