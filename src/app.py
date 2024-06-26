import os

import polars as pl
import streamlit as st
from infrastructure.duckdb import DuckDB

# データベース接続
db = DuckDB()

# Streamlitアプリの設定
st.title("DuckDBとStreamlitによるBIツール")

storage = st.selectbox("Storage", ["minio", "s3"])

# 指定したストレージごとの設定と接続
if storage == "s3":
    # S3への接続に必要な情報を入力
    bucket_name = st.text_input("Bucket Name", value="warehouse")
    region = st.text_input("Region", value="ap-northeast-1")
    #  S3への接続
    db.connect_storage(storage, region)

else:
    bucket_name = os.environ.get("MINIO_BUCKET")
    db.connect_storage(storage, None)

if "show_query_area" not in st.session_state:
    st.session_state["show_query_area"] = False

# ファイルアップロードセクション
uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv")

if uploaded_file is not None:
    # CSVをDataFrameに読み込む
    uploaded_data = db.load_uploaded_data(uploaded_file)

    # アップロードされたデータの表示
    st.write("アップロードされたデータ:")
    st.dataframe(uploaded_data.pl(), hide_index=True)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("S3に保存", key="upload_data", use_container_width=True):
            # S3にファイルをアップロード
            file_name = uploaded_file.name.split(".")[0]
            db.upload_data_to_s3(bucket_name, file_name, uploaded_data)

    with col2:
        if st.button("SQLを作成", use_container_width=True):
            st.session_state["show_query_area"] = not st.session_state["show_query_area"]

if st.session_state["show_query_area"]:
    table_name = st.text_input("SQLを作成する際のテーブル名を入力してください")
    if not table_name:
        st.stop()

    uploaded_data.create(table_name)

    # SQLクエリの入力
    query = st.text_area("SQLクエリを入力してください")

    if query:
        try:
            result = db.conn.execute(query).fetchdf()
            result_pl = pl.from_pandas(result)
            st.write("クエリ結果:")
            st.dataframe(result_pl, hide_index=True)

            col3, col4 = st.columns(2)

            with col3:
                if st.button("S3に保存", key="query_result", use_container_width=True):
                    # S3にファイルをアップロード
                    db.upload_data_to_s3(bucket_name, table_name, result_pl)

            with col4:
                # CSVダウンロードリンクの生成
                csv = result_pl.write_csv()
                st.download_button(
                    label="CSVファイルとしてダウンロード",
                    data=csv,
                    file_name="query_result.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception as e:
            st.error(f"クエリの実行中にエラーが発生しました: {e}")
