import os

import duckdb
import polars as pl
import streamlit as st
from infrastructure.s3_handler import S3Handler

# データベース接続
con = duckdb.connect()

# DuckDBのHTTPFS拡張をロード
con.sql("INSTALL httpfs;")
con.sql("LOAD httpfs;")

# Streamlitアプリの設定
st.title("DuckDBとStreamlitによるBIツール")

storage = st.selectbox("Storage", ["minio", "s3"])

# S3への接続に必要な情報を入力
if storage == "s3":
    # S3への接続に必要な情報を入力
    bucket_name = st.text_input("Bucket Name", value="warehouse")
    region = st.text_input("Region", value="ap-northeast-1")
    con.sql(
        f"""
        CREATE SECRET aws (
            TYPE S3,
            KEY_ID '{os.environ.get("AWS_ACCESS_KEY_ID")}',
            SECRET '{os.environ.get("AWS_SECRET_ACCESS_KEY")}',
            REGION '{region}',
            ENDPOINT 's3.{region}.amazonaws.com'
        );
        """
    )
else:
    bucket_name = os.environ.get("MINIO_BUCKET")

    # https://duckdb.org/docs/extensions/httpfs/s3api
    con.sql(
        f"""
        CREATE SECRET minio (
            TYPE S3,
            KEY_ID {os.environ.get("MINIO_ACCESS_KEY_ID")},
            SECRET {os.environ.get("MINIO_SECRET_ACCESS_KEY")},
            ENDPOINT 'minio:9000',
            URL_STYLE vhost,
            USE_SSL false
        );
        """
    )

s3 = S3Handler(bucket_name=os.environ.get("S3_BUCKET"))

if "show_query_area" not in st.session_state:
    st.session_state["show_query_area"] = False

# ファイルアップロードセクション
uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv")

if uploaded_file is not None:
    # CSVをPolars DataFrameに読み込む
    uploaded_data = con.read_csv(uploaded_file)

    # アップロードされたデータの表示
    st.write("アップロードされたデータ:")
    st.dataframe(uploaded_data.pl(), hide_index=True)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("S3に保存", key="upload_data", use_container_width=True):
            # S3にファイルをアップロード
            file_name = uploaded_file.name.split(".")[0]
            con.sql(f"COPY uploaded_data TO 's3://{bucket_name}/{file_name}.parquet';")
            st.write("ファイルをS3にアップロードしました")

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
            result = con.execute(query).fetchdf()
            result_pl = pl.from_pandas(result)
            st.write("クエリ結果:")
            st.dataframe(result_pl, hide_index=True)

            col3, col4 = st.columns(2)

            with col3:
                if st.button("S3に保存", key="query_result", use_container_width=True):
                    # S3にファイルをアップロード
                    con.sql(f"COPY uploaded_data TO 's3://{bucket_name}/{table_name}.parquet';")
                    st.write("ファイルをS3にアップロードしました")

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
