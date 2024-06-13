import os

import duckdb
import polars as pl
import streamlit as st
from infrastructure.s3_handler import S3Handler

s3 = S3Handler(bucket_name=os.environ.get("S3_BUCKET"))

# Streamlitアプリの設定
st.title("DuckDBとStreamlitによるBIツール")

# ファイルアップロードセクション
uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv")

if uploaded_file is not None:
    # CSVをPolars DataFrameに読み込む
    uploaded_data = pl.read_csv(uploaded_file)

    # アップロードされたデータの表示
    st.write("アップロードされたデータ:")
    st.dataframe(uploaded_data, hide_index=True)

    if st.button("S3に保存"):
        # S3にファイルをアップロード
        s3.upload_file(upload_file_object=uploaded_file, key=uploaded_file.name)
        st.write("ファイルをS3にアップロードしました")

    # データベース接続
    con = duckdb.connect()

    table_name = st.text_input("テーブル名を入力してください")
    if not table_name:
        st.stop()

    table_create_query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM uploaded_data"  # noqa: S608
    con.execute(table_create_query)

    # SQLクエリの入力
    query = st.text_area("DuckDB SQLクエリを入力してください")

    if query:
        try:
            result = con.execute(query).fetchdf()
            result_pl = pl.from_pandas(result)
            st.write("クエリ結果:")
            st.dataframe(result_pl, hide_index=True)

            # CSVダウンロードリンクの生成
            csv = result_pl.write_csv()
            st.download_button(
                label="CSVファイルとしてダウンロード",
                data=csv,
                file_name="query_result.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.error(f"クエリの実行中にエラーが発生しました: {e}")
