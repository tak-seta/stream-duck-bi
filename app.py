import duckdb
import polars as pl
import streamlit as st

# Streamlitアプリの設定
st.title("DuckDBとStreamlitによるBIツール")

# ファイルアップロードセクション
uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv")

if uploaded_file is not None:
    table_name = uploaded_file.name.split(".")[0]
    st.write("作成されたテーブル名:", table_name)

    # CSVをPolars DataFrameに読み込む
    uploaded_data = pl.read_csv(uploaded_file)

    # DuckDBにテーブルを作成しデータをロードする
    con = duckdb.connect()

    # SQL injection対策のため、テーブル名をパラメータとして渡してクエリを実行
    table_create_query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM uploaded_data"  # noqa: S608

    con.execute(table_create_query)

    # データの表示
    st.write("アップロードされたデータ:")
    st.write(uploaded_data)

    # SQLクエリの入力
    query = st.text_area("DuckDB SQLクエリを入力してください")

    if query:
        try:
            result = con.execute(query).fetchdf()
            result_pl = pl.from_pandas(result)
            st.write("クエリ結果:")
            st.write(result_pl)

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
