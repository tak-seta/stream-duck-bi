import os  # noqa: INP001

import polars as pl
import streamlit as st
from infrastructure.duckdb import DuckDB


def connect_to_storage(storage: str, db: DuckDB) -> tuple:
    """Connect to the storage and returns the bucket name.

    Args:
    ----
        storage (str): The type of storage.
        db (DuckDB): The DuckDB instance.

    Returns:
    -------
        tuple: The bucket name.

    """
    if storage == "s3":
        bucket_name = st.text_input("Bucket Name", value="warehouse")
        region = st.text_input("Region", value="ap-northeast-1")
        db.connect_storage(storage, region)
    else:
        bucket_name = os.environ.get("MINIO_BUCKET")
        db.connect_storage(storage, None)
    return bucket_name


def upload_file_process(
    uploaded_data: pl.DataFrame, bucket_name: str, file_name: str, db: DuckDB
) -> None:
    """Process the uploaded file and save it to an S3 or Minio bucket.

    Display the uploaded data and allow the user to choose whether to save the data to S3 or create
    an SQL query.

    Args:
    ----
        uploaded_data (pl.DataFrame): The uploaded data as a polars DataFrame.
        bucket_name (str): The name of the bucket to save the data.
        file_name (str): The name of the file to save.
        db (DuckDB): The DuckDB instance.

    Returns:
    -------
        None

    """
    st.write("アップロードされたデータ:")
    st.dataframe(uploaded_data.pl(), hide_index=True)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("S3に保存", key="upload_data", use_container_width=True):
            db.upload_data_to_s3(bucket_name, file_name, uploaded_data)
    with col2:
        if st.button("SQLを作成", use_container_width=True):
            st.session_state["show_query_area"] = not st.session_state["show_query_area"]


def execute_query_process(
    uploaded_data: pl.DataFrame, table_name: str, bucket_name: str, db: DuckDB
) -> None:
    """Execute a SQL query and performs actions based on the query result.

    Args:
    ----
        uploaded_data (pl.DataFrame): The uploaded data as a polars DataFrame.
        table_name (str): The name of the table to create from the uploaded data.
        bucket_name (str): The name of the S3 bucket to upload the query result to.
        db (DuckDB): The DuckDB instance.

    Returns:
    -------
        None

    """
    uploaded_data.create(table_name)
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
                    db.upload_data_to_s3(bucket_name, table_name, result_pl)
            with col4:
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


def csv_upload() -> None:
    """Perform CSV upload ,save to storage and query execution."""
    # データベース接続
    db = DuckDB()

    # Streamlitアプリの設定
    st.title("CSVアップロードとクエリ実行")

    storage = st.selectbox("Storage", ["minio", "s3"])

    # 指定したストレージごとの設定と接続
    bucket_name = connect_to_storage(storage, db)

    if "show_query_area" not in st.session_state:
        st.session_state["show_query_area"] = False

    # ファイルアップロードセクション
    uploaded_file = st.file_uploader("CSVファイルをアップロードしてください", type="csv")
    if uploaded_file is not None:
        uploaded_data = db.load_uploaded_data(uploaded_file)
        file_name = uploaded_file.name.split(".")[0]
        upload_file_process(uploaded_data, bucket_name, file_name, db)

    # クエリ実行セクション
    if st.session_state["show_query_area"]:
        table_name = st.text_input(
            "SQLを作成する際のテーブル名を入力してください", key="table_name"
        )
        if not table_name:
            st.stop()
        execute_query_process(uploaded_data, table_name, bucket_name, db)


if __name__ == "__main__":
    csv_upload()
