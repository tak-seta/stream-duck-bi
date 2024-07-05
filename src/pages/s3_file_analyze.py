import os

import streamlit as st
from infrastructure.duckdb import DuckDB
from infrastructure.s3_handler import S3Handler
from page_link import page_link


def connect_to_storage(storage: str, db: DuckDB) -> str:
    """Connect to the storage and returns the bucket name.

    Args:
    ----
        storage (str): The type of storage.
        db (DuckDB): The DuckDB instance.

    Returns:
    -------
        str: The bucket name.

    """
    if storage == "s3":
        bucket_name = st.text_input("Bucket Name", value="warehouse")
        region = st.text_input("Region", value="ap-northeast-1")
        db.connect_storage(storage, region)
    else:
        bucket_name = os.environ.get("MINIO_BUCKET")
        db.connect_storage(storage, None)
    return bucket_name


def s3_file_analize() -> None:
    """Display the contents of the selected file from the storage."""
    # サイドバーにページリンクを表示
    page_link()

    # データベース接続
    db = DuckDB()

    # Streamlitアプリの設定
    st.title("ストレージ内の分析")

    storage = st.selectbox("Storage", ["minio", "s3"])

    # 指定したストレージごとの設定と接続
    s3 = S3Handler(storage)
    bucket_name = connect_to_storage(storage, db)

    # バケット内のファイル一覧を取得
    files = s3.list_files(bucket_name)
    selected_file = st.selectbox("Select a file", files)

    # ファイルの内容を表示
    if selected_file:
        db.load_data_from_s3(bucket_name, selected_file)

    st.snow()


if __name__ == "__main__":
    s3_file_analize()
