import os

import duckdb
import polars as pl
import streamlit as st


class DuckDB:
    """Represents a DuckDB connection."""

    def __init__(self: "DuckDB") -> None:
        """Initialize the DuckDB connection."""
        self.conn = duckdb.connect()

        # DuckDBのHTTPFS拡張をロード
        self.conn.sql("INSTALL httpfs;")
        self.conn.sql("LOAD httpfs;")

    def connect_storage(self: "DuckDB", storage_type: str, region: str | None) -> None:
        """Connect to the storage.

        Args:
        ----
            storage_type (str): The type of storage.
            region (str | None): The region of the storage.

        """
        # S3への接続に必要な情報を入力
        if storage_type == "s3":
            # https://duckdb.org/docs/extensions/httpfs/s3api
            # REGIONとENDPOINTはセットで指定する必要がある
            self.conn.sql(
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
            # https://duckdb.org/docs/extensions/httpfs/s3api
            self.conn.sql(
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

    def load_uploaded_data(self: "DuckDB", uploaded_file: bytes) -> pl.DataFrame:
        """Load the uploaded data.

        Args:
        ----
            uploaded_file (bytes): The uploaded file.

        Returns:
        -------
            pl.DataFrame: The uploaded data.

        """
        return self.conn.read_csv(uploaded_file)

    def upload_data_to_s3(
        self: "DuckDB",
        bucket_name: str,
        file_name: str,
        data: pl.DataFrame,  # noqa: ARG002
    ) -> None:
        """Upload data to S3.

        Args:
        ----
            bucket_name (str): The name of the S3 bucket.
            file_name (str): The name of the file to be uploaded.
            data (pl.DataFrame): The data to be uploaded.

        Returns:
        -------
            None

        """
        self.conn.sql(f"COPY data TO 's3://{bucket_name}/{file_name}.parquet';")
        st.write("ファイルをS3にアップロードしました")
