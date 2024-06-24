import os

import duckdb


class DuckDB:
    """Represents a DuckDB connection."""

    def __init__(self) -> None:
        """Initialize the DuckDB connection."""
        self.conn = duckdb.connect()

        # DuckDBのHTTPFS拡張をロード
        self.conn.sql("INSTALL httpfs;")
        self.conn.sql("LOAD httpfs;")

    def connect_storage(self, storage_type: str, region: str | None) -> None:
        """Connect to the storage.

        Args:
        ----
            storage_type (str): The type of storage.
            bucket_name (str | None): The name of the bucket.
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
