import os

import boto3


class S3Handler:  # noqa: D101
    def __init__(self, storage_type: str) -> None:  # noqa: ANN101
        """Initialize the S3Handler object."""
        # 環境(s3 or minio)に応じて、clientを生成
        if storage_type == "s3":
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            )
        else:
            self.s3 = boto3.client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("MINIO_SECRET_ACCESS_KEY"),
                verify=False,
            )

    def list_files(self, bucket_name) -> list:  # noqa: ANN101
        """List all files in the S3 bucket.

        Returns
        -------
            list: A list of file names.

        """
        s3_files = self.s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])

        return [file["Key"] for file in s3_files]
