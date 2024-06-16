import os

import boto3


class S3Handler:  # noqa: D101
    def __init__(self, bucket_name: str) -> None:  # noqa: ANN101
        """Initialize the S3Handler object.

        Args:
        ----
            bucket_name (str): The name of the S3 bucket.

        """
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            verify=False,
        )
        self.bucket_name = bucket_name

    def upload_file(self, upload_file_object: object, key: str) -> None:  # noqa: ANN101
        """Upload a file to the S3 bucket.

        Args:
        ----
            upload_file_object (object): The file object to upload.
            key (str): The key to use for the uploaded file.

        Returns:
        -------
            None

        """
        self.s3.upload_fileobj(upload_file_object, self.bucket_name, key)

    def list_files(self) -> list:  # noqa: ANN101
        """List all files in the S3 bucket.

        Returns
        -------
            list: A list of file names.

        """
        s3_files = self.s3.list_objects_v2(Bucket=self.bucket_name).get("Contents", [])

        return [file["Key"] for file in s3_files]
