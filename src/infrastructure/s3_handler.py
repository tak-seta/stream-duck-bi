import os

import boto3


class S3Handler:  # noqa: D101
    def __init__(self, bucket_name: str) -> None:  # noqa: ANN101
        """Initialize the S3Handler object.

        Args:
        ----
            bucket_name (str): The name of the S3 bucket.

        """
        session = boto3.session.Session(profile_name=os.environ.get("AWS_PROFILE"))
        self.s3 = session.client("s3")
        self.bucket_name = bucket_name
        self.region = session.region_name

    def upload_file(self, uploade_file_object: object, key: str) -> None:  # noqa: ANN101
        """Upload a file to the S3 bucket.

        Args:
        ----
            uploade_file_object (object): The file object to upload.
            key (str): The key to use for the uploaded file.

        Returns:
        -------
            None

        """
        self.s3.upload_fileobj(uploade_file_object, self.bucket_name, key)

    def list_files(self) -> list:  # noqa: ANN101
        """List all files in the S3 bucket.

        Returns
        -------
            list: A list of file names.

        """
        s3_files = self.s3.list_objects_v2(Bucket=self.bucket_name).get("Contents", [])

        return [file["Key"] for file in s3_files]
