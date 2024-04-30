from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, IO, TypedDict

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client  # Stubs for boto3.

    from data.settings import Settings


class S3Object(TypedDict):
    key: str
    last_modified: datetime.datetime


class S3Service:
    """
    Service for interacting with AWS S3.
    """
    _client: S3Client
    _bucket_name: str

    def __init__(self, settings: Settings) -> None:
        self.client = boto3.client(
            service_name="s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
        self._bucket_name = settings.aws_s3_bucket_name

    def upload_file_obj(self, file_obj: IO, object_name: str, tag: str | None = None) -> None:
        """
        Uploads file to S3 bucket.
        :param file_obj: File object to upload.
        :param object_name: Name of the object in S3 bucket.
        :param tag: Tag to add to the object.
        :return: None.
        """
        extra_args = None
        if tag:
            extra_args = {"Tagging": tag}

        self.client.upload_fileobj(file_obj, self._bucket_name, object_name, ExtraArgs=extra_args)

    def download_file_obj(self, file_obj: IO, object_name: str) -> None:
        """
        Downloads file from S3 bucket.
        :param file_obj: File object to download to.
        :param object_name: Name of the object in S3 bucket.
        :return: None.
        """
        self.client.download_fileobj(self._bucket_name, object_name, file_obj)

    def list_files_in_dir(self, dir_name: str) -> list[S3Object]:
        """
        Lists files in directory in S3 bucket.
        :param dir_name: Name of the directory.
        :return: List of files in directory.
        """
        response = self.client.list_objects_v2(Bucket=self._bucket_name, Prefix=dir_name)
        return [
            {
                "key": content["Key"],
                "last_modified": content["LastModified"]
            }
            for content in response["Contents"]
        ]

    def check_if_file_exists(self, object_name: str) -> bool:
        """
        Checks if file exists in S3 bucket.
        :param object_name: Name of the object to check.
        :return: True if file exists, False otherwise.
        """
        # noinspection PyBroadException
        try:
            self.client.head_object(Bucket=self._bucket_name, Key=object_name)
        except Exception:  # TODO: specify exception
            return False
        else:
            return True
