import json
import os
import shutil
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import joblib


class ModelRegistryError(Exception):
    """Raised when model registry operations fail."""


class ModelRegistry:
    """
    Handles model artifacts stored in the MinIO/S3A model registry.

    Expected structure:

    s3a://<base_path>/<model_name>/<version>/
        model.joblib
        metadata.json
    """

    def __init__(
        self,
        model_base_path: str | None = None,
        local_base_path: str = "/tmp/model-registry",
    ):
        self.model_base_path = (
            model_base_path
            or os.getenv(
                "ML_MODEL_PATH",
                "s3a://models/fraud-detection",
            )
        ).rstrip("/")

        self.local_base_path = Path(
            local_base_path
        )

    def get_model_uri(
        self,
        model_name: str,
        version: str,
    ) -> str:

        return (
            f"{self.model_base_path}/"
            f"{model_name}/"
            f"{version}"
        )

    def get_local_model_dir(
        self,
        model_name: str,
        version: str,
    ) -> Path:

        safe_name = (
            f"{model_name}_{version}"
            .replace("/", "_")
            .replace(":", "_")
        )

        return (
            self.local_base_path
            / safe_name
        )

    def download_artifacts(
        self,
        model_name: str,
        version: str,
    ) -> Path:

        model_uri = self.get_model_uri(
            model_name,
            version,
        )

        local_dir = self.get_local_model_dir(
            model_name,
            version,
        )

        if local_dir.exists():
            shutil.rmtree(local_dir)

        local_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        print(
            f"Downloading model from: "
            f"{model_uri}"
        )

        clean_uri = model_uri.replace("s3a://", "").replace("s3://", "")
        if "/" not in clean_uri:
            raise ModelRegistryError(
                f"Invalid model URI format: {model_uri}"
            )

        bucket_name, prefix = clean_uri.split("/", 1)
        prefix = prefix.strip("/")

        endpoint_url = (
            os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("MINIO_S3_ENDPOINT")
            or "http://minio:9000"
        )
        aws_access_key = os.getenv(
            "AWS_ACCESS_KEY_ID",
            "minio",
        )
        aws_secret_key = os.getenv(
            "AWS_SECRET_ACCESS_KEY",
            "MinioAdmin2026",
        )
        aws_region = os.getenv(
            "AWS_REGION",
            "us-east-1",
        )

        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region,
            )

            for filename in [
                "model.joblib",
                "metadata.json",
            ]:
                s3_key = f"{prefix}/{filename}"
                dest_file = local_dir / filename
                s3_client.download_file(
                    bucket_name,
                    s3_key,
                    str(dest_file),
                )

        except (BotoCoreError, ClientError, Exception) as exc:
            raise ModelRegistryError(
                f"Failed to download model artifacts from {model_uri}: {exc}"
            ) from exc

        if (
            not (local_dir / "model.joblib").exists()
            or not (local_dir / "metadata.json").exists()
        ):
            raise ModelRegistryError(
                "Downloaded model artifacts are incomplete. "
                "Expected model.joblib and metadata.json."
            )

        print(
            f"Model artifacts downloaded to: "
            f"{local_dir}"
        )

        return local_dir

    def load_metadata(
        self,
        artifact_dir: Path,
    ) -> dict:

        metadata_path = (
            artifact_dir / "metadata.json"
        )

        if not metadata_path.exists():
            raise ModelRegistryError(
                f"Missing metadata: "
                f"{metadata_path}"
            )

        with open(
            metadata_path,
            "r",
            encoding="utf-8",
        ) as file:

            metadata = json.load(file)

        return metadata

    def validate_metadata(
        self,
        metadata: dict,
        expected_model_name: str,
        expected_version: str,
        expected_threshold: float | None = None,
    ):

        if metadata.get(
            "model_name"
        ) != expected_model_name:

            raise ModelRegistryError(
                "Model name mismatch: "
                f"{metadata.get('model_name')} != "
                f"{expected_model_name}"
            )

        if expected_version != "production":
            if metadata.get(
                "model_version"
            ) != expected_version:
    
                raise ModelRegistryError(
                    "Model version mismatch: "
                    f"{metadata.get('model_version')} != "
                    f"{expected_version}"
                )

        if expected_threshold is not None:

            actual_threshold = float(
                metadata[
                    "decision_threshold"
                ]
            )

            if (
                actual_threshold
                != expected_threshold
            ):

                raise ModelRegistryError(
                    "Decision threshold mismatch: "
                    f"{actual_threshold} != "
                    f"{expected_threshold}"
                )

        expected_features = (
            [f"V{i}" for i in range(1, 29)]
            + ["Amount"]
        )

        actual_features = metadata.get(
            "features"
        )

        if actual_features != expected_features:

            raise ModelRegistryError(
                "Feature contract mismatch.\n"
                f"Expected: {expected_features}\n"
                f"Actual: {actual_features}"
            )

        print(
            "Model metadata validation PASSED."
        )

    def load_model(
        self,
        artifact_dir: Path,
    ):

        model_path = (
            artifact_dir / "model.joblib"
        )

        if not model_path.exists():
            raise ModelRegistryError(
                f"Missing model artifact: "
                f"{model_path}"
            )

        print(
            f"Loading model: {model_path}"
        )

        return joblib.load(
            model_path
        )

    def load(
        self,
        model_name: str,
        version: str,
    ):

        artifact_dir = self.download_artifacts(
            model_name,
            version,
        )

        metadata = self.load_metadata(
            artifact_dir
        )

        model = self.load_model(
            artifact_dir
        )

        return model, metadata