import json
import os
import sys

import joblib
import numpy as np

from pyspark.sql import SparkSession


MODEL_NAME = os.getenv(
    "ML_MODEL_NAME",
    "fraud-logistic-regression",
)

MODEL_VERSION = os.getenv(
    "ML_MODEL_VERSION",
    "production",
)

MODEL_PATH = os.getenv(
    "ML_MODEL_PATH",
    "s3a://models/fraud-detection",
)

EXPECTED_THRESHOLD = float(
    os.getenv(
        "ML_DECISION_THRESHOLD",
        "0.99",
    )
)

EXPECTED_MIN_RECALL = float(
    os.getenv(
        "ML_MIN_RECALL",
        "0.85",
    )
)

FEATURE_COLUMNS = (
    [f"V{i}" for i in range(1, 29)]
    + ["Amount"]
)

MODEL_URI = (
    f"{MODEL_PATH}/"
    f"{MODEL_NAME}/"
    f"{MODEL_VERSION}"
)

LOCAL_MODEL_DIR = (
    "/tmp/model-verification"
)

LOCAL_MODEL_FILE = (
    f"{LOCAL_MODEL_DIR}/model.joblib"
)

LOCAL_METADATA_FILE = (
    f"{LOCAL_MODEL_DIR}/metadata.json"
)


def download_artifacts():

    os.makedirs(
        LOCAL_MODEL_DIR,
        exist_ok=True,
    )

    print(
        f"Downloading model from: "
        f"{MODEL_URI}"
    )

    spark = (
        SparkSession.builder
        .appName("VerifyModel")
        .getOrCreate()
    )

    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    s3_model_path = Path(f"{MODEL_URI}/model.joblib")
    s3_metadata_path = Path(f"{MODEL_URI}/metadata.json")
    fs = s3_model_path.getFileSystem(conf)

    fs.copyToLocalFile(
        False,
        s3_model_path,
        Path(f"file://{LOCAL_MODEL_DIR}/model.joblib"),
        True
    )

    fs.copyToLocalFile(
        False,
        s3_metadata_path,
        Path(f"file://{LOCAL_MODEL_DIR}/metadata.json"),
        True
    )

    spark.stop()


def verify_metadata():

    print("\n" + "=" * 70)
    print("VERIFYING MODEL METADATA")
    print("=" * 70)

    with open(
        LOCAL_METADATA_FILE,
        "r",
        encoding="utf-8",
    ) as file:

        metadata = json.load(file)

    assert (
        metadata["model_name"]
        == MODEL_NAME
    )

    if MODEL_VERSION != "production":
        assert (
            metadata["model_version"]
            == MODEL_VERSION
        ), f"Expected version {MODEL_VERSION}, got {metadata['model_version']}"

    actual_features = metadata[
        "features"
    ]

    assert (
        actual_features
        == FEATURE_COLUMNS
    ), (
        "Feature contract mismatch."
    )

    threshold = float(
        metadata[
            "decision_threshold"
        ]
    )

    assert (
        0.0 <= threshold <= 1.0
    ), (
        f"Invalid threshold: "
        f"{threshold}"
    )

    min_recall = float(
        metadata[
            "threshold_selection"
        ][
            "minimum_recall"
        ]
    )

    assert (
        min_recall
        == EXPECTED_MIN_RECALL
    )

    print(
        f"Model: "
        f"{metadata['model_name']}"
    )

    print(
        f"Version: "
        f"{metadata['model_version']}"
    )

    print(
        f"Features: "
        f"{len(actual_features)}"
    )

    print(
        f"Decision threshold: "
        f"{threshold}"
    )

    print(
        f"Minimum recall constraint: "
        f"{min_recall}"
    )

    print(
        "\nMetadata verification PASSED."
    )


def verify_model():

    print("\n" + "=" * 70)
    print("VERIFYING MODEL ARTIFACT")
    print("=" * 70)

    model = joblib.load(
        LOCAL_MODEL_FILE
    )

    # Create synthetic input with the exact
    # feature dimensionality.
    sample = np.zeros(
        (
            2,
            len(FEATURE_COLUMNS),
        ),
        dtype=np.float64,
    )

    probabilities = (
        model.predict_proba(
            sample
        )[:, 1]
    )

    predictions = (
        probabilities
        >= EXPECTED_THRESHOLD
    ).astype(int)

    print(
        f"Prediction probabilities: "
        f"{probabilities}"
    )

    print(
        f"Predictions: "
        f"{predictions}"
    )

    assert (
        len(probabilities) == 2
    )

    assert np.all(
        probabilities >= 0
    )

    assert np.all(
        probabilities <= 1
    )

    print(
        "\nModel inference verification PASSED."
    )


def main():

    print("=" * 70)
    print("FRAUD MODEL ARTIFACT VERIFICATION")
    print("=" * 70)

    try:

        download_artifacts()

        verify_metadata()

        verify_model()

        print("\n" + "=" * 70)
        print(
            "MODEL ARTIFACT VERIFICATION PASSED"
        )
        print("=" * 70)

    except Exception as exc:

        print(
            f"\nMODEL VERIFICATION FAILED: "
            f"{exc}"
        )

        sys.exit(1)


if __name__ == "__main__":
    main()