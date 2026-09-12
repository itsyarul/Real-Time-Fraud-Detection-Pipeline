import os
import sys

from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()

ML_ROOT = CURRENT_FILE.parents[1]

sys.path.insert(
    0,
    str(ML_ROOT),
)

from common.model_registry import (
    ModelRegistry,
)


def main():

    model_name = os.getenv(
        "ML_INFERENCE_MODEL_NAME",
        "fraud-logistic-regression",
    )

    version = os.getenv(
        "ML_INFERENCE_MODEL_VERSION",
        "1.0.1",
    )

    print("=" * 70)
    print("MODEL REGISTRY TEST")
    print("=" * 70)

    registry = ModelRegistry()

    print(
        f"Model: {model_name}"
    )

    print(
        f"Version: {version}"
    )

    model, metadata = registry.load(
        model_name,
        version,
    )

    registry.validate_metadata(
        metadata=metadata,
        expected_model_name=model_name,
        expected_version=version,
        expected_threshold=float(
            os.getenv(
                "ML_INFERENCE_THRESHOLD",
                "0.99",
            )
        ),
    )

    print()
    print("=" * 70)
    print("MODEL REGISTRY TEST PASSED")
    print("=" * 70)

    print(
        f"Model type: {type(model).__name__}"
    )

    print(
        f"Metadata model: "
        f"{metadata['model_name']}"
    )

    print(
        f"Metadata version: "
        f"{metadata['model_version']}"
    )

    print(
        f"Decision threshold: "
        f"{metadata['decision_threshold']}"
    )


if __name__ == "__main__":
    main()