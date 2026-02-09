import os
import importlib.util
from pathlib import Path


def main():
    """
    Perform a lightweight Flask route smoke check for the quiz app.

    Args:
        None

    Returns:
        None
    """
    app_dir = Path(__file__).resolve().parents[1] / "app"
    os.chdir(app_dir)
    spec = importlib.util.spec_from_file_location("chemresqa_app", app_dir / "app.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    client = module.app.test_client()
    with client.session_transaction() as session_data:
        session_data["user_id"] = "smoke-test-user"
        session_data["question_indices"] = [0]
        session_data["current_index"] = 0
    response = client.get("/question")
    if response.status_code != 200:
        raise SystemExit(f"Smoke check failed with status {response.status_code}")
    print("smoke_ok")


if __name__ == "__main__":
    main()
