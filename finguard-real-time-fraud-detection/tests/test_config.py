import os
from pathlib import Path


def test_env_example_contains_required_keys():
    p = Path(__file__).parents[1] / "producer" / ".env.example"
    text = p.read_text(encoding="utf-8")
    for key in ["BOOTSTRAP_SERVERS", "API_KEY", "API_SECRET", "TOPIC_NAME"]:
        assert f"{key}=" in text
