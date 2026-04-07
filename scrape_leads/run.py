#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests"]
# ///

import json
import sys
import os
import time
import requests

ACTOR_ID = "code_crafter~leads-finder"
APIFY_BASE = "https://api.apify.com/v2"


def parse_csv(value: str) -> list[str]:
    """Parse a comma-separated string into a trimmed list."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    with open(config_path) as f:
        return json.load(f)


def build_actor_input(params: dict) -> dict:
    actor_input = {}

    csv_array_fields = [
        "company_industry",
        "company_not_industry",
        "company_keywords",
        "company_not_keywords",
        "company_domain",
        "contact_job_title",
        "contact_not_job_title",
        "contact_location",
        "contact_not_location",
        "contact_city",
        "contact_not_city",
        "email_status",
        "seniority_level",
        "functional_level",
        "size",
        "funding",
    ]

    for key in csv_array_fields:
        if key in params and params[key]:
            actor_input[key] = parse_csv(params[key])

    if "fetch_count" in params:
        actor_input["fetch_count"] = int(params["fetch_count"])

    for key in ("min_revenue", "max_revenue", "file_name"):
        if key in params:
            actor_input[key] = params[key]

    return actor_input


def wait_for_run(run_id: str, token: str, poll_interval: int = 10, max_wait: int = 600) -> dict:
    """Poll until the actor run finishes or max_wait seconds elapse."""
    url = f"{APIFY_BASE}/actor-runs/{run_id}"
    deadline = time.time() + max_wait

    while time.time() < deadline:
        resp = requests.get(url, params={"token": token}, timeout=30)
        resp.raise_for_status()
        run = resp.json()["data"]
        status = run["status"]

        if status == "SUCCEEDED":
            return run
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Actor run ended with status: {status}")

        time.sleep(poll_interval)

    raise TimeoutError(f"Actor run {run_id} did not finish within {max_wait} seconds")


def fetch_dataset(dataset_id: str, token: str, limit: int) -> list:
    url = f"{APIFY_BASE}/datasets/{dataset_id}/items"
    resp = requests.get(
        url,
        params={"token": token, "limit": limit, "clean": "true"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    try:
        config = load_config()
    except FileNotFoundError:
        print(json.dumps({"error": "config.json not found — please configure apify_api_key"}))
        sys.exit(1)

    api_token = config.get("apify_api_key", "").strip()
    if not api_token:
        print(json.dumps({"error": "apify_api_key is missing or empty in config.json"}))
        sys.exit(1)

    params = json.load(sys.stdin)
    actor_input = build_actor_input(params)
    fetch_count = actor_input.get("fetch_count", 100000)

    run_url = f"{APIFY_BASE}/acts/{ACTOR_ID}/runs"
    try:
        resp = requests.post(
            run_url,
            params={"token": api_token},
            json=actor_input,
            timeout=60,
        )
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(json.dumps({"error": f"Failed to start actor run: {e}", "response": resp.text}))
        sys.exit(1)

    run_data = resp.json()["data"]
    run_id = run_data["id"]

    # Poll until complete
    try:
        finished_run = wait_for_run(run_id, api_token)
    except (RuntimeError, TimeoutError) as e:
        print(json.dumps({"error": str(e), "run_id": run_id}))
        sys.exit(1)

    dataset_id = finished_run["defaultDatasetId"]

    try:
        leads = fetch_dataset(dataset_id, api_token, fetch_count)
    except requests.HTTPError as e:
        print(json.dumps({"error": f"Failed to fetch results: {e}", "run_id": run_id}))
        sys.exit(1)

    print(json.dumps({
        "status": "success",
        "total_leads": len(leads),
        "run_id": run_id,
        "dataset_id": dataset_id,
        "leads": leads,
    }))


if __name__ == "__main__":
    main()
