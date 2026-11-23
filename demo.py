"""Reproduce customer changes, historical attribution, replay, and blocked publication."""
import argparse
from copy import deepcopy
import json
from pathlib import Path

import duckdb

from warehouse import QualityError, ROOT, load_batch, publish


def run(output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    database = output / "warehouse.duckdb"
    publication = output / "published"
    results = [load_batch(database, ROOT / filename) for filename in ("batch_001.json", "batch_002.json")]
    replay = load_batch(database, ROOT / "batch_002.json")
    assert replay["status"] == "already_loaded"
    snapshot = publish(database, publication)
    previous = (publication / "LATEST").read_text()
    bad = deepcopy(json.loads((ROOT / "batch_002.json").read_text()))
    bad["batch_id"] = "003-bad-controls"
    bad["customers"] = []
    bad["transactions"] = []
    bad["controls"] = {"new_transactions": 1, "new_amount_cents": 100, "rejected_rows": 0}
    failure_input = output / "bad_controls.json"
    failure_input.write_text(json.dumps(bad, indent=2), encoding="utf-8")
    try:
        load_batch(database, failure_input)
    except QualityError as exc:
        (output / "blocked_batch.json").write_text(str(exc), encoding="utf-8")
    else:
        raise AssertionError("Deliberately bad controls should block the batch")
    assert (publication / "LATEST").read_text() == previous
    with duckdb.connect(str(database), read_only=True) as db:
        assert db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 6
        assert db.execute("SELECT SUM(amount_cents) FROM transactions").fetchone()[0] == 97500
        assert db.execute("SELECT COUNT(*) FROM customer_history").fetchone()[0] == 6
        assert db.execute("SELECT COUNT(*) FROM exceptions").fetchone()[0] == 4
        assert db.execute("SELECT segment FROM transaction_detail WHERE transaction_id='t7'").fetchone()[0] == "retail"
    summary = {"batches": results, "replay": replay, "blocked_batch": "quality_control_mismatch",
               "metrics": json.loads((snapshot / "_SUCCESS.json").read_text())["metrics"]}
    (output / "demo_results.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Open the dashboard: {(snapshot / 'dashboard.html').resolve()}")
    return snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("demo-output"))
    run(parser.parse_args().output)
