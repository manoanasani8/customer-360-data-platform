"""Customer history, transaction ingestion, quality gates, and Parquet exports."""
import argparse
from collections import Counter
from datetime import datetime
from decimal import Decimal
import hashlib
import html
import json
import os
from pathlib import Path
import tempfile
import uuid

import duckdb

ROOT = Path(__file__).resolve().parent
CUSTOMER_FIELDS = {"event_id", "customer_id", "version", "effective_at", "segment", "region", "operation"}
TRANSACTION_FIELDS = {"transaction_id", "customer_id", "occurred_at", "amount_cents"}
REJECTIONS = {"invalid_customer", "event_id_conflict", "version_conflict", "invalid_effective_time",
              "invalid_transaction", "transaction_id_conflict", "no_active_customer_at_event_time"}


class QualityError(ValueError):
    """A batch failed its source controls or warehouse invariants."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def timestamp(value):
    """Require one UTC timestamp convention so ordering is unambiguous."""
    if not isinstance(value, str):
        raise ValueError("Timestamp must be text")
    result = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    if result.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ValueError("Timestamp must use YYYY-MM-DDTHH:MM:SSZ")
    return result


def valid_id(value):
    return isinstance(value, str) and 0 < len(value) <= 100 and value == value.strip()


def customer_event(db, event):
    if not isinstance(event, dict) or set(event) != CUSTOMER_FIELDS:
        return "invalid_customer"
    if (not all(valid_id(event[k]) for k in ("event_id", "customer_id"))
            or type(event["version"]) is not int or not 1 <= event["version"] < 2**63
            or event["segment"] not in ("retail", "business")
            or event["region"] not in ("IL", "NY", "CA", "TX")
            or event["operation"] not in ("upsert", "delete")):
        return "invalid_customer"
    try:
        effective = timestamp(event["effective_at"])
    except (ValueError, TypeError):
        return "invalid_customer"
    payload = canonical(event)
    prior = db.execute("SELECT payload FROM customer_events WHERE event_id = ?", [event["event_id"]]).fetchone()
    if prior:
        return "duplicate" if prior[0] == payload else "event_id_conflict"
    current = db.execute("""
        SELECT version, valid_from FROM customer_history
        WHERE customer_id = ? AND valid_to IS NULL
    """, [event["customer_id"]]).fetchone()
    if current and event["version"] == current[0]:
        return "version_conflict"
    if current and event["version"] < current[0]:
        db.execute("INSERT INTO customer_events VALUES (?, ?)", [event["event_id"], payload])
        return "stale"
    if current and effective <= current[1]:
        return "invalid_effective_time"
    db.execute("UPDATE customer_history SET valid_to = ? WHERE customer_id = ? AND valid_to IS NULL",
               [effective, event["customer_id"]])
    db.execute("INSERT INTO customer_history VALUES (?, ?, ?, ?, ?, NULL, ?)",
               [event["customer_id"], event["version"], event["segment"], event["region"],
                effective, event["operation"] == "delete"])
    db.execute("INSERT INTO customer_events VALUES (?, ?)", [event["event_id"], payload])
    return "deleted" if event["operation"] == "delete" else "applied"


def transaction_event(db, event):
    if not isinstance(event, dict) or set(event) != TRANSACTION_FIELDS:
        return "invalid_transaction"
    if (not all(valid_id(event[k]) for k in ("transaction_id", "customer_id"))
            or type(event["amount_cents"]) is not int or not 1 <= event["amount_cents"] < 2**63):
        return "invalid_transaction"
    try:
        occurred = timestamp(event["occurred_at"])
    except (ValueError, TypeError):
        return "invalid_transaction"
    payload = canonical(event)
    prior = db.execute("SELECT payload FROM transactions WHERE transaction_id = ?",
                       [event["transaction_id"]]).fetchone()
    if prior:
        return "duplicate" if prior[0] == payload else "transaction_id_conflict"
    active = db.execute("""
        SELECT COUNT(*) FROM customer_history
        WHERE customer_id = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
          AND NOT is_deleted
    """, [event["customer_id"], occurred, occurred]).fetchone()[0]
    if active != 1:
        return "no_active_customer_at_event_time"
    db.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?)",
               [event["transaction_id"], event["customer_id"], occurred, event["amount_cents"], payload])
    return "applied"


def invariant_counts(db):
    return {
        "multiple_current_versions": db.execute("""
            SELECT COUNT(*) FROM (
                SELECT customer_id FROM customer_history
                GROUP BY customer_id HAVING SUM(CASE WHEN valid_to IS NULL THEN 1 ELSE 0 END) != 1
            )
        """).fetchone()[0],
        "overlapping_history": db.execute("""
            SELECT COUNT(*) FROM customer_history a JOIN customer_history b
            ON a.customer_id = b.customer_id AND a.version < b.version
            AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
            AND (b.valid_to IS NULL OR b.valid_to > a.valid_from)
        """).fetchone()[0],
        "invalid_history_intervals": db.execute("""
            SELECT COUNT(*) FROM customer_history WHERE valid_to <= valid_from
        """).fetchone()[0],
        "unmatched_transactions": db.execute("""
            SELECT COUNT(*) FROM transactions t LEFT JOIN transaction_detail d USING(transaction_id)
            WHERE d.transaction_id IS NULL
        """).fetchone()[0],
        "duplicated_fact_joins": db.execute("""
            SELECT COUNT(*) FROM (
                SELECT transaction_id FROM transaction_detail GROUP BY transaction_id HAVING COUNT(*) != 1
            )
        """).fetchone()[0],
        "transaction_control_count_mismatch": db.execute("""
            SELECT ABS((SELECT COUNT(*) FROM transactions) - COALESCE((
                SELECT SUM(CAST(json_extract(controls, '$.new_transactions') AS BIGINT)) FROM batches
            ), 0))
        """).fetchone()[0],
        "transaction_control_amount_mismatch": db.execute("""
            SELECT ABS(COALESCE((SELECT SUM(amount_cents) FROM transactions), 0) - COALESCE((
                SELECT SUM(CAST(json_extract(controls, '$.new_amount_cents') AS BIGINT)) FROM batches
            ), 0))
        """).fetchone()[0],
    }


def load_batch(database, source):
    """Commit an immutable batch once, or roll back everything on failed controls."""
    batch = json.loads(Path(source).read_text(encoding="utf-8"))
    if (not isinstance(batch, dict) or set(batch) != {"batch_id", "customers", "transactions", "controls"}
            or not valid_id(batch["batch_id"]) or not isinstance(batch["customers"], list)
            or not isinstance(batch["transactions"], list)):
        raise ValueError("Invalid batch envelope")
    controls = batch["controls"]
    if (not isinstance(controls, dict) or set(controls) != {"new_transactions", "new_amount_cents", "rejected_rows"}
            or any(type(v) is not int or v < 0 for v in controls.values())):
        raise ValueError("Batch requires nonnegative integer source controls")
    fingerprint = hashlib.sha256(canonical(batch).encode()).hexdigest()
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as db:
        db.execute((ROOT / "schema.sql").read_text())
        db.execute((ROOT / "marts.sql").read_text())
        prior = db.execute("SELECT content_hash FROM batches WHERE batch_id = ?", [batch["batch_id"]]).fetchone()
        if prior:
            if prior[0] != fingerprint:
                raise ValueError("Batch ID already exists with different content")
            return {"batch_id": batch["batch_id"], "status": "already_loaded"}
        db.begin()
        decisions = Counter()
        accepted_count = 0
        accepted_amount = 0
        rejected = 0
        try:
            for source_name, handler in (("customers", customer_event), ("transactions", transaction_event)):
                for number, event in enumerate(batch[source_name], 1):
                    decision = handler(db, event)
                    decisions[f"{source_name}.{decision}"] += 1
                    db.execute("INSERT INTO outcomes VALUES (?, ?, ?, ?)",
                               [batch["batch_id"], source_name, number, decision])
                    if source_name == "transactions" and decision == "applied":
                        accepted_count += 1
                        accepted_amount += event["amount_cents"]
                    if decision in REJECTIONS:
                        rejected += 1
                        db.execute("INSERT INTO exceptions VALUES (?, ?, ?, ?, ?)",
                                   [batch["batch_id"], source_name, number, decision, canonical(event)])
            observed = {"new_transactions": accepted_count, "new_amount_cents": accepted_amount,
                        "rejected_rows": rejected}
            db.execute("INSERT INTO batches VALUES (?, ?, ?)",
                       [batch["batch_id"], fingerprint, canonical(controls)])
            failures = invariant_counts(db)
            if observed != controls or any(failures.values()):
                raise QualityError(canonical({"expected": controls, "observed": observed, "invariants": failures}))
            db.commit()
        except Exception:
            db.rollback()
            raise
    return {"batch_id": batch["batch_id"], "status": "loaded", "decisions": dict(decisions),
            "controls": observed}


def atomic_text(path, text):
    """Expose either the complete old file or the complete new file to readers."""
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, encoding="utf-8", delete=False) as file:
        temporary = Path(file.name)
        file.write(text)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def dashboard(metrics, records):
    cards = "".join(f'<article><strong>{html.escape(str(value))}</strong><span>{html.escape(label)}</span></article>'
                    for label, value in metrics.items())
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>" for row in records)
    return f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Customer 360 demo results</title><style>
body{{font:18px/1.5 system-ui,sans-serif;background:#f2f5f3;color:#173e34;max-width:1100px;margin:auto;padding:32px}}
h1{{font-size:44px;margin-bottom:6px}}p{{color:#4b635c}}section{{display:flex;gap:16px;flex-wrap:wrap;margin:30px 0}}
article{{background:white;padding:24px;border-radius:12px;flex:1;min-width:150px}}strong{{display:block;font-size:36px}}span{{font-size:16px}}
table{{border-collapse:collapse;background:white;width:100%}}td,th{{text-align:left;padding:14px;border-bottom:1px solid #dae3df}}
@media(max-width:640px){{body{{padding:16px}}h1{{font-size:32px}}table{{font-size:13px}}td,th{{padding:8px}}}}
</style><h1>Customer 360 warehouse</h1><p>Verified local results using synthetic banking data. Amounts are transaction volume in USD.</p>
<section>{cards}</section><h2>Activity using the customer segment at transaction time</h2><table>
<thead><tr><th>Date</th><th>Segment</th><th>Region</th><th>Transactions</th><th>Amount cents</th></tr></thead>
<tbody>{body}</tbody></table><p>Source tables, quality results and Parquet exports are included in the same snapshot folder.</p></html>'''


def publish(database, output):
    """Publish an immutable snapshot, then atomically point LATEST to it."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database), read_only=True) as db:
        db.begin()
        failures = invariant_counts(db)
        if any(failures.values()):
            raise QualityError(canonical(failures))
        versions = db.execute("SELECT batch_id, content_hash FROM batches ORDER BY batch_id").fetchall()
        snapshot = hashlib.sha256(canonical(versions).encode()).hexdigest()[:16]
        folder = output / snapshot
        folder.mkdir(exist_ok=True)
        if not (folder / "_SUCCESS.json").exists():
            for name in ("customer_history", "current_customers", "transaction_detail", "segment_activity", "exceptions", "outcomes"):
                # The relation names are a fixed allowlist; file paths are bound parameters.
                db.execute(f"COPY (SELECT * FROM {name}) TO ? (FORMAT PARQUET)", [str(folder / f"{name}.parquet")])
            rows = db.execute("SELECT * FROM segment_activity ORDER BY activity_date, segment, region").fetchall()
            db.execute("COPY (SELECT * FROM segment_activity ORDER BY activity_date, segment, region) TO ? (HEADER, DELIMITER ',')",
                       [str(folder / "segment_activity.csv")])
            active = db.execute("SELECT COUNT(*) FROM current_customers WHERE NOT is_deleted").fetchone()[0]
            transactions, amount = db.execute("SELECT COUNT(*), COALESCE(SUM(amount_cents),0) FROM transactions").fetchone()
            history = db.execute("SELECT COUNT(*) FROM customer_history").fetchone()[0]
            rejected = db.execute("SELECT COUNT(*) FROM exceptions").fetchone()[0]
            metrics = {"Active customers": active, "Customer versions": history,
                       "Transactions": transactions, "Volume USD": f"{Decimal(amount)/100:,.2f}", "Exceptions": rejected}
            atomic_text(folder / "dashboard.html", dashboard(metrics, rows))
            marker = {"snapshot": snapshot, "batches": [v[0] for v in versions], "metrics": metrics, "invariants": failures}
            atomic_text(folder / "_SUCCESS.json", json.dumps(marker, indent=2))
        db.commit()
    atomic_text(output / "LATEST", snapshot + "\n")
    return folder


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "publish", "run"))
    parser.add_argument("--batch", type=Path)
    parser.add_argument("--db", type=Path, default=Path("runs/warehouse.duckdb"))
    parser.add_argument("--output", type=Path, default=Path("runs/published"))
    args = parser.parse_args()
    if args.command in ("load", "run") and not args.batch:
        parser.error("--batch is required for load and run")
    try:
        if args.command in ("load", "run"):
            print(json.dumps(load_batch(args.db, args.batch), indent=2))
        if args.command in ("publish", "run"):
            print(f"Published snapshot: {publish(args.db, args.output)}")
    except (ValueError, duckdb.Error) as exc:
        args.output.mkdir(parents=True, exist_ok=True)
        report = args.output / f"failure-{uuid.uuid4().hex}.json"
        atomic_text(report, json.dumps({"error": str(exc), "batch": str(args.batch)}, indent=2))
        parser.exit(1, f"Batch blocked. Failure report: {report}\n{exc}\n")


if __name__ == "__main__":
    main()
