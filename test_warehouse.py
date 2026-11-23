"""Regression coverage for temporal joins, duplicate handling, and atomic publication."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from demo import run
from warehouse import ROOT, QualityError, load_batch, publish


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.database = self.folder / "warehouse.duckdb"
        self.first = json.loads((ROOT / "batch_001.json").read_text())
        self.second = json.loads((ROOT / "batch_002.json").read_text())

    def load(self, batch):
        source = self.folder / "input.json"
        source.write_text(json.dumps(batch), encoding="utf-8")
        return load_batch(self.database, source)

    def query(self, sql):
        with duckdb.connect(str(self.database), read_only=True) as db:
            return db.execute(sql).fetchall()

    def test_historical_attribution_and_delete(self):
        self.load(self.first)
        self.load(self.second)
        self.assertEqual(self.query("SELECT transaction_id, segment FROM transaction_detail ORDER BY transaction_id"),
                         [('t1','retail'),('t2','retail'),('t3','business'),('t4','business'),('t5','retail'),('t7','retail')])
        self.assertEqual(self.query("SELECT customer_id FROM current_customers WHERE NOT is_deleted ORDER BY customer_id"),
                         [('c1',),('c2',)])
        self.assertEqual(self.query("SELECT COUNT(*), SUM(amount_cents) FROM transactions"), [(6,97500)])

    def test_exact_batch_replay_is_noop(self):
        self.load(self.first)
        self.assertEqual(self.load(self.first)["status"], "already_loaded")
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_history"), [(3,)])

    def test_changed_payload_cannot_reuse_batch_id(self):
        self.load(self.first)
        self.first["customers"][0]["region"] = "TX"
        with self.assertRaisesRegex(ValueError, "different content"):
            self.load(self.first)

    def test_quarantine_and_duplicate_outcomes(self):
        self.load(self.first)
        self.load(self.second)
        self.assertEqual(self.query("SELECT reason FROM exceptions ORDER BY reason"),
                         [('event_id_conflict',),('invalid_customer',),
                          ('no_active_customer_at_event_time',),('transaction_id_conflict',)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM outcomes WHERE decision='duplicate'"), [(2,)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM outcomes WHERE decision='stale'"), [(1,)])

    def test_bad_controls_roll_back_entire_batch(self):
        self.load(self.first)
        self.second["controls"]["new_amount_cents"] += 1
        with self.assertRaises(QualityError):
            self.load(self.second)
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_history"), [(3,)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM transactions"), [(3,)])
        self.assertEqual(self.query("SELECT COUNT(*) FROM exceptions"), [(0,)])
        self.second["controls"]["new_amount_cents"] -= 1
        self.assertEqual(self.load(self.second)["status"], "loaded")

    def test_timestamp_boundary_uses_new_version(self):
        self.load(self.first)
        self.second["transactions"][0]["occurred_at"] = "2026-01-03T00:00:00Z"
        self.load(self.second)
        self.assertEqual(self.query("SELECT customer_version FROM transaction_detail WHERE transaction_id='t4'"), [(2,)])

    def test_boolean_amount_is_quarantined(self):
        self.first["transactions"][0]["amount_cents"] = True
        self.first["controls"] = {"new_transactions":2,"new_amount_cents":55000,"rejected_rows":1}
        self.load(self.first)
        self.assertEqual(self.query("SELECT reason FROM exceptions"), [('invalid_transaction',)])

    def test_same_version_conflict_does_not_create_history(self):
        self.load(self.first)
        self.second["customers"][0]["version"] = 1
        # The update and the formerly stale version-1 event now conflict with version 1.
        self.second["controls"]["rejected_rows"] += 2
        self.load(self.second)
        self.assertEqual(self.query("SELECT COUNT(*) FROM customer_history WHERE customer_id='c1'"), [(1,)])

    def test_backdated_customer_update_is_rejected(self):
        self.load(self.first)
        self.second["customers"][0]["effective_at"] = "2025-12-31T00:00:00Z"
        self.second["controls"]["rejected_rows"] += 2
        self.load(self.second)
        self.assertIn(('invalid_effective_time',), self.query("SELECT reason FROM exceptions"))

    def test_published_parquet_matches_warehouse(self):
        self.load(self.first)
        self.load(self.second)
        folder = publish(self.database, self.folder / "published")
        with duckdb.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*), SUM(amount_cents) FROM read_parquet(?)",
                                        [str(folder / 'transaction_detail.parquet')]).fetchall(), [(6,97500)])
        self.assertEqual(publish(self.database, self.folder / "published"), folder)
        self.assertTrue((folder / "dashboard.html").exists())

    def test_corruption_blocks_publication(self):
        self.load(self.first)
        output = self.folder / "published"
        publish(self.database, output)
        before = (output / "LATEST").read_text()
        with duckdb.connect(str(self.database)) as db:
            db.execute("DELETE FROM customer_history WHERE customer_id='c1'")
        with self.assertRaises(QualityError):
            publish(self.database, output)
        self.assertEqual((output / "LATEST").read_text(), before)

    def test_amount_corruption_blocks_publication(self):
        self.load(self.first)
        output = self.folder / "published"
        publish(self.database, output)
        before = (output / "LATEST").read_text()
        with duckdb.connect(str(self.database)) as db:
            db.execute("UPDATE transactions SET amount_cents=amount_cents+1 WHERE transaction_id='t1'")
        with self.assertRaises(QualityError):
            publish(self.database, output)
        self.assertEqual((output / "LATEST").read_text(), before)

    def test_end_to_end_demo(self):
        folder = run(self.folder / "demo")
        metrics = json.loads((folder / "_SUCCESS.json").read_text())["metrics"]
        self.assertEqual(metrics["Volume USD"], "975.00")


if __name__ == "__main__":
    unittest.main()
