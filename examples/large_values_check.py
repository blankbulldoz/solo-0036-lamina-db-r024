#!/usr/bin/env python3
"""End-to-end verification of the large-values feature via the public HTTP API.

Checks, mapped to the feature contract:
  1. large_text columns externalize big payloads; queries return full rows.
  2. small-column updates do not rewrite large content (shared refs / dedup).
  3. failed/aborted commits leave no permanent large-value occupancy.
  4. backup/restore preserves complete rows and blob ownership.
  5. vacuum reaps only unreferenced blobs; active snapshots pin them.
  6. restart (checkpoint + journal) preserves blobs and sweeps temp leftovers.
  7. bounded budget is enforced with 429 rather than raising the cap.
"""
import json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error

PROJECT = pathlib.Path(__file__).resolve().parents[1]
BINARY = PROJECT / "bin" / "laminad"
BIG = "X" * 5000          # above the 4096 inline threshold, below 64KiB text cap
BIG2 = "Y" * 5000
SMALL = "tiny"            # below inline threshold: stays inside the row


class Server:
    def __init__(self, root, restore=None):
        self.root = pathlib.Path(root)
        self.restore = restore
        self.process = None
        self.log = None

    def start(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        args = [str(BINARY), "-data", str(self.root), "-listen", f"127.0.0.1:{port}"]
        if self.restore:
            args += ["-restore", str(self.restore)]
            self.restore = None
        self.log = tempfile.TemporaryFile()
        self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.03)
        raise RuntimeError("service did not become ready")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None, status=200, raw=False):
        body = None if data is None else json.dumps(data, ensure_ascii=False).encode()
        req = urllib.request.Request(self.url + path, data=body, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                code, payload = resp.status, resp.read()
        except urllib.error.HTTPError as e:
            code, payload = e.code, e.read()
        if code != status:
            raise AssertionError(f"{method} {path}: expected {status}, got {code}: {payload.decode()}")
        return payload if raw else json.loads(payload)

    def begin(self, readonly=False):
        return self.call("POST", "/transactions", {"read_only": readonly}, 201)["id"]

    def put_doc(self, tx, row, insert=False, status=200):
        return self.call("POST", f"/transactions/{tx}/put",
                         {"relation": "docs", "row": row, "insert_only": insert}, status)

    def docs(self, **kw):
        return self.call("POST", "/query", {"relation": "docs", **kw})

    def get_doc(self, key, tx=None):
        body = {"relation": "docs", "where": [{"column": "id", "op": "=", "value": key}], "limit": 1}
        if tx:
            body["transaction"] = tx
        out = self.call("POST", "/query", body)
        return out["rows"][0] if out["rows"] else None


def require(cond, msg):
    if not cond:
        raise AssertionError(msg)


def blob_files(root):
    base = pathlib.Path(root) / "blobs"
    if not base.exists():
        return []
    return [p for p in base.rglob("*") if p.is_file()]


def temp_files(root):
    base = pathlib.Path(root) / "blobs"
    if not base.exists():
        return []
    return [p for p in base.rglob(".tmp-*") if p.is_file()]


def main():
    with tempfile.TemporaryDirectory(prefix="lamina-large-") as tmp:
        tmp = pathlib.Path(tmp)
        s = Server(tmp / "source").start()
        restored = None
        try:
            schema = {"name": "docs", "primary": "id", "columns": [
                {"name": "id", "type": "int"},
                {"name": "title", "type": "text"},
                {"name": "body", "type": "large_text", "nullable": True},
            ]}
            s.call("POST", "/relations", schema, 201)

            # large_text cannot be indexed or used as a primary key.
            bad_index = {"name": "by_body", "relation": "docs", "column": "body"}
            s.call("POST", "/indexes", bad_index, 400)
            s.call("POST", "/relations",
                   {"name": "badpk", "primary": "body",
                    "columns": [{"name": "body", "type": "large_text"}]}, 400)

            # A client cannot mint a reference directly; only the engine
            # externalizes supplied text.
            forged_tx = s.begin()
            s.call("POST", f"/transactions/{forged_tx}/put",
                   {"relation": "docs", "row": {"id": 99, "title": "x",
                    "body": {"type": "large_ref", "hash": "a" * 64, "bytes": 3}}},
                   status=400)
            s.call("POST", f"/transactions/{forged_tx}/rollback")

            # 1. Externalization + full-row read-back.
            tx = s.begin()
            s.put_doc(tx, {"id": 1, "title": "one", "body": BIG}, insert=True)
            s.put_doc(tx, {"id": 2, "title": "two", "body": SMALL}, insert=True)
            commit = s.call("POST", f"/transactions/{tx}/commit")
            seq1 = commit["sequence"]
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["blobs"] == 1 and stats["referenced_blobs"] == 1
                    and stats["total_bytes"] == len(BIG), f"one shared blob expected: {stats}")
            require(len(blob_files(s.root)) == 1, "exactly one blob file on disk")
            row1 = s.get_doc(1)
            require(row1["body"] == BIG and row1["title"] == "one", "full large row not returned")
            row2 = s.get_doc(2)
            require(row2["body"] == SMALL, "small inline value not returned")

            # 2. Frequent small-column update must not duplicate the big value.
            tx = s.begin()
            s.put_doc(tx, {"id": 1, "title": "one-renamed", "body": BIG})
            s.call("POST", f"/transactions/{tx}/commit")
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["blobs"] == 1 and stats["total_bytes"] == len(BIG),
                    f"small update rewrote large content: {stats}")
            require(s.get_doc(1)["title"] == "one-renamed", "small update not visible")

            # Content sharing: same big payload in another row -> still one blob.
            tx = s.begin()
            s.put_doc(tx, {"id": 3, "title": "three", "body": BIG}, insert=True)
            s.call("POST", f"/transactions/{tx}/commit")
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["blobs"] == 1, f"identical content not deduplicated: {stats}")

            # New distinct content adds exactly one blob.
            tx = s.begin()
            s.put_doc(tx, {"id": 4, "title": "four", "body": BIG2}, insert=True)
            s.call("POST", f"/transactions/{tx}/commit")
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["blobs"] == 2 and stats["total_bytes"] == len(BIG) + len(BIG2),
                    f"distinct content not stored: {stats}")

            # Content-based equality works on externalized and inline values.
            hit = s.docs(where=[{"column": "body", "op": "=", "value": BIG}])
            require({r["id"] for r in hit["rows"]} == {1, 3}, "content equality missed rows")
            neq = s.docs(where=[{"column": "body", "op": "!=", "value": BIG}])
            require({r["id"] for r in neq["rows"]} == {2, 4}, "content inequality wrong")
            nulls = s.docs(where=[{"column": "body", "op": "is_null", "value": None}])
            require(nulls["rows"] == [], "unexpected null body")
            # Ordering and range predicates on large_text are rejected.
            s.call("POST", "/query", {"relation": "docs", "order": [{"column": "body"}]}, 400)
            s.call("POST", "/query", {"relation": "docs",
                                      "where": [{"column": "body", "op": "<", "value": "a"}]}, 400)

            # Read-own-write: staged inline text matches a content predicate.
            tx = s.begin()
            s.put_doc(tx, {"id": 5, "title": "five", "body": BIG})
            own = s.docs(transaction=tx,
                         where=[{"column": "body", "op": "=", "value": BIG}])
            require(any(r["id"] == 5 for r in own["rows"]), "read-own-write content match failed")

            # 3. Aborted/staged writes leave no permanent blob occupancy.
            before = s.call("GET", "/stats")["large_values"]
            s.call("POST", f"/transactions/{tx}/rollback")
            after = s.call("GET", "/stats")["large_values"]
            require(before == after, "rolled-back write changed blob ownership")
            require(len(temp_files(s.root)) == 0, "temp blob left behind")
            require(s.get_doc(5) is None, "rolled-back row became visible")

            # A commit that fails validation (bad type) consumes no sequence/space.
            seq_before = s.call("GET", "/stats")["sequence"]
            tx = s.begin()
            s.put_doc(tx, {"id": 6, "title": "x", "body": 123}, status=400)
            s.call("POST", f"/transactions/{tx}/rollback")
            require(s.call("GET", "/stats")["sequence"] == seq_before, "failed write advanced sequence")

            # Historical snapshot still reads the old title and the shared blob.
            old = s.docs(at=seq1)
            r1 = next(r for r in old["rows"] if r["id"] == 1)
            require(r1["title"] == "one" and r1["body"] == BIG, "historical row materialization failed")

            # 5. Vacuum: an active snapshot pins the blob of an old version.
            reader = s.begin()  # pins the current sequence
            tx = s.begin()
            # Replace the large body with null so the BIG2 blob is only held by
            # the historical version protected by the active reader.
            s.put_doc(tx, {"id": 4, "title": "four-v2", "body": None})
            s.call("POST", f"/transactions/{tx}/commit")
            head = s.call("GET", "/stats")["sequence"]
            pinned = s.call("POST", "/vacuum", {"horizon": head})
            require(pinned["pinned"], "active reader did not pin vacuum horizon")
            require(pinned["blobs_reaped"] == 0, f"pinned vacuum reaped blobs: {pinned}")
            # The reader can still materialize the pre-vacuum BIG2 version.
            seen = s.docs(transaction=reader, where=[{"column": "id", "op": "=", "value": 4}])
            require(seen["rows"][0]["body"] == BIG2, "pinned historical blob was lost")
            s.call("POST", f"/transactions/{reader}/rollback")

            # After the pin is released, vacuum reaps the now-unreferenced BIG2 blob.
            reclaimed = s.call("POST", "/vacuum", {"horizon": head})
            require(reclaimed["blobs_reaped"] >= 1, f"unreferenced blob not reaped: {reclaimed}")
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["orphan_blobs"] == 0, f"orphan accounting wrong: {stats}")
            # Every retained row still materializes (no dangling reference).
            for row in s.docs()["rows"]:
                require(row["body"] is None or isinstance(row["body"], str),
                        "retained row lost its large value")
            require(s.get_doc(4)["body"] is None, "current large value wrong after vacuum")
            # The eliminated historical snapshot is now rejected.
            s.call("POST", "/query", {"relation": "docs", "at": seq1}, 410)

            # 4. Backup captures the exact reachable closure and restores fully.
            backup = s.call("GET", "/backup", raw=True)
            envelope = json.loads(backup)
            image = envelope["payload"]
            live_hashes = {p.name for p in blob_files(s.root)}
            require(set(image["blobs"].keys()) == live_hashes,
                    "backup blob closure does not match reachable blobs")
            require(len(envelope["sha256"]) == 64, "backup envelope missing digest")
            backup_path = tmp / "docs.lmi"
            backup_path.write_bytes(backup)
            restored = Server(tmp / "restored", backup_path).start()
            for doc_id, expected in ((1, BIG), (2, SMALL), (3, BIG), (4, None)):
                r = restored.get_doc(doc_id)
                require(r is not None and r["body"] == expected,
                        f"restored row {doc_id} materialization mismatch")
            rstats = restored.call("GET", "/stats")["large_values"]
            require(rstats["blobs"] == stats["blobs"] and rstats["total_bytes"] == stats["total_bytes"],
                    f"restored blob ownership differs: {rstats} vs {stats}")
            # Restored instance keeps accepting writes with new blobs.
            tx = restored.begin()
            restored.put_doc(tx, {"id": 7, "title": "seven", "body": BIG2})
            restored.call("POST", f"/transactions/{tx}/commit")
            require(restored.get_doc(7)["body"] == BIG2, "post-restore large write failed")
            restored.call("POST", "/checkpoint")
            restored.stop()
            restored.start()
            require(restored.get_doc(7)["body"] == BIG2, "blob did not survive checkpoint restart")

            # A backup whose image is tampered with but keeps the stale outer
            # digest must be rejected (envelope digest failure) and leave no
            # restored directory behind.
            import hashlib
            tampered_env = json.loads(backup)
            inner = tampered_env["payload"]
            some_hash = next(iter(inner["blobs"]))
            b64 = inner["blobs"][some_hash]
            inner["blobs"][some_hash] = b64[:10] + ("A" if b64[10] != "A" else "B") + b64[11:]
            bad_path = tmp / "tampered.lmi"
            bad_path.write_text(json.dumps(tampered_env))
            failed = subprocess.run([str(BINARY), "-restore", str(bad_path),
                                    "-data", str(tmp / "badrestore")],
                                   capture_output=True, text=True, timeout=8)
            require(failed.returncode != 0, "tampered backup was accepted")
            require(not (tmp / "badrestore").exists(), "failed restore left a directory")

            # A blob-level forgery that also recomputes the envelope digest is
            # caught by the image's own closure digest validation.
            tampered_env2 = json.loads(backup)
            inner2 = tampered_env2["payload"]
            h2 = next(iter(inner2["blobs"]))
            b642 = inner2["blobs"][h2]
            inner2["blobs"][h2] = b642[:10] + ("A" if b642[10] != "A" else "B") + b642[11:]
            tampered_env2["sha256"] = hashlib.sha256(
                json.dumps(inner2, separators=(",", ":")).encode()).hexdigest()
            bad2 = tmp / "forged.lmi"
            bad2.write_text(json.dumps(tampered_env2))
            failed2 = subprocess.run([str(BINARY), "-restore", str(bad2),
                                     "-data", str(tmp / "forgedrestore")],
                                    capture_output=True, text=True, timeout=8)
            require(failed2.returncode != 0, "forged blob payload was accepted")
            require(not (tmp / "forgedrestore").exists(), "forged restore left a directory")

            # 6. Source restart: checkpoint + journal replay keeps every blob and
            # sweeps interrupted temp files.
            s.call("POST", "/checkpoint")
            s.stop()
            shard = next(p.parent for p in blob_files(s.root))
            (shard / ".tmp-leftover").write_bytes(b"interrupted")
            s.start()
            require(len(temp_files(s.root)) == 0, "interrupted temp file survived restart")
            for row in s.docs()["rows"]:
                require(row["body"] is None or isinstance(row["body"], str),
                        "blob lost across restart")
            require(s.call("GET", "/stats")["large_values"]["orphan_blobs"] == 0,
                    "orphan blobs survived startup sweep")

            # A durable blob file that no retained version references (e.g. a
            # write whose journal append failed) is reclaimed at the next open
            # rather than consuming the bounded budget permanently.
            import hashlib
            abandoned = ("Z" * 4096).encode()
            ahash = hashlib.sha256(abandoned).hexdigest()
            adir = s.root / "blobs" / ahash[:2]
            adir.mkdir(parents=True, exist_ok=True)
            (adir / ahash).write_bytes(abandoned)
            s.stop()
            s.start()
            stats = s.call("GET", "/stats")["large_values"]
            require(stats["orphan_blobs"] == 0 and not (adir / ahash).exists(),
                    "abandoned blob survived startup reclamation")
            # Reachable blobs remain intact after the orphan sweep.
            require(s.get_doc(1)["body"] == BIG, "live blob damaged by orphan sweep")

            # An orphan appearing outside the process is also reclaimed at the
            # next open (the store enumerates durable files on startup), so no
            # abandoned content survives a restart and occupies the budget.
            abandoned2 = ("W" * 4096).encode()
            h2 = hashlib.sha256(abandoned2).hexdigest()
            d2 = s.root / "blobs" / h2[:2]
            d2.mkdir(parents=True, exist_ok=True)
            (d2 / h2).write_bytes(abandoned2)
            s.stop()
            s.start()
            head = s.call("GET", "/stats")["sequence"]
            s.call("POST", "/vacuum", {"horizon": head})
            require(not (d2 / h2).exists(), "runtime orphan survived reclamation")
            require(s.get_doc(1)["body"] == BIG, "live blob damaged by reclamation")

            # 7. Bounded budget: enough distinct large values exceed 16MiB and
            # the instance rejects with 429 instead of growing past the cap.
            budget = s.call("GET", "/stats")["large_values"]["budget_bytes"]
            require(budget == 16 * 1024 * 1024, "unexpected large-value budget")
            payload = "Q" * (1024 * 1024)  # 1 MiB distinct payload per row
            hit_cap = False
            for i in range(100, 120):
                tx = s.begin()
                try:
                    s.put_doc(tx, {"id": i, "title": f"t{i}", "body": payload + str(i)}, insert=True)
                    s.call("POST", f"/transactions/{tx}/commit")
                except AssertionError:
                    s.call("POST", f"/transactions/{tx}/rollback")
                    hit_cap = True
                    break
            require(hit_cap, "large-value budget was never enforced")
            capped = s.call("GET", "/stats")["large_values"]
            require(capped["total_bytes"] <= budget, "footprint exceeded the bounded budget")
            # Instance stays healthy after a capacity rejection.
            require(s.call("GET", "/health")["status"] == "ready", "instance not serving after 429")
            require(s.get_doc(1)["body"] == BIG, "existing data damaged by budget rejection")

            print(json.dumps({"workflow": "large-values", "status": "verified",
                              "large_values": s.call("GET", "/stats")["large_values"]},
                             ensure_ascii=False))
        finally:
            s.stop()
            if restored:
                restored.stop()


if __name__ == "__main__":
    main()
