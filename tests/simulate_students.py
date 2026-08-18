"""Simulate how a cohort actually uses this lab.  `make simulate`

`make test` and `make run-all` prove the lab works when you do it right.
This proves it works when you do it *wrong* — which is what 40 students do.

Each scenario rsyncs a fresh clone (no venv, no data, no .ipynb) into a
scratch dir and abuses it. Two real bugs came out of this suite and are now
regression-tested in tests/test_lab18.py:

  * NB4 died with a raw Rust `Os { code: 2 }` when `make data` was skipped.
  * `make smoke` deleted the Iceberg catalog of a notebook that was running.
    Intermittent — one run passed it by timing luck before it was forced.

S10 (Python 3.10) and S11 (plain pip) build their own venvs and are slow;
skip them with SIM_FAST=1.
"""
from __future__ import annotations

import concurrent.futures as cf
import os
import shutil
import subprocess
import tempfile
import sys
import time
from pathlib import Path

LAB = Path(__file__).resolve().parents[1]
WORK = Path(os.environ.get("SIM_WORKDIR", tempfile.gettempdir())) / "day18-student-sim"
PY_BIN = Path(sys.executable)
NBS = sorted(p.name for p in (LAB / "notebooks").glob("[0-9]*.py"))

results: list[tuple[str, bool, str]] = []


def fresh_clone(name: str) -> Path:
    """Simulate `git clone` — no venv, no data, no generated notebooks."""
    dst = WORK / name
    shutil.rmtree(dst, ignore_errors=True)
    dst.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "rsync", "-a",
        "--exclude", ".venv*", "--exclude", "_lakehouse", "--exclude", ".git",
        "--exclude", "*.ipynb", "--exclude", ".pytest_cache",
        f"{LAB}/", f"{dst}/",
    ], check=True)
    return dst


def run(cmd: list[str], cwd: Path, env: dict | None = None, timeout: int = 600):
    e = dict(os.environ)
    e.pop("PYTHONPATH", None)
    if env:
        e.update(env)
    return subprocess.run(cmd, cwd=cwd, env=e, capture_output=True, text=True, timeout=timeout)


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  — {detail}" if detail and not ok else ""))


# ── S1: notebooks run out of order ────────────────────────────────────────
def s1_out_of_order():
    d = fresh_clone("out_of_order")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    bad = []
    for nb in reversed(NBS):                       # 08 → 01
        p = run([str(PY_BIN), f"notebooks/{nb}"], d)
        if p.returncode != 0:
            bad.append((nb, (p.stdout + p.stderr).strip().splitlines()[-1][:120]))
    record("S1 notebooks run in reverse order", not bad, "; ".join(f"{n}: {e}" for n, e in bad))


# ── S2: re-running a notebook (the #1 student action) ─────────────────────
def s2_idempotent():
    d = fresh_clone("rerun")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    bad = []
    for nb in NBS:
        run([str(PY_BIN), f"notebooks/{nb}"], d)
        p = run([str(PY_BIN), f"notebooks/{nb}"], d)   # second time
        if p.returncode != 0:
            bad.append((nb, (p.stdout + p.stderr).strip().splitlines()[-1][:120]))
    record("S2 every notebook re-runs cleanly", not bad, "; ".join(f"{n}: {e}" for n, e in bad))


# ── S3: forgot `make data` / `make data-ai` ───────────────────────────────
def s3_missing_data():
    d = fresh_clone("no_data")
    bad = []
    for nb in NBS:
        p = run([str(PY_BIN), f"notebooks/{nb}"], d)
        if p.returncode != 0:
            bad.append((nb, (p.stdout + p.stderr).strip().splitlines()[-1][:140]))
    record("S3 notebooks work without running the generators first",
           not bad, "; ".join(f"{n}: {e}" for n, e in bad))


# ── S4: launched from inside notebooks/ (Jupyter's default cwd) ───────────
def s4_cwd_notebooks():
    d = fresh_clone("cwd")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    bad = []
    for nb in NBS:
        p = run([str(PY_BIN), nb], d / "notebooks")
        if p.returncode != 0:
            bad.append((nb, (p.stdout + p.stderr).strip().splitlines()[-1][:120]))
    record("S4 notebooks run with cwd=notebooks/ (Jupyter default)",
           not bad, "; ".join(f"{n}: {e}" for n, e in bad))


# ── S5: two notebooks open at once in Jupyter Lab ─────────────────────────
def s5_concurrent():
    d = fresh_clone("concurrent")
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    pairs = [("05_iceberg_catalog.py", "06_maintenance.py"),
             ("05_iceberg_catalog.py", "08_agents_provenance.py"),
             ("06_maintenance.py", "08_agents_provenance.py")]
    bad = []
    for a, b in pairs:
        with cf.ThreadPoolExecutor(2) as ex:
            fa = ex.submit(run, [str(PY_BIN), f"notebooks/{a}"], d)
            time.sleep(0.12)          # force the overlap instead of hoping for it
            fb = ex.submit(run, [str(PY_BIN), f"notebooks/{b}"], d)
            ra, rb = fa.result(), fb.result()
        for nb, r in ((a, ra), (b, rb)):
            if r.returncode != 0:
                bad.append(f"{a}+{b}→{nb}: {(r.stdout + r.stderr).strip().splitlines()[-1][:90]}")
    record("S5 two notebooks running concurrently (Jupyter Lab)", not bad, " | ".join(bad[:3]))


# ── S6: `make smoke` while a notebook is open ─────────────────────────────
def s6_smoke_during_work():
    d = fresh_clone("smoke_race")
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    with cf.ThreadPoolExecutor(2) as ex:
        fa = ex.submit(run, [str(PY_BIN), "notebooks/05_iceberg_catalog.py"], d)
        time.sleep(0.15)
        fb = ex.submit(run, [str(PY_BIN), "scripts/verify_lite.py"], d)
        ra, rb = fa.result(), fb.result()
    bad = []
    if ra.returncode != 0:
        bad.append("NB5 broken by concurrent smoke: " + (ra.stdout + ra.stderr).strip().splitlines()[-1][:90])
    if rb.returncode != 0:
        bad.append("smoke broken by concurrent NB5: " + (rb.stdout + rb.stderr).strip().splitlines()[-1][:90])
    record("S6 `make smoke` while a notebook is running", not bad, " | ".join(bad))


# ── S7: fully offline (no DNS, no proxy) ──────────────────────────────────
def s7_offline():
    d = fresh_clone("offline")
    env = {"HTTP_PROXY": "http://127.0.0.1:9", "HTTPS_PROXY": "http://127.0.0.1:9",
           "ALL_PROXY": "http://127.0.0.1:9", "NO_PROXY": "",
           "HOME": str(d / "fakehome")}       # also hides any cached duckdb extensions
    (d / "fakehome").mkdir(exist_ok=True)
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d, env)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d, env)
    bad = []
    p = run([str(PY_BIN), "scripts/verify_lite.py"], d, env)
    if p.returncode != 0:
        bad.append("smoke: " + (p.stdout + p.stderr).strip().splitlines()[-1][:100])
    for nb in NBS:
        r = run([str(PY_BIN), f"notebooks/{nb}"], d, env)
        if r.returncode != 0:
            bad.append(f"{nb}: " + (r.stdout + r.stderr).strip().splitlines()[-1][:100])
    record("S7 everything works with no network at all", not bad, " | ".join(bad[:3]))


# ── S8: student edits a threshold / runs on a slow box ────────────────────
def s8_low_resource():
    """Simulate a laptop where wall-clock benchmarks are unreliable."""
    d = fresh_clone("slow")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    # NB2's gate must not depend on wall-clock alone; force the timing path to
    # look bad by running under heavy CPU contention.
    load = [subprocess.Popen([str(PY_BIN), "-c",
                              "import time;t=time.time()\nwhile time.time()-t<25: pass"])
            for _ in range(max(2, os.cpu_count() // 2))]
    try:
        p = run([str(PY_BIN), "notebooks/02_optimize_zorder.py"], d)
    finally:
        for x in load:
            x.kill()
    record("S8 NB2 passes under CPU contention (no wall-clock-only gate)",
           p.returncode == 0, (p.stdout + p.stderr).strip().splitlines()[-1][:140])



# ── S9: executed as a real .ipynb through Jupyter ─────────────────────────
def s9_nbconvert():
    d = fresh_clone("nbconvert")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    jt = run([str(PY_BIN.parent / "jupytext"), "--to", "notebook", "notebooks/"], d)
    if jt.returncode != 0:
        jt = run([str(PY_BIN.parent / "jupytext"), "--to", "notebook",
                  *[f"notebooks/{n}" for n in NBS]], d)
    bad = []
    for nb in NBS:
        ipynb = f"notebooks/{nb[:-3]}.ipynb"
        if not (d / ipynb).exists():
            bad.append(f"{ipynb} not generated")
            continue
        r = run([str(PY_BIN), "-m", "jupyter", "nbconvert", "--to", "notebook",
                 "--execute", "--inplace", ipynb], d, timeout=900)
        if r.returncode != 0:
            tail = (r.stderr or r.stdout).strip().splitlines()
            bad.append(f"{nb}: {tail[-1][:110] if tail else 'unknown'}")
    record("S9 notebooks execute as .ipynb via nbconvert (real Jupyter path)",
           not bad, " | ".join(bad[:3]))


# ── S10: oldest supported Python (3.10) ───────────────────────────────────
def s10_py310():
    d = fresh_clone("py310")
    v = run(["uv", "venv", ".venv", "--python", "3.10", "-q"], d, timeout=900)
    if v.returncode != 0:
        record("S10 Python 3.10 (oldest supported)", False, "could not create 3.10 venv")
        return
    i = run(["uv", "pip", "install", "--python", ".venv/bin/python", "-q",
             "-r", "requirements.txt"], d, timeout=1800)
    if i.returncode != 0:
        record("S10 Python 3.10 (oldest supported)", False,
               "install failed: " + (i.stderr or "").strip().splitlines()[-1][:140])
        return
    py = str(d / ".venv/bin/python")
    run([py, "scripts/generate_data_lite.py"], d)
    run([py, "scripts/generate_ai_data.py"], d)
    sm = run([py, "scripts/verify_lite.py"], d)
    ra = run([py, "scripts/run_all.py"], d, timeout=1800)
    ok = sm.returncode == 0 and ra.returncode == 0
    record("S10 Python 3.10 (oldest supported)", ok,
           (ra.stdout or "").strip().splitlines()[-1][:140] if not ok else "")


# ── S11: plain pip, no uv (the documented fallback) ───────────────────────
def s11_plain_pip():
    d = fresh_clone("pip")
    v = run([sys.executable, "-m", "venv", ".venv"], d, timeout=900)
    if v.returncode != 0:
        record("S11 plain `python -m venv` + pip (no uv)", False, "venv failed")
        return
    i = run([str(d / ".venv/bin/pip"), "install", "-q", "-r", "requirements.txt"],
            d, timeout=2400)
    if i.returncode != 0:
        record("S11 plain `python -m venv` + pip (no uv)", False,
               (i.stderr or "").strip().splitlines()[-1][:140])
        return
    py = str(d / ".venv/bin/python")
    run([py, "scripts/generate_data_lite.py"], d)
    run([py, "scripts/generate_ai_data.py"], d)
    r = run([py, "scripts/run_all.py"], d, timeout=1800)
    record("S11 plain `python -m venv` + pip (no uv)", r.returncode == 0,
           (r.stdout or "").strip().splitlines()[-1][:140])


# ── S12: `make clean` halfway through, then keep working ──────────────────
def s12_clean_midway():
    d = fresh_clone("clean_mid")
    run([str(PY_BIN), "scripts/generate_data_lite.py"], d)
    run([str(PY_BIN), "scripts/generate_ai_data.py"], d)
    run([str(PY_BIN), "notebooks/04_medallion.py"], d)
    shutil.rmtree(d / "_lakehouse", ignore_errors=True)      # `make clean`
    bad = []
    for nb in NBS:
        r = run([str(PY_BIN), f"notebooks/{nb}"], d)
        if r.returncode != 0:
            bad.append(f"{nb}: " + (r.stdout + r.stderr).strip().splitlines()[-1][:110])
    record("S12 notebooks recover after `make clean` wipes _lakehouse",
           not bad, " | ".join(bad[:3]))


def main():
    print(f"Simulating student usage against {LAB.name}")
    print(f"  interpreter: {PY_BIN}")
    print(f"  scratch:     {WORK}\n")
    scenarios = [s3_missing_data, s2_idempotent, s1_out_of_order, s4_cwd_notebooks,
                 s5_concurrent, s6_smoke_during_work, s7_offline, s8_low_resource,
                 s9_nbconvert, s12_clean_midway]
    if os.environ.get("SIM_FAST") != "1":
        scenarios += [s10_py310, s11_plain_pip]     # build their own venvs; slow
    for fn in scenarios:
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            record(fn.__name__, False, f"harness error: {type(e).__name__}: {e}")
    n_fail = sum(1 for _, ok, _ in results if not ok)
    print(f"\n{len(results) - n_fail}/{len(results)} scenarios passed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
