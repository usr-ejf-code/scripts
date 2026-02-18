#!/usr/bin/env python3
"""
rrd2perfdata.py

Drop-in Python replacement for rrd2perfdata.pl.
Requirements: rrdtool CLI available in PATH. No external Python packages required.

Behavior:
- Iterates hosts -> servicechecks -> metrics under RRD_ROOT (like the Perl script)
- Uses rrdtool dump to create XML and parses timestamps from comments + <row>/<v>
- Reads +metadata.db (SQLite) for uom when present
- Writes per-task temp output directories under OUT_DIR, then combines:
  - OUT_DIR/perfdatarrd/<timestamp>.0000
  - OUT_DIR/interval-history.tsv
- Parallelized across hosts with configurable worker count (-p)
"""

from pathlib import Path
from urllib.parse import unquote
import argparse
import os
import re
import sqlite3
import subprocess
import sys
import time
import multiprocessing
import shutil
import uuid

# Globals
TMP_PREFIX = "/tmp/migrate_rrds.dump"
COMMENT_ROW_RE = re.compile(
    r"<!--.*?/\s*(\d+)\s*-->.*?<row\b[^>]*>\s*(?:<v\b[^>]*>\s*([^<\s]+)\s*</v>|([^<\s]+))\s*</row>",
    flags=re.S,
)


def cpu_count():
    return os.cpu_count() or 1


def msg(*args):
    sys.stderr.write(f"{os.getpid()}> {' '.join(map(str, args))}\n")


def run_rrd_dump(rrd_file, tmp_xml):
    try:
        with open(tmp_xml, "w") as f:
            subprocess.run(
                ["rrdtool", "dump", rrd_file],
                stdout=f,
                stderr=subprocess.DEVNULL,
                check=True,
            )
        return os.path.exists(tmp_xml) and os.path.getsize(tmp_xml) > 0
    except Exception:
        return False


def parse_rrd_xml(xml_file, metric, uom):
    """
    Parse raw rrdtool dump XML text to extract:
      - step and dstype
      - intervals (list of [interval_seconds, start_ts, end_ts])
      - datapoints { ts: { metric: {value, uom} } }
    """
    try:
        with open(xml_file, "r", encoding="utf-8", errors="ignore") as fh:
            text = fh.read()
    except Exception:
        return {}, []

    # step
    m = re.search(r"<step>\s*(\d+)\s*</step>", text)
    step = int(m.group(1)) if m else 300

    # dstype
    m = re.search(r"<ds>.*?<type>\s*([^<\s]+)\s*</type>.*?</ds>", text, flags=re.S)
    dstype = m.group(1).strip() if m else "GAUGE"
    if dstype == "COUNTER":
        uom = "c"

    # build intervals (reverse RRA order like Perl)
    intervals = []
    rra_blocks = re.findall(r"<rra\b[^>]*>.*?</rra>", text, flags=re.S)
    rra_blocks.reverse()
    for rra in rra_blocks:
        pdp = re.search(r"<pdp_per_row>\s*(\d+)\s*</pdp_per_row>", rra)
        if not pdp:
            continue
        interval_seconds = int(pdp.group(1)) * step
        interval_data = [interval_seconds, 0, 0]

        db_blocks = re.findall(r"<database>(.*?)</database>", rra, flags=re.S)
        for db in db_blocks:
            for mm in COMMENT_ROW_RE.finditer(db):
                ts = int(mm.group(1))
                if interval_data[1] == 0:
                    interval_data[1] = ts
                interval_data[2] = ts

        if intervals:
            prev = intervals[-1]
            if prev[2] > interval_data[1]:
                prev[2] = interval_data[1]
        intervals.append(interval_data)

    # build datapoints (reverse all database blocks)
    datapoints = {}
    all_db_blocks = re.findall(r"<database>(.*?)</database>", text, flags=re.S)
    all_db_blocks.reverse()
    for db in all_db_blocks:
        for mm in COMMENT_ROW_RE.finditer(db):
            ts = int(mm.group(1))
            val_text = mm.group(2) if mm.group(2) is not None else mm.group(3)
            if not val_text:
                continue
            val_text = val_text.strip()
            if val_text == "NaN":
                continue
            try:
                value = float(val_text)
            except Exception:
                continue

            if dstype in ("COUNTER", "DERIVE"):
                value = int(round(value)) * int(step)

            value_str = ("%.11f" % value).rstrip("0").rstrip(".")
            datapoints.setdefault(ts, {})[metric] = {"value": value_str, "uom": uom or ""}

    return datapoints, intervals


def process_host_tuple(args):
    """
    Worker for a single host. Args is tuple: (host_dirname, rrd_root, out_dir)
    This mirrors the Perl script's per-host child.
    """
    host_dir, rrd_root, out_dir = args
    pid = os.getpid()
    uniq = uuid.uuid4().hex[:8]
    proc_outdir = Path(out_dir) / f"{pid}_{uniq}"
    proc_outdir.mkdir(parents=True, exist_ok=True)

    metadata_db_path = Path(rrd_root) / "+metadata.db"
    db_conn = None
    cursor = None
    if metadata_db_path.exists():
        try:
            db_conn = sqlite3.connect(str(metadata_db_path))
            cursor = db_conn.cursor()
        except Exception:
            cursor = None

    host_name = unquote(host_dir)
    host_dir_path = Path(rrd_root) / host_dir

    try:
        servicechecks = sorted([d for d in os.listdir(host_dir_path) if not d.startswith(".") and (host_dir_path / d).is_dir()])
    except Exception:
        servicechecks = []

    msg(f"Host: {host_name} with {len(servicechecks)} service checks")

    for sc_dir in servicechecks:
        sc_name = unquote(sc_dir)
        sc_dir_path = host_dir_path / sc_dir

        try:
            metrics = sorted([d for d in os.listdir(sc_dir_path) if not d.startswith(".") and (sc_dir_path / d).is_dir()])
        except Exception:
            metrics = []

        PERFDATA = {"host": host_name, "service": sc_name, "datapoints": {}, "intervals": {}}

        for mt_dir in metrics:
            metric = unquote(mt_dir)
            mt_dir_path = sc_dir_path / mt_dir
            rrd_path = mt_dir_path / "value.rrd"
            uom_path = mt_dir_path / "uom"

            if not rrd_path.exists():
                continue

            tmp_xml = f"{TMP_PREFIX}.{pid}.{uuid.uuid4().hex[:6]}.xml"
            if not run_rrd_dump(str(rrd_path), tmp_xml):
                # dump failed; skip
                try:
                    os.remove(tmp_xml)
                except Exception:
                    pass
                continue

            # try metadata DB first
            uom = None
            if cursor:
                try:
                    cursor.execute("SELECT uom FROM uoms WHERE host=? AND service=? AND metric=?", (PERFDATA["host"], PERFDATA["service"], metric))
                    row = cursor.fetchone()
                    if row:
                        uom = row[0]
                except Exception:
                    uom = None

            if not uom and uom_path.exists():
                try:
                    with open(uom_path, "r") as uf:
                        uom = uf.read().strip()
                except Exception:
                    uom = None

            datapoints, intervals = parse_rrd_xml(tmp_xml, metric, uom)
            PERFDATA["intervals"][metric] = intervals

            for ts, md in datapoints.items():
                PERFDATA["datapoints"].setdefault(ts, {}).update(md)

            # remove tmp file
            try:
                os.remove(tmp_xml)
            except Exception:
                pass

        # write timestamped files to proc_outdir
        for ts in sorted(PERFDATA["datapoints"].keys()):
            out_file = proc_outdir / str(ts)
            try:
                with open(out_file, "a") as fh:
                    perfline = [str(ts), PERFDATA["host"], PERFDATA["service"], "RRD"]
                    metrics_parts = []
                    for metric_name in sorted(PERFDATA["datapoints"][ts].keys()):
                        d = PERFDATA["datapoints"][ts][metric_name]
                        metrics_parts.append(f"{metric_name}={d['value']}{d['uom']}")
                    fh.write("||".join(perfline + ["; ".join(metrics_parts)]) + "\n")
            except Exception as e:
                msg(f"Error writing perf file {out_file}: {e}")

        # write interval history for this proc
        hist_file = proc_outdir / "interval-history.tsv"
        try:
            with open(hist_file, "a") as hf:
                for metric_name in sorted(PERFDATA["intervals"].keys()):
                    for interval in sorted(PERFDATA["intervals"][metric_name], key=lambda x: x[1]):
                        start = interval[1]
                        end = interval[2]
                        interval_seconds = interval[0]
                        hsm = "::".join([PERFDATA["host"], PERFDATA["service"], metric_name])
                        hf.write(f"{hsm}\t{start}\t{end}\t{interval_seconds}\n")
        except Exception as e:
            msg(f"Error writing interval history {hist_file}: {e}")

    if db_conn:
        try:
            db_conn.close()
        except Exception:
            pass

    return


def combine_outputs(out_dir):
    out_dir = Path(out_dir)
    print("Combining perfdata file by timestamp...")
    # find per-task dirs (not starting with dot, and are directories)
    pids = [d for d in os.listdir(out_dir) if not d.startswith(".") and (out_dir / d).is_dir()]
    ts_files = {}

    for pid in sorted(pids):
        pid_dir = out_dir / pid
        files = [f for f in os.listdir(pid_dir) if (pid_dir / f).is_file() and f != "interval-history.tsv"]
        for f in files:
            ts_files.setdefault(f, []).append(str(pid_dir / f))

    perf_dir = out_dir / "perfdatarrd"
    perf_dir.mkdir(parents=True, exist_ok=True)

    # concatenate source files into perfdatarrd/<ts>.0000
    for ts in sorted(ts_files.keys(), key=lambda x: int(x)):
        dest = perf_dir / f"{ts}.0000"
        with open(dest, "a") as dst:
            for src in ts_files[ts]:
                try:
                    with open(src, "r") as s:
                        shutil.copyfileobj(s, dst)
                except Exception as e:
                    msg(f"Combine error reading {src}: {e}")

    # combine interval-history.tsv
    hist_out = out_dir / "interval-history.tsv"
    with open(hist_out, "a") as outfh:
        for pid in pids:
            hist_src = out_dir / pid / "interval-history.tsv"
            if hist_src.exists():
                try:
                    with open(hist_src, "r") as inh:
                        shutil.copyfileobj(inh, outfh)
                except Exception as e:
                    msg(f"Error combining {hist_src}: {e}")

    # remove per-task dirs
    for pid in pids:
        try:
            shutil.rmtree(out_dir / pid)
        except Exception as e:
            msg(f"Error removing {out_dir / pid}: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--input", required=True, help="RRD root directory")
    p.add_argument("-o", "--output", required=True, help="Output directory")
    p.add_argument("-p", "--processes", type=int, help="Max parallel processes")
    args = p.parse_args()

    rrd_root = os.path.abspath(args.input)
    out_dir = os.path.abspath(args.output)
    max_proc = args.processes or (cpu_count() + 1)

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # collect host directories (top-level, non-dot)
    try:
        hosts = sorted([d for d in os.listdir(rrd_root) if not d.startswith(".") and os.path.isdir(os.path.join(rrd_root, d))])
    except Exception as e:
        print(f"Cannot list hosts under {rrd_root}: {e}", file=sys.stderr)
        sys.exit(1)

    if not hosts:
        print("No hosts found under", rrd_root, file=sys.stderr)
        sys.exit(1)

    # prepare worker args list
    worker_args = [(h, rrd_root, out_dir) for h in hosts]

    # use multiprocessing Pool to process hosts in parallel
    pool = multiprocessing.Pool(processes=max_proc)
    try:
        pool.map(process_host_tuple, worker_args)
    finally:
        pool.close()
        pool.join()

    print("All metrics extracted!")
    combine_outputs(out_dir)
    print("Done.")


if __name__ == "__main__":
    main()

