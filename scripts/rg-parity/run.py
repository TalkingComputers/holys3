import subprocess, json, os, re, sys
from collections import defaultdict

H3 = os.environ.get("SEAGREP_BIN", "target/release/seagrep")
ENV = {**os.environ, "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin"}
ENV.pop("AWS_PROFILE", None)
SG_ARGS = ["s3://parity", "--index", "s3://holys3-index/parity-idx",
           "--region", "us-east-1", "--endpoint", "http://127.0.0.1:9000",
           "--index-region", "us-east-1", "--index-endpoint", "http://127.0.0.1:9000"]

# map seagrep keys (incl. archive members) to rg decoded-twin filenames
def norm_sg(key):
    key = key.replace("archive.zip!/member_a.txt", "archive_member_a.txt")
    key = key.replace("archive.zip!/sub/member_b.txt", "archive_member_b.txt")
    key = key.replace("bundle.tar.gz!/logs/app.log", "bundle_app.log")
    key = key.replace("compressed.txt.gz", "compressed.txt")
    return key

PATTERNS = [
    ("needle", []),                    # literal
    (r"needle\w+", []),                # word continuation
    (r"^needle", []),                  # line anchor start
    (r"needle$", []),                  # line anchor end (CRLF sensitivity!)
    (r"na.ve", []),                    # dot across unicode
    ("NEEDLE", ["-i"]),                # case-insensitive
    (r"\bneedle\b", []),               # word boundaries
    ("second", []),
]

def run_sg(pattern, flags):
    cmd = [H3, *flags, "-a", "--line-number", pattern, *SG_ARGS]
    p = subprocess.run(cmd, capture_output=True, text=False, env=ENV)
    if p.returncode not in (0, 1):  # 1 = no matches, same contract as rg
        sys.exit(f"seagrep failed ({p.returncode}): {p.stderr.decode(errors='replace')}")
    hits = defaultdict(set)
    for raw in p.stdout.splitlines():
        line = raw.decode(errors="surrogateescape")
        m = re.match(r"^([^:]+(?:!/[^:]+)?):(\d+):", line)
        if m:
            hits[norm_sg(m.group(1))].add(int(m.group(2)))
    return hits

def run_rg(pattern, flags):
    cmd = ["rg", *flags, "-a", "--no-mmap", "--line-number", "--no-heading", pattern, "."]
    p = subprocess.run(cmd, capture_output=True, text=False, cwd="/tmp/parity/decoded", env=ENV)
    if p.returncode not in (0, 1):
        sys.exit(f"ripgrep failed ({p.returncode}): {p.stderr.decode(errors='replace')}")
    hits = defaultdict(set)
    for raw in p.stdout.splitlines():
        line = raw.decode(errors="surrogateescape")
        m = re.match(r"^\./?([^:]+):(\d+):", line)
        if m:
            hits[m.group(1)].add(int(m.group(2)))
    return hits

divergences = 0
for pattern, flags in PATTERNS:
    sg, rg = run_sg(pattern, flags), run_rg(pattern, flags)
    files = sorted(set(sg) | set(rg))
    for f in files:
        if sg.get(f, set()) != rg.get(f, set()):
            divergences += 1
            print(f"DIVERGE pattern={pattern!r} flags={flags} file={f}")
            print(f"  seagrep: {sorted(sg.get(f, set()))}")
            print(f"  ripgrep: {sorted(rg.get(f, set()))}")
print(f"\n{'PARITY CLEAN' if divergences == 0 else f'{divergences} divergences'} across {len(PATTERNS)} patterns")
sys.exit(1 if divergences else 0)
