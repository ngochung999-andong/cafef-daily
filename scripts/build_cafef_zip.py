# scripts/build_cafef_zip.py

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests

TZ_OFFSET = 7  # GMT+7
BASE_URL = "https://cafef1.mediacdn.vn/data/ami_data"

LOOKBACK_LAST_TRADE_DAYS = 14       # dò ngày giao dịch cuối trên web (daily INDEX)
MAX_BACK_DAYS_FOR_UPTO = 7          # dò bộ Upto mới nhất
MAX_BACK_DAYS_PATCH_GUARD = 21      # chặn an toàn patch

OUT_DIR = Path("out")
WORK_DIR = Path("work")


# ----------------------------
# Time helpers
# ----------------------------
def now_gmt7() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).astimezone(
        dt.timezone(dt.timedelta(hours=TZ_OFFSET))
    ).replace(tzinfo=None)


def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")


def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def iso_to_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


# ----------------------------
# Network helpers
# ----------------------------
def head_ok(url: str, timeout: int = 20) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def download(url: str, path: Path, timeout: int = 240) -> None:
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


# ----------------------------
# CafeF URL builders
# ----------------------------
def build_daily_urls_for_date(d_iso: str) -> Tuple[str, str]:
    d = iso_to_date(d_iso)
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return solieu, index


def build_upto_urls_for_date(d_iso: str) -> Tuple[str, str]:
    d = iso_to_date(d_iso)
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.Upto{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.Upto{d_str}.zip"
    return solieu, index


# ----------------------------
# Zip / CSV helpers
# ----------------------------
def unzip_to(src_zip: Path, dest_dir: Path) -> None:
    with zipfile.ZipFile(src_zip, "r") as z:
        z.extractall(dest_dir)


def normalize_files(extract_dir: Path, required_keys: Tuple[str, ...]) -> None:
    """
    Chuẩn hoá CSV giải nén về file chuẩn.
    required_keys:
      - ("INDEX",) khi zip chỉ có index
      - ("HSX","HNX","UPCOM","INDEX") khi zip full
    """
    mapping_all = {
        "HSX": "CafeF.HSX.csv",
        "HNX": "CafeF.HNX.csv",
        "UPCOM": "CafeF.UPCOM.csv",
        "INDEX": "CafeF.INDEX.csv",
    }
    mapping = {k: mapping_all[k] for k in required_keys}

    files = list(extract_dir.rglob("*.csv"))
    if not files:
        raise RuntimeError("Không thấy CSV sau khi giải nén.")

    for key, out_name in mapping.items():
        cand = []
        for f in files:
            name = f.name.upper()
            if f".{key}." in name or f".{key}_" in name or name.endswith(f"{key}.CSV"):
                cand.append(f)
        if not cand:
            cand = [f for f in files if key in f.name.upper()]
        if not cand:
            raise RuntimeError(f"Thiếu file cho {key} trong zip.")

        cand.sort(key=lambda x: x.stat().st_size, reverse=True)
        shutil.copyfile(cand[0], extract_dir / out_name)


# ----------------------------
# Robust date extraction (QUAN TRỌNG)
# ----------------------------
def parse_any_date_token(tok: str) -> Optional[str]:
    tok = tok.strip().strip('"')
    if not tok:
        return None

    # YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", tok):
        return tok

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", tok)
    if m:
        dd = int(m.group(1))
        mm = int(m.group(2))
        yyyy = int(m.group(3))
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    # YYYYMMDD
    if re.match(r"^\d{8}$", tok):
        yyyy, mm, dd = int(tok[0:4]), int(tok[4:6]), int(tok[6:8])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    return None


def extract_date_from_line(line: str) -> Optional[str]:
    """
    KHÔNG giả định cột ngày.
    Quét toàn bộ cột (comma-separated) để tìm token giống ngày.
    """
    parts = [p.strip() for p in line.split(",")]
    for p in parts:
        di = parse_any_date_token(p)
        if di:
            return di
    return None


def max_date_in_csv(csv_path: Path) -> Optional[str]:
    """
    Lấy ngày lớn nhất trong CSV bằng cách quét tất cả cột mỗi dòng.
    """
    if not csv_path.exists():
        return None
    lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return None

    max_iso: Optional[str] = None
    for line in lines[1:]:
        d = extract_date_from_line(line)
        if not d:
            continue
        if (max_iso is None) or (d > max_iso):
            max_iso = d
    return max_iso


def csv_contains_date(csv_path: Path, target_iso: str) -> bool:
    if not csv_path.exists():
        return False
    lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return False
    for line in lines[1:]:
        d = extract_date_from_line(line)
        if d == target_iso:
            return True
    return False


def merge_daily_into_upto(upto_csv: Path, daily_csv: Path, target_iso: str) -> int:
    """
    Merge theo dòng (line) cho đúng target date.
    - Chống trùng bằng set các line thuộc target date đã có trong Upto
    - Append line thuộc target date từ daily nếu chưa có
    Ưu tiên "chắc ăn" hơn key ticker/date vì format CSV không ổn định.
    """
    if not upto_csv.exists() or not daily_csv.exists():
        return 0

    upto_lines = upto_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    daily_lines = daily_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not upto_lines or len(daily_lines) < 2:
        return 0

    header = upto_lines[0]

    # chỉ lưu set các dòng thuộc target date trong Upto để tiết kiệm RAM
    existing_target_lines: Set[str] = set()
    for line in upto_lines[1:]:
        d = extract_date_from_line(line)
        if d == target_iso:
            existing_target_lines.add(line)

    to_add: List[str] = []
    for line in daily_lines[1:]:
        d = extract_date_from_line(line)
        if d != target_iso:
            continue
        if line in existing_target_lines:
            continue
        to_add.append(line)
        existing_target_lines.add(line)

    if not to_add:
        return 0

    out = [header]
    out.extend(upto_lines[1:])
    out.extend(to_add)
    upto_csv.write_text("\n".join(out) + "\n", encoding="utf-8")
    return len(to_add)


def make_zip(src_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name in ["CafeF.HSX.csv", "CafeF.HNX.csv", "CafeF.UPCOM.csv", "CafeF.INDEX.csv"]:
            p = src_dir / name
            if not p.exists():
                raise RuntimeError(f"Thiếu {name} để đóng gói cafef.zip")
            z.write(p, arcname=name)


# ----------------------------
# 1) Find expected last trading day on web (daily INDEX zip)
# ----------------------------
def find_expected_last_trade_date_web() -> str:
    """
    Dò web CafeF để chốt 'ngày giao dịch cuối':
    - Ngày nào có CafeF.Index.DDMMYYYY.zip và CSV INDEX chứa đúng ngày đó
    - Trả về ngày mới nhất (dò từ hôm nay lùi dần)
    """
    today = now_gmt7().date()
    probe_dir = WORK_DIR / "probe_last_trade"

    for back in range(0, LOOKBACK_LAST_TRADE_DAYS + 1):
        d = today - dt.timedelta(days=back)
        d_iso = d.isoformat()
        _, index_url = build_daily_urls_for_date(d_iso)

        if not head_ok(index_url):
            continue

        if probe_dir.exists():
            shutil.rmtree(probe_dir)
        probe_dir.mkdir(parents=True, exist_ok=True)

        zpath = probe_dir / "index.zip"
        download(index_url, zpath, timeout=180)

        unzip_to(zpath, probe_dir)
        normalize_files(probe_dir, required_keys=("INDEX",))

        if csv_contains_date(probe_dir / "CafeF.INDEX.csv", d_iso):
            return d_iso

    raise RuntimeError("Không chốt được ngày giao dịch cuối từ web CafeF trong phạm vi lookback.")


# ----------------------------
# 2) Find latest Upto bundle (web)
# ----------------------------
@dataclass(frozen=True)
class UptoBundle:
    found_upto_date_iso: str
    solieu_url: str
    index_url: str


def find_latest_upto_bundle_web() -> UptoBundle:
    today = now_gmt7().date()
    for back in range(0, MAX_BACK_DAYS_FOR_UPTO + 1):
        d = today - dt.timedelta(days=back)
        d_iso = d.isoformat()
        solieu_url, index_url = build_upto_urls_for_date(d_iso)
        if head_ok(solieu_url) and head_ok(index_url):
            return UptoBundle(found_upto_date_iso=d_iso, solieu_url=solieu_url, index_url=index_url)

    raise RuntimeError("Không tìm thấy bộ Upto trong phạm vi lùi cho phép.")


# ----------------------------
# 3) Patch missing days until expected_last_trade_date
# ----------------------------
def patch_missing_days_until_expected(
    extract_dir: Path,
    expected_last_trade_date_iso: str,
) -> Dict[str, object]:
    files = {
        "HSX": extract_dir / "CafeF.HSX.csv",
        "HNX": extract_dir / "CafeF.HNX.csv",
        "UPCOM": extract_dir / "CafeF.UPCOM.csv",
        "INDEX": extract_dir / "CafeF.INDEX.csv",
    }

    candidates = []
    for k in ["INDEX", "HSX", "HNX", "UPCOM"]:
        md = max_date_in_csv(files[k])
        if md:
            candidates.append(md)
    upto_max_before = max(candidates) if candidates else None

    appended_rows = {"HSX": 0, "HNX": 0, "UPCOM": 0, "INDEX": 0}
    patched_days: List[str] = []

    if not upto_max_before:
        return {
            "upto_max_date_before": None,
            "upto_max_date_after": None,
            "patched_days": [],
            "appended_rows": appended_rows,
            "note": "Không xác định được max_date trong Upto (parse date thất bại).",
        }

    expected = iso_to_date(expected_last_trade_date_iso)
    cur = iso_to_date(upto_max_before)

    if cur >= expected:
        return {
            "upto_max_date_before": upto_max_before,
            "upto_max_date_after": upto_max_before,
            "patched_days": [],
            "appended_rows": appended_rows,
            "note": "Upto đã đủ tới expected_last_trade_date.",
        }

    gap = (expected - cur).days
    if gap > MAX_BACK_DAYS_PATCH_GUARD:
        return {
            "upto_max_date_before": upto_max_before,
            "upto_max_date_after": upto_max_before,
            "patched_days": [],
            "appended_rows": appended_rows,
            "note": f"Khoảng thiếu {gap} ngày > guard {MAX_BACK_DAYS_PATCH_GUARD}. Dừng patch.",
        }

    day = cur + dt.timedelta(days=1)
    while day <= expected:
        d_iso = day.isoformat()
        solieu_url, index_url = build_daily_urls_for_date(d_iso)

        if head_ok(solieu_url) and head_ok(index_url):
            daily_dir = WORK_DIR / f"daily_{yyyymmdd(day)}"
            if daily_dir.exists():
                shutil.rmtree(daily_dir)
            daily_dir.mkdir(parents=True, exist_ok=True)

            z_solieu = WORK_DIR / f"daily_solieu_{yyyymmdd(day)}.zip"
            z_index = WORK_DIR / f"daily_index_{yyyymmdd(day)}.zip"
            download(solieu_url, z_solieu, timeout=240)
            download(index_url, z_index, timeout=240)

            unzip_to(z_solieu, daily_dir)
            unzip_to(z_index, daily_dir)
            normalize_files(daily_dir, required_keys=("HSX", "HNX", "UPCOM", "INDEX"))

            appended_rows["HSX"] += merge_daily_into_upto(files["HSX"], daily_dir / "CafeF.HSX.csv", d_iso)
            appended_rows["HNX"] += merge_daily_into_upto(files["HNX"], daily_dir / "CafeF.HNX.csv", d_iso)
            appended_rows["UPCOM"] += merge_daily_into_upto(files["UPCOM"], daily_dir / "CafeF.UPCOM.csv", d_iso)
            appended_rows["INDEX"] += merge_daily_into_upto(files["INDEX"], daily_dir / "CafeF.INDEX.csv", d_iso)

            patched_days.append(d_iso)

        day += dt.timedelta(days=1)

    # max after
    candidates_after = []
    for k in ["INDEX", "HSX", "HNX", "UPCOM"]:
        md = max_date_in_csv(files[k])
        if md:
            candidates_after.append(md)
    upto_max_after = max(candidates_after) if candidates_after else upto_max_before

    return {
        "upto_max_date_before": upto_max_before,
        "upto_max_date_after": upto_max_after,
        "patched_days": patched_days,
        "appended_rows": appended_rows,
        "note": "OK",
    }


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Chốt ngày giao dịch cuối từ web (daily INDEX)
    expected_last_trade_date = find_expected_last_trade_date_web()

    # Step 2: Tìm và tải bộ Upto mới nhất
    upto_bundle = find_latest_upto_bundle_web()

    solieu_zip = WORK_DIR / "upto_solieu.zip"
    index_zip = WORK_DIR / "upto_index.zip"
    download(upto_bundle.solieu_url, solieu_zip)
    download(upto_bundle.index_url, index_zip)

    extract_dir = WORK_DIR / "extract_upto"
    extract_dir.mkdir(parents=True, exist_ok=True)
    unzip_to(solieu_zip, extract_dir)
    unzip_to(index_zip, extract_dir)
    normalize_files(extract_dir, required_keys=("HSX", "HNX", "UPCOM", "INDEX"))

    # Step 3: Bù ngày thiếu cho đủ tới expected_last_trade_date
    patch_report = patch_missing_days_until_expected(
        extract_dir=extract_dir,
        expected_last_trade_date_iso=expected_last_trade_date,
    )

    # Step 4: Tạo cafef.zip
    cafef_zip = OUT_DIR / "cafef.zip"
    make_zip(extract_dir, cafef_zip)

    # Step 5: latest.json
    latest = {
        "run_time_gmt7": now_gmt7().strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "GMT+7",
        "source": {
            "cafef_base": BASE_URL,
            "expected_last_trade_date_method": "Probe CafeF daily Index zip on web; verify CSV contains the date",
        },
        "expected_last_trade_date": expected_last_trade_date,
        "upto_bundle": {
            "found_upto_date_iso": upto_bundle.found_upto_date_iso,
            "solieu_url": upto_bundle.solieu_url,
            "index_url": upto_bundle.index_url,
        },
        "patch_report": patch_report,
        "assets": {"cafef_zip": "cafef.zip"},
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK: out/cafef.zip, out/latest.json")
    print("expected_last_trade_date:", expected_last_trade_date)
    print("patch_report:", json.dumps(patch_report, ensure_ascii=False))


if __name__ == "__main__":
    main()
