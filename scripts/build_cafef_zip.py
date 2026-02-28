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

LOOKBACK_LAST_TRADE_DAYS = 14      # dò ngày giao dịch cuối trên web
MAX_BACK_DAYS_FOR_UPTO = 7         # dò bộ Upto mới nhất

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
# Zip / CSV normalize
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
# Robust date detection (quét mọi cột)
# ----------------------------
def parse_any_date_token(tok: str) -> Optional[str]:
    tok = tok.strip().strip('"').strip()
    if not tok:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", tok):
        return tok

    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", tok)
    if m:
        dd = int(m.group(1))
        mm = int(m.group(2))
        yyyy = int(m.group(3))
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    if re.fullmatch(r"\d{8}", tok):
        yyyy = int(tok[0:4])
        mm = int(tok[4:6])
        dd = int(tok[6:8])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    if re.fullmatch(r"\d{14}", tok):  # YYYYMMDDHHMMSS
        yyyy = int(tok[0:4])
        mm = int(tok[4:6])
        dd = int(tok[6:8])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    return None


def extract_date_from_line(line: str) -> Optional[str]:
    for p in line.split(","):
        d = parse_any_date_token(p)
        if d:
            return d
    return None


def csv_contains_date(csv_path: Path, target_iso: str) -> bool:
    if not csv_path.exists():
        return False
    lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return False
    for line in lines[1:]:
        if extract_date_from_line(line) == target_iso:
            return True
    return False


def merge_daily_into_upto(upto_csv: Path, daily_csv: Path, target_iso: str) -> int:
    """
    Ghép theo dòng (line) cho đúng target date.
    - chống trùng theo line của target date
    """
    if not upto_csv.exists() or not daily_csv.exists():
        return 0

    upto_lines = upto_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    daily_lines = daily_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not upto_lines or len(daily_lines) < 2:
        return 0

    header = upto_lines[0]

    existing_target: Set[str] = set()
    for line in upto_lines[1:]:
        if extract_date_from_line(line) == target_iso:
            existing_target.add(line)

    to_add: List[str] = []
    for line in daily_lines[1:]:
        if extract_date_from_line(line) != target_iso:
            continue
        if line in existing_target:
            continue
        to_add.append(line)
        existing_target.add(line)

    if not to_add:
        return 0

    out = [header]
    out.extend(upto_lines[1:])
    out.extend(to_add)
    upto_csv.write_text("\n".join(out) + "\n", encoding="utf-8")
    return len(to_add)


# ----------------------------
# (1) Chốt ngày giao dịch cuối từ web (daily INDEX)
# ----------------------------
def find_last_trade_date_from_web() -> str:
    """
    Dò từ hôm nay lùi LOOKBACK_LAST_TRADE_DAYS:
    - Có CafeF.Index.DDMMYYYY.zip (HEAD 200)
    - Và CSV INDEX chứa đúng ngày đó
    => ngày đầu tiên thoả = ngày giao dịch cuối cùng có dữ liệu
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

    raise RuntimeError("Không xác định được ngày giao dịch cuối từ web CafeF trong phạm vi lookback.")


# ----------------------------
# (2) Tìm bộ Upto mới nhất
# ----------------------------
@dataclass(frozen=True)
class UptoBundle:
    found_upto_date_iso: str
    solieu_url: str
    index_url: str


def find_latest_upto_bundle() -> UptoBundle:
    today = now_gmt7().date()
    for back in range(0, MAX_BACK_DAYS_FOR_UPTO + 1):
        d = today - dt.timedelta(days=back)
        d_iso = d.isoformat()
        solieu_url, index_url = build_upto_urls_for_date(d_iso)
        if head_ok(solieu_url) and head_ok(index_url):
            return UptoBundle(d_iso, solieu_url, index_url)
    raise RuntimeError("Không tìm thấy bộ Upto trong phạm vi lùi cho phép.")


# ----------------------------
# (4)(5) Zip output
# ----------------------------
def build_output_zip(zip_path: Path, files_to_include: List[Tuple[Path, str]]) -> None:
    """
    files_to_include: list of (path_on_disk, arcname_in_zip)
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for src, arc in files_to_include:
            if not src.exists():
                raise RuntimeError(f"Thiếu file để zip: {src}")
            z.write(src, arcname=arc)


# ----------------------------
# Main theo 5 bước user yêu cầu
# ----------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Xác định ngày giao dịch cuối cùng bằng web
    last_trade_iso = find_last_trade_date_from_web()
    last_trade_date = iso_to_date(last_trade_iso)
    last_trade_yyyymmdd = yyyymmdd(last_trade_date)

    # 2) Tải tất cả file Upto và theo ngày giao dịch cuối về
    upto = find_latest_upto_bundle()

    upto_solieu_zip = WORK_DIR / "upto_solieu.zip"
    upto_index_zip = WORK_DIR / "upto_index.zip"
    download(upto.solieu_url, upto_solieu_zip)
    download(upto.index_url, upto_index_zip)

    daily_solieu_url, daily_index_url = build_daily_urls_for_date(last_trade_iso)

    if not (head_ok(daily_solieu_url) and head_ok(daily_index_url)):
        raise RuntimeError(
            f"Daily zip cho ngày giao dịch cuối không tồn tại đủ bộ: {last_trade_iso}"
        )

    daily_solieu_zip = WORK_DIR / f"daily_solieu_{last_trade_yyyymmdd}.zip"
    daily_index_zip = WORK_DIR / f"daily_index_{last_trade_yyyymmdd}.zip"
    download(daily_solieu_url, daily_solieu_zip)
    download(daily_index_url, daily_index_zip)

    # Giải nén + chuẩn hoá Upto (4 file chuẩn)
    upto_dir = WORK_DIR / "upto_extract"
    upto_dir.mkdir(parents=True, exist_ok=True)
    unzip_to(upto_solieu_zip, upto_dir)
    unzip_to(upto_index_zip, upto_dir)
    normalize_files(upto_dir, required_keys=("HSX", "HNX", "UPCOM", "INDEX"))

    # Giải nén + chuẩn hoá Daily (4 file chuẩn)
    daily_dir = WORK_DIR / f"daily_extract_{last_trade_yyyymmdd}"
    daily_dir.mkdir(parents=True, exist_ok=True)
    unzip_to(daily_solieu_zip, daily_dir)
    unzip_to(daily_index_zip, daily_dir)
    normalize_files(daily_dir, required_keys=("HSX", "HNX", "UPCOM", "INDEX"))

    # 3) Kiểm tra ngày tại file Upto xem có bằng ngày giao dịch cuối không,
    #    nếu nhỏ hơn (thực tế: không có ngày đó) thì ghép daily đúng ngày đó vào.
    merge_report = {"HSX": 0, "HNX": 0, "UPCOM": 0, "INDEX": 0}
    missing_report = {}

    for k in ["HSX", "HNX", "UPCOM", "INDEX"]:
        upto_csv = upto_dir / f"CafeF.{k}.csv"
        daily_csv = daily_dir / f"CafeF.{k}.csv"

        has_last = csv_contains_date(upto_csv, last_trade_iso)
        missing_report[k] = (not has_last)

        if not has_last:
            merge_report[k] = merge_daily_into_upto(upto_csv, daily_csv, last_trade_iso)

    # 4) Đổi tên Upto theo chuẩn
    # (Đã chuẩn trong upto_dir: CafeF.<MARKET>.csv)

    # 5) Tạo cafef.zip gồm:
    #    - 4 file Upto chuẩn hoá (đã ghép nếu thiếu)
    #    - + 4 file Daily theo ngày giao dịch cuối (đặt tên rõ ràng trong zip)
    out_zip = OUT_DIR / "cafef.zip"

    include_files: List[Tuple[Path, str]] = []

    # Upto chuẩn (arcname cố định)
    include_files.append((upto_dir / "CafeF.HSX.csv", "CafeF.HSX.csv"))
    include_files.append((upto_dir / "CafeF.HNX.csv", "CafeF.HNX.csv"))
    include_files.append((upto_dir / "CafeF.UPCOM.csv", "CafeF.UPCOM.csv"))
    include_files.append((upto_dir / "CafeF.INDEX.csv", "CafeF.INDEX.csv"))

    # Daily file (arcname có suffix DAILY_YYYYMMDD)
    include_files.append((daily_dir / "CafeF.HSX.csv", f"CafeF.HSX.DAILY_{last_trade_yyyymmdd}.csv"))
    include_files.append((daily_dir / "CafeF.HNX.csv", f"CafeF.HNX.DAILY_{last_trade_yyyymmdd}.csv"))
    include_files.append((daily_dir / "CafeF.UPCOM.csv", f"CafeF.UPCOM.DAILY_{last_trade_yyyymmdd}.csv"))
    include_files.append((daily_dir / "CafeF.INDEX.csv", f"CafeF.INDEX.DAILY_{last_trade_yyyymmdd}.csv"))

    build_output_zip(out_zip, include_files)

    latest = {
        "run_time_gmt7": now_gmt7().strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "GMT+7",
        "source": {"cafef_base": BASE_URL},
        "last_trade_date_web": last_trade_iso,
        "upto_bundle": {
            "found_upto_date_iso": upto.found_upto_date_iso,
            "solieu_url": upto.solieu_url,
            "index_url": upto.index_url,
        },
        "daily_urls_last_trade_date": {
            "solieu_url": daily_solieu_url,
            "index_url": daily_index_url,
        },
        "upto_missing_last_trade_date": missing_report,
        "merge_report_appended_rows": merge_report,
        "zip_content": [arc for _, arc in include_files],
        "assets": {"cafef_zip": "cafef.zip"},
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK: out/cafef.zip, out/latest.json")
    print("last_trade_date_web:", last_trade_iso)
    print("merge_report:", json.dumps(merge_report, ensure_ascii=False))


if __name__ == "__main__":
    main()
