# scripts/build_cafef_zip.py
# Mục tiêu:
# 1) Tải bộ Upto (SolieuGD + Index) mới nhất (lùi tối đa 7 ngày)
# 2) Chuẩn hoá 4 file CSV: CafeF.HSX.csv / CafeF.HNX.csv / CafeF.UPCOM.csv / CafeF.INDEX.csv
# 3) Kiểm tra nếu bộ Upto thiếu phiên cuối (ngày target = found_package_date) thì tải thêm bộ theo ngày
#    (CafeF.SolieuGD.DDMMYYYY.zip + CafeF.Index.DDMMYYYY.zip) và ghép (append) phần thiếu vào file Upto
# 4) Đóng gói thành out/cafef.zip và out/latest.json
#
# Ghi chú:
# - Tham số ?t=... (cache buster) KHÔNG bắt buộc, thường tải được khi bỏ.
# - Mã này ưu tiên độ bền: heuristic parse date + chống trùng bằng key.

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

import requests

TZ_OFFSET = 7  # GMT+7
BASE_URL = "https://cafef1.mediacdn.vn/data/ami_data"
MAX_BACK_DAYS = 7

OUT_DIR = Path("out")
WORK_DIR = Path("work")


# ----------------------------
# Time helpers
# ----------------------------
def now_gmt7() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=TZ_OFFSET)


def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")


def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


# ----------------------------
# Network helpers
# ----------------------------
def head_ok(url: str, timeout: int = 20) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def download(url: str, path: Path, timeout: int = 180) -> None:
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


# ----------------------------
# CafeF URL discovery
# ----------------------------
@dataclass(frozen=True)
class CafeFBundle:
    found_package_date_iso: str  # YYYY-MM-DD
    folder: str  # YYYYMMDD
    solieu_url: str
    index_url: str
    pattern_used: str  # "upto" or "daily"


def find_latest_upto_bundle() -> CafeFBundle:
    """
    Tìm bộ Upto mới nhất trong phạm vi lùi MAX_BACK_DAYS.
    Ưu tiên Upto; nếu không có Upto thì fallback daily (nhưng vẫn coi như bundle chính).
    """
    today = now_gmt7().date()

    patterns = [
        ("upto", "CafeF.SolieuGD.Upto{d}.zip", "CafeF.Index.Upto{d}.zip"),
        ("daily", "CafeF.SolieuGD.{d}.zip", "CafeF.Index.{d}.zip"),
    ]

    for back in range(0, MAX_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)
        d_str = ddmmyyyy(d)

        for kind, p1, p2 in patterns:
            f1 = p1.format(d=d_str)
            f2 = p2.format(d=d_str)
            u1 = f"{BASE_URL}/{folder}/{f1}"
                    found_package_date_iso=d.isoformat(),
                    folder=folder,
                    solieu_url=u1,
                    index_url=u2,
                    pattern_used=kind,
                )

    raise RuntimeError("Không tìm thấy bộ file CafeF (Upto hoặc theo ngày) trong phạm vi lùi 7 ngày.")


def build_daily_urls_for_date(d_iso: str) -> Tuple[str, str]:
    d = dt.date.fromisoformat(d_iso)
    d_str = ddmmyyyy(d)
    folder = yyyymmdd(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return solieu, index


# ----------------------------
# Zip / CSV helpers
# ----------------------------
def unzip_to(src_zip: Path, dest_dir: Path) -> None:
    with zipfile.ZipFile(src_zip, "r") as z:
        z.extractall(dest_dir)


def normalize_files(extract_dir: Path) -> None:
    """
    Gom các CSV giải nén về đúng 4 file chuẩn.
    Chọn file lớn nhất match theo key (HSX/HNX/UPCOM/INDEX).
    """
    mapping = {
        "HSX": "CafeF.HSX.csv",
        "HNX": "CafeF.HNX.csv",
        "UPCOM": "CafeF.UPCOM.csv",
        "INDEX": "CafeF.INDEX.csv",
    }

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
            u2 = f"{BASE_URL}/{folder}/{f2}"
            if head_ok(u1) and head_ok(u2):
                return CafeFBundle(
        shutil.copyfile(cand[0], extract_dir / out_name)


def parse_any_date_token(tok: str) -> Optional[str]:
    tok = tok.strip()
    if not tok:
        return None

    # YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", tok):
        return tok

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{4})$", tok)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}"

    # YYYYMMDD
    if re.match(r"^\d{8}$", tok):

        yyyy, mm, dd = tok[0:4], tok[4:6], tok[6:8]
        return f"{yyyy}-{mm}-{dd}"

    return None


KeyType = Union[Tuple[str, str], Tuple[str]]  # (TICKER, ISO_DATE) or (ISO_DATE,)


def extract_key_from_line(line: str) -> Optional[KeyType]:
    """
    Heuristic key extraction:
    - If looks like: Ticker, Date, ... -> (TICKER, ISO_DATE)
    - Else if looks like: Date, ... -> (ISO_DATE,)
    """
    parts = [p.strip() for p in line.split(",")]
    if not parts:
        return None

    # common case: ticker in col0, date in col1
    if len(parts) >= 2:
        d = parse_any_date_token(parts[1])
        if d and re.match(r"^[A-Za-z0-9\.\-_]{1,20}$", parts[0]):
            return (parts[0].upper(), d)

    # fallback: date in col0
    d0 = parse_any_date_token(parts[0])
    if d0:
        return (d0,)

    # fallback: scan first 3 cols for a date
    for i in range(min(3, len(parts))):
        di = parse_any_date_token(parts[i])
        if di:
            return (di,)

    return None


def file_has_target_date(csv_path: Path, target_iso: str) -> bool:
    if not csv_path.exists():
        return False
    lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return False
    for line in lines[1:]:
        k = extract_key_from_line(line)
        if not k:
            continue
        if k[-1] == target_iso:
            return True
    return False


def merge_daily_into_upto(upto_csv: Path, daily_csv: Path, target_iso: str) -> int:
    """
    Append lines of target date from daily_csv into upto_csv if not present.
    Returns number of appended lines.
    """
    if not upto_csv.exists() or not daily_csv.exists():
        return 0

    upto_lines = upto_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    daily_lines = daily_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not upto_lines or len(daily_lines) < 2:
        return 0

    # Choose header: keep existing upto header
    header = upto_lines[0]

    existing: Set[KeyType] = set()
    for line in upto_lines[1:]:
        k = extract_key_from_line(line)
        if k:
            existing.add(k)

    to_add: List[str] = []
    for line in daily_lines[1:]:
        k = extract_key_from_line(line)
        if not k:
            continue
        if k[-1] != target_iso:
            continue
        if k in existing:
            continue
        to_add.append(line)
        existing.add(k)

    if not to_add:
        return 0

    out = [header]
    out.extend(upto_lines[1:])
    out.extend(to_add)
    upto_csv.write_text("\n".join(out) + "\n", encoding="utf-8")
    return len(to_add)


def read_data_date_from_index(index_csv: Path) -> str:
    """
    Theo quy ước dự án: 2 dòng đầu tiên sau header phản ánh ngày giao dịch gần nhất.
    Ở đây đọc dòng đầu tiên sau header và cố parse date.
    """
    if not index_csv.exists():
        return "N/A"
    lines = index_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return "N/A"
    row1 = lines[1]
    tokens = re.split(r"[,\t;]", row1)
    for t in tokens[:8]:
        di = parse_any_date_token(t)
        if di:
            return di
    return "N/A"


def make_zip(src_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name in ["CafeF.HSX.csv", "CafeF.HNX.csv", "CafeF.UPCOM.csv", "CafeF.INDEX.csv"]:
            p = src_dir / name
            if not p.exists():
                raise RuntimeError(f"Thiếu {name} để đóng gói cafef.zip")
            z.write(p, arcname=name)


# ----------------------------
# Main pipeline
# ----------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Find latest bundle (prefer Upto)
    bundle = find_latest_upto_bundle()

    # 2) Download Upto bundle
    solieu_zip = WORK_DIR / "solieu.zip"
    index_zip = WORK_DIR / "index.zip"
    download(bundle.solieu_url, solieu_zip)
    download(bundle.index_url, index_zip)

    extract_dir = WORK_DIR / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)

    unzip_to(solieu_zip, extract_dir)
    unzip_to(index_zip, extract_dir)
    normalize_files(extract_dir)

    # 3) Patch if missing target date in Upto files:
    target_iso = bundle.found_package_date_iso
    patched = False
    patch_detail: Dict[str, int] = {"HSX": 0, "HNX": 0, "UPCOM": 0, "INDEX": 0}

    # Determine if any of 4 files missing target date
    need_patch = False
    for fn in ["CafeF.HSX.csv", "CafeF.HNX.csv", "CafeF.UPCOM.csv", "CafeF.INDEX.csv"]:
        if not file_has_target_date(extract_dir / fn, target_iso):
            need_patch = True
            break

    if need_patch:
        daily_solieu_url, daily_index_url = build_daily_urls_for_date(target_iso)
        if head_ok(daily_solieu_url) and head_ok(daily_index_url):
            daily_solieu_zip = WORK_DIR / "daily_solieu.zip"
            daily_index_zip = WORK_DIR / "daily_index.zip"
            download(daily_solieu_url, daily_solieu_zip)
            download(daily_index_url, daily_index_zip)

            daily_dir = WORK_DIR / "daily_extract"
            daily_dir.mkdir(parents=True, exist_ok=True)
            unzip_to(daily_solieu_zip, daily_dir)
            unzip_to(daily_index_zip, daily_dir)
            normalize_files(daily_dir)

            # Merge per file
            added = merge_daily_into_upto(extract_dir / "CafeF.HSX.csv", daily_dir / "CafeF.HSX.csv", target_iso)
            patch_detail["HSX"] = added
            added = merge_daily_into_upto(extract_dir / "CafeF.HNX.csv", daily_dir / "CafeF.HNX.csv", target_iso)
            patch_detail["HNX"] = added
            added = merge_daily_into_upto(extract_dir / "CafeF.UPCOM.csv", daily_dir / "CafeF.UPCOM.csv", target_iso)
            patch_detail["UPCOM"] = added
            added = merge_daily_into_upto(extract_dir / "CafeF.INDEX.csv", daily_dir / "CafeF.INDEX.csv", target_iso)
            patch_detail["INDEX"] = added

            patched = any(v > 0 for v in patch_detail.values())

    # 4) Build cafef.zip
    cafef_zip = OUT_DIR / "cafef.zip"
    make_zip(extract_dir, cafef_zip)

    # 5) latest.json
    data_date = read_data_date_from_index(extract_dir / "CafeF.INDEX.csv")
    latest = {
        "run_time_gmt7": now_gmt7().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "CafeF ami_data (cafef1.mediacdn.vn)",
        "bundle_pattern_used": bundle.pattern_used,
        "found_package_date": bundle.found_package_date_iso,
        "data_date_from_index": data_date,
        "upto_urls": {
            "solieu_url": bundle.solieu_url,
            "index_url": bundle.index_url,
        },
        "patch_daily": {
            "attempted": bool(need_patch),
            "patched": bool(patched),
            "target_date": target_iso,
            "detail_appended_rows": patch_detail,
        },
        "assets": {
            "cafef_zip": "cafef.zip",
        },
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK: out/cafef.zip, out/latest.json")


if __name__ == "__main__":
    main()
