#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/build_cafef_zip.py

Mục tiêu:
1) Chốt NGÀY GIAO DỊCH CUỐI CÙNG bằng web (probe URL CafeF theo ngày).
2) Tải bộ Upto mới nhất + tải bộ theo NGÀY GIAO DỊCH CUỐI CÙNG.
3) Giải nén Upto -> chuẩn hoá 4 CSV (HSX/HNX/UPCOM/INDEX). Kiểm tra ngày max trong từng file.
   Nếu < ngày giao dịch cuối cùng: lấy dữ liệu ngày đó từ bộ theo ngày và GHÉP vào (insert ngay sau header).
4) Đổi tên file theo chuẩn: CafeF.HSX.csv / CafeF.HNX.csv / CafeF.UPCOM.csv / CafeF.INDEX.csv
5) Tạo out/cafef.zip gồm: 4 CSV chuẩn + 2 zip theo ngày (SolieuGD + Index) + latest.json.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

import requests

# ====== Config ======
TZ_OFFSET = 7  # GMT+7
BASE_URL = "https://cafef1.mediacdn.vn/data/ami_data"
PROBE_BACK_DAYS = 20  # số ngày lùi tối đa để chốt ngày giao dịch cuối cùng
UPTO_BACK_DAYS = 10   # số ngày lùi tối đa để tìm bộ Upto

OUT_DIR = Path("out")
WORK_DIR = Path("work")

# ====== Time helpers ======
def now_gmt7() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=TZ_OFFSET)

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")

def iso(d: dt.date) -> str:
    return d.isoformat()

# ====== Network helpers ======
def head_status(url: str, timeout: int = 20) -> Optional[int]:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code
    except Exception:
        return None

def exists(url: str) -> bool:
    return head_status(url) == 200

def download(url: str, path: Path, timeout: int = 180) -> None:
    r = requests.get(url, timeout=timeout, stream=True, allow_redirects=True)
    r.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)

# ====== CafeF URL builders ======
@dataclass(frozen=True)
class DailyPair:
    date_iso: str
    folder: str
    solieu_url: str
    index_url: str

@dataclass(frozen=True)
class UptoPair:
    date_iso: str
    folder: str
    solieu_url: str
    index_url: str
    kind: str  # "upto" or "daily" fallback

def build_daily_urls(d: dt.date) -> DailyPair:
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return DailyPair(date_iso=iso(d), folder=folder, solieu_url=solieu, index_url=index)

def build_upto_urls(d: dt.date, kind: str) -> Tuple[str, str]:
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    if kind == "upto":
        solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.Upto{d_str}.zip"
        index = f"{BASE_URL}/{folder}/CafeF.Index.Upto{d_str}.zip"
    else:
        solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
        index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return solieu, index

# ====== Step 1: Determine last trade date by web ======
def probe_last_trade_date() -> DailyPair:
    """Chốt ngày giao dịch cuối cùng: lùi dần từ hôm nay GMT+7 cho đến khi tồn tại đủ 2 zip theo ngày."""
    today = now_gmt7().date()
    for back in range(0, PROBE_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)
        pair = build_daily_urls(d)
        if exists(pair.solieu_url) and exists(pair.index_url):
            return pair
    raise RuntimeError(
        f"Không chốt được ngày giao dịch cuối cùng trong phạm vi lùi {PROBE_BACK_DAYS} ngày "
        f"(không thấy đủ CafeF.SolieuGD.DDMMYYYY.zip và CafeF.Index.DDMMYYYY.zip)."
    )

# ====== Step 2: Find latest Upto bundle ======
def find_latest_upto_bundle() -> UptoPair:
    """Ưu tiên tìm Upto; nếu không có thì fallback daily (nhưng vẫn dùng để tạo baseline)."""
    today = now_gmt7().date()

    for back in range(0, UPTO_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)

        solieu_u, index_u = build_upto_urls(d, "upto")
        if exists(solieu_u) and exists(index_u):
            return UptoPair(date_iso=iso(d), folder=yyyymmdd(d), solieu_url=solieu_u, index_url=index_u, kind="upto")

        solieu_d, index_d = build_upto_urls(d, "daily")
        if exists(solieu_d) and exists(index_d):
            return UptoPair(date_iso=iso(d), folder=yyyymmdd(d), solieu_url=solieu_d, index_url=index_d, kind="daily")

    raise RuntimeError(f"Không tìm thấy bộ Upto (hoặc fallback daily) trong phạm vi lùi {UPTO_BACK_DAYS} ngày.")

# ====== Zip / CSV helpers ======
def unzip_to(src_zip: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(src_zip, "r") as z:
        z.extractall(dest_dir)

def pick_largest_matching_csv(all_csv: Sequence[Path], key: str) -> Path:
    key_u = key.upper()
    candidates: List[Path] = []

    for f in all_csv:
        name = f.name.upper()
        if f".{key_u}." in name or f".{key_u}_" in name or name.endswith(f"{key_u}.CSV"):
            candidates.append(f)

    if not candidates:
        candidates = [f for f in all_csv if key_u in f.name.upper()]

    if not candidates:
        raise RuntimeError(f"Thiếu CSV cho {key} trong zip.")

    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]

def normalize_to_4_csv(extract_dir: Path) -> Dict[str, Path]:
    """Trả về mapping key->path của 4 file chuẩn trong extract_dir."""
    mapping = {
        "HSX": extract_dir / "CafeF.HSX.csv",
        "HNX": extract_dir / "CafeF.HNX.csv",
        "UPCOM": extract_dir / "CafeF.UPCOM.csv",
        "INDEX": extract_dir / "CafeF.INDEX.csv",
    }

    all_csv = list(extract_dir.rglob("*.csv"))
    if not all_csv:
        raise RuntimeError("Không thấy CSV sau khi giải nén.")

    for key, out_path in mapping.items():
        src = pick_largest_matching_csv(all_csv, key)
        shutil.copyfile(src, out_path)

    return mapping

# ====== Date parsing / merging ======
def parse_any_date_token(tok: str) -> Optional[str]:
    tok = tok.strip()
    if not tok:
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}$", tok):
        return tok

    m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{4})$", tok)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}"

    if re.match(r"^\d{8}$", tok):
        yyyy, mm, dd = tok[0:4], tok[4:6], tok[6:8]
        return f"{yyyy}-{mm}-{dd}"

    return None

KeyType = Union[Tuple[str, str], Tuple[str]]  # (TICKER, ISO_DATE) or (ISO_DATE,)

def extract_key_from_line(line: str) -> Optional[KeyType]:
    parts = [p.strip() for p in line.split(",")]
    if not parts:
        return None

    # common: ticker col0, date col1
    if len(parts) >= 2:
        d = parse_any_date_token(parts[1])
        if d and re.match(r"^[A-Za-z0-9\.\-_]{1,20}$", parts[0]):
            return (parts[0].upper(), d)

    # date in col0
    d0 = parse_any_date_token(parts[0])
    if d0:
        return (d0,)

    for i in range(min(3, len(parts))):
        di = parse_any_date_token(parts[i])
        if di:
            return (di,)

    return None

def max_date_in_csv(csv_path: Path) -> Optional[str]:
    if not csv_path.exists():
        return None
    max_iso: Optional[str] = None
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for line in f:
            if first:
                first = False
                continue
            k = extract_key_from_line(line)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if (max_iso is None) or (d > max_iso):
                max_iso = d
    return max_iso

def csv_contains_date(csv_path: Path, target_iso: str) -> bool:
    if not csv_path.exists():
        return False
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for line in f:
            if first:
                first = False
                continue
            k = extract_key_from_line(line)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if d == target_iso:
                return True
    return False

def collect_lines_for_date(csv_path: Path, target_iso: str) -> List[str]:
    """Lấy các dòng dữ liệu (không lấy header) có date == target_iso."""
    lines: List[str] = []
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for line in f:
            if first:
                first = False
                continue
            k = extract_key_from_line(line)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if d == target_iso:
                lines.append(line.rstrip("\n"))
    return lines

def insert_after_header_dedup(csv_path: Path, new_lines: Sequence[str]) -> int:
    """Chèn new_lines ngay sau header; chống trùng theo key. Trả về số dòng thực sự chèn."""
    if not new_lines:
        return 0

    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        all_lines = [ln.rstrip("\n") for ln in f]

    if not all_lines:
        raise RuntimeError(f"File rỗng: {csv_path}")

    header = all_lines[0]
    body = all_lines[1:]

    existing: Set[KeyType] = set()
    for ln in body:
        k = extract_key_from_line(ln)
        if k:
            existing.add(k)

    to_insert: List[str] = []
    for ln in new_lines:
        k = extract_key_from_line(ln)
        if not k:
            continue
        if k in existing:
            continue
        existing.add(k)
        to_insert.append(ln)

    if not to_insert:
        return 0

    merged = [header] + to_insert + body

    with open(csv_path, "w", encoding="utf-8", newline="\n") as f:
        for ln in merged:
            f.write(ln + "\n")

    return len(to_insert)

# ====== Main ======
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    run_time = now_gmt7()

    # 1) Chốt ngày giao dịch cuối cùng bằng web
    last = probe_last_trade_date()
    target_iso = last.date_iso
    target_date = dt.date.fromisoformat(target_iso)

    # 2) Tải Upto (baseline)
    upto = find_latest_upto_bundle()

    upto_zip1 = WORK_DIR / "upto_solieu.zip"
    upto_zip2 = WORK_DIR / "upto_index.zip"
    download(upto.solieu_url, upto_zip1)
    download(upto.index_url, upto_zip2)

    # 3) Giải nén và chuẩn hoá 4 CSV từ Upto
    extract_dir = WORK_DIR / "extract"
    unzip_to(upto_zip1, extract_dir)
    unzip_to(upto_zip2, extract_dir)
    norm = normalize_to_4_csv(extract_dir)

    # 4) Tải bộ theo ngày (ngày giao dịch cuối) để bổ sung nếu thiếu
    daily_zip1 = WORK_DIR / f"CafeF.SolieuGD.{ddmmyyyy(target_date)}.zip"
    daily_zip2 = WORK_DIR / f"CafeF.Index.{ddmmyyyy(target_date)}.zip"
    download(last.solieu_url, daily_zip1)
    download(last.index_url, daily_zip2)

    daily_extract = WORK_DIR / "daily_extract"
    unzip_to(daily_zip1, daily_extract)
    unzip_to(daily_zip2, daily_extract)
    daily_norm = normalize_to_4_csv(daily_extract)

    # 5) Bổ sung nếu thiếu (insert ngay sau header)
    patch_report: Dict[str, Dict[str, Union[str, int, bool, None]]] = {}
    for key, csv_path in norm.items():
        has_target = csv_contains_date(csv_path, target_iso)
        inserted = 0
        if not has_target:
            new_lines = collect_lines_for_date(daily_norm[key], target_iso)
            inserted = insert_after_header_dedup(csv_path, new_lines)
            has_target = csv_contains_date(csv_path, target_iso)

        patch_report[key] = {
            "max_date_after": max_date_in_csv(csv_path),
            "had_target": bool(has_target),
            "inserted_lines": int(inserted),
        }

    # 6) Copy 4 CSV chuẩn ra out/ (để Pages/ChatGPT đọc)
    out_csv_paths: Dict[str, Path] = {}
    for _, src in norm.items():
        dst = OUT_DIR / src.name
        shutil.copyfile(src, dst)
        out_csv_paths[src.name] = dst

    # 7) latest.json + cafef.zip
    latest = {
        "run_time_gmt7": run_time.strftime("%Y-%m-%d %H:%M:%S"),
        "last_trade_date_web_probe": target_iso,
        "upto_bundle_date": upto.date_iso,
        "upto_bundle_kind": upto.kind,
        "urls": {
            "upto_solieu": upto.solieu_url,
            "upto_index": upto.index_url,
            "daily_solieu": last.solieu_url,
            "daily_index": last.index_url,
        },
        "patch_report": patch_report,
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    zip_out = OUT_DIR / "cafef.zip"
    if zip_out.exists():
        zip_out.unlink()

    with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in out_csv_paths.values():
            z.write(p, arcname=p.name)
        z.write(daily_zip1, arcname=daily_zip1.name)
        z.write(daily_zip2, arcname=daily_zip2.name)
        z.write(OUT_DIR / "latest.json", arcname="latest.json")

    print("OK")
    print("last_trade_date_web_probe:", target_iso)
    print("out:", zip_out.as_posix())

if __name__ == "__main__":
    main()
