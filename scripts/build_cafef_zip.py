#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_cafef_zip.py

Yêu cầu bạn chốt:
1) Xác định ngày giao dịch cuối cùng bằng web (probe URL CafeF theo ngày).
2) Tải tất cả file Upto và theo ngày giao dịch cuối về.
3) Kiểm tra ngày trong file Upto: nếu nhỏ hơn ngày giao dịch cuối -> ghép nội dung ngày đó từ file theo ngày vào Upto.
   - GHÉP NGAY SAU DÒNG HEADER (không chèn cuối file)
   - chống trùng theo (ticker, date) hoặc (date) tùy cấu trúc.
4) Đổi tên Upto theo chuẩn: CafeF.HSX.csv / CafeF.HNX.csv / CafeF.UPCOM.csv / CafeF.INDEX.csv
5) Tạo out/cafef.zip gồm các file đã đổi tên + (optional) kèm 2 zip theo ngày + latest.json
   Đồng thời xuất 4 CSV + latest.json ra out/ để GitHub Pages publish (ChatGPT đọc chắc).
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

# ===== CONFIG =====
TZ_OFFSET = 7  # GMT+7
BASE_URL = "https://cafef1.mediacdn.vn/data/ami_data"

PROBE_BACK_DAYS = 20   # tìm ngày giao dịch cuối bằng web (lùi tối đa)
UPTO_BACK_DAYS = 10    # tìm bộ Upto mới nhất (lùi tối đa)

OUT_DIR = Path("out")
WORK_DIR = Path("work")

TIMEOUT_HEAD = 20
TIMEOUT_GET = 240


# ===== TIME HELPERS =====
def now_gmt7() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=TZ_OFFSET)


def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")


def iso(d: dt.date) -> str:
    return d.isoformat()


# ===== NETWORK =====
def head_ok(url: str) -> bool:
    try:
        r = requests.head(url, timeout=TIMEOUT_HEAD, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def download(url: str, out_path: Path) -> None:
    r = requests.get(url, timeout=TIMEOUT_GET, stream=True, allow_redirects=True)
    r.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)


# ===== URL BUILDERS =====
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
    kind: str  # "upto" or "daily-fallback"


def build_daily_pair(d: dt.date) -> DailyPair:
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return DailyPair(date_iso=iso(d), folder=folder, solieu_url=solieu, index_url=index)


def build_upto_urls(d: dt.date) -> Tuple[str, str]:
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.Upto{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.Upto{d_str}.zip"
    return solieu, index


def build_daily_urls(d: dt.date) -> Tuple[str, str]:
    folder = yyyymmdd(d)
    d_str = ddmmyyyy(d)
    solieu = f"{BASE_URL}/{folder}/CafeF.SolieuGD.{d_str}.zip"
    index = f"{BASE_URL}/{folder}/CafeF.Index.{d_str}.zip"
    return solieu, index


# ===== STEP 1: FIND LAST TRADE DATE BY WEB =====
def probe_last_trade_date() -> DailyPair:
    """
    Chốt ngày giao dịch cuối bằng web:
    lùi từ hôm nay (GMT+7) cho đến khi tồn tại đủ 2 file daily: SolieuGD + Index.
    """
    today = now_gmt7().date()
    for back in range(0, PROBE_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)
        pair = build_daily_pair(d)
        if head_ok(pair.solieu_url) and head_ok(pair.index_url):
            return pair
    raise RuntimeError(
        f"Không tìm được ngày giao dịch cuối trong phạm vi lùi {PROBE_BACK_DAYS} ngày."
    )


# ===== STEP 2: FIND LATEST UPTO =====
def find_latest_upto() -> UptoPair:
    """
    Tìm bộ Upto mới nhất; nếu không có thì fallback sang daily (để vẫn tạo baseline).
    """
    today = now_gmt7().date()
    for back in range(0, UPTO_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)

        u_sol, u_idx = build_upto_urls(d)
        if head_ok(u_sol) and head_ok(u_idx):
            return UptoPair(date_iso=iso(d), folder=yyyymmdd(d), solieu_url=u_sol, index_url=u_idx, kind="upto")

        d_sol, d_idx = build_daily_urls(d)
        if head_ok(d_sol) and head_ok(d_idx):
            return UptoPair(date_iso=iso(d), folder=yyyymmdd(d), solieu_url=d_sol, index_url=d_idx, kind="daily-fallback")

    raise RuntimeError(f"Không tìm thấy Upto trong phạm vi lùi {UPTO_BACK_DAYS} ngày.")


# ===== ZIP/CSV NORMALIZE =====
def unzip_to(zip_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)


def pick_best_csv(csv_files: Sequence[Path], key: str) -> Path:
    key_u = key.upper()
    candidates: List[Path] = []

    for f in csv_files:
        name = f.name.upper()
        if f".{key_u}." in name or f".{key_u}_" in name or name.endswith(f"{key_u}.CSV"):
            candidates.append(f)

    if not candidates:
        candidates = [f for f in csv_files if key_u in f.name.upper()]

    if not candidates:
        raise RuntimeError(f"Thiếu CSV cho {key}")

    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]


def normalize_4_csv(extract_dir: Path) -> Dict[str, Path]:
    """
    Chuẩn hoá về đúng 4 file:
      CafeF.HSX.csv, CafeF.HNX.csv, CafeF.UPCOM.csv, CafeF.INDEX.csv
    """
    csvs = list(extract_dir.rglob("*.csv"))
    if not csvs:
        raise RuntimeError("Không thấy CSV sau khi giải nén.")

    out_map = {
        "HSX": extract_dir / "CafeF.HSX.csv",
        "HNX": extract_dir / "CafeF.HNX.csv",
        "UPCOM": extract_dir / "CafeF.UPCOM.csv",
        "INDEX": extract_dir / "CafeF.INDEX.csv",
    }

    for k, outp in out_map.items():
        src = pick_best_csv(csvs, k)
        shutil.copyfile(src, outp)

    return out_map


# ===== DATE PARSE + MERGE =====
def parse_date_token(tok: str) -> Optional[str]:
    tok = tok.strip().strip('"')
    if not tok:
        return None

    # YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", tok):
        return tok

    # DD/MM/YYYY or DD-MM-YYYY
    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", tok)
    if m:
        dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    # YYYYMMDD
    if re.fullmatch(r"\d{8}", tok):
        yyyy, mm, dd = int(tok[0:4]), int(tok[4:6]), int(tok[6:8])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    # YYYYMMDDHHMMSS -> YYYY-MM-DD
    if re.fullmatch(r"\d{14}", tok):
        yyyy, mm, dd = int(tok[0:4]), int(tok[4:6]), int(tok[6:8])
        if 1 <= dd <= 31 and 1 <= mm <= 12:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

    return None


KeyType = Union[Tuple[str, str], Tuple[str]]  # (TICKER, DATE) or (DATE,)


def line_key(line: str) -> Optional[KeyType]:
    parts = [p.strip() for p in line.split(",")]
    if not parts:
        return None

    # phổ biến: ticker col0, date col1
    if len(parts) >= 2:
        d = parse_date_token(parts[1])
        if d:
            t = parts[0].upper()
            if re.fullmatch(r"[A-Z0-9\.\-_]{1,20}", t):
                return (t, d)

    # date ở col0
    d0 = parse_date_token(parts[0])
    if d0:
        return (d0,)

    # fallback tìm date trong 3 cột đầu
    for p in parts[:3]:
        di = parse_date_token(p)
        if di:
            return (di,)

    return None


def csv_has_date(csv_path: Path, target_iso: str) -> bool:
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for ln in f:
            if first:
                first = False
                continue
            k = line_key(ln)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if d == target_iso:
                return True
    return False


def collect_date_lines(csv_path: Path, target_iso: str) -> List[str]:
    lines: List[str] = []
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for ln in f:
            if first:
                first = False
                continue
            k = line_key(ln)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if d == target_iso:
                lines.append(ln.rstrip("\n"))
    return lines


def insert_after_header(csv_path: Path, new_lines: Sequence[str]) -> int:
    """
    Chèn new_lines NGAY SAU HEADER, chống trùng theo key.
    """
    if not new_lines:
        return 0

    all_lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not all_lines:
        raise RuntimeError(f"File rỗng: {csv_path}")

    header = all_lines[0]
    body = all_lines[1:]

    existing: Set[KeyType] = set()
    for ln in body:
        k = line_key(ln)
        if k:
            existing.add(k)

    to_add: List[str] = []
    for ln in new_lines:
        k = line_key(ln)
        if not k:
            continue
        if k in existing:
            continue
        existing.add(k)
        to_add.append(ln)

    if not to_add:
        return 0

    merged = [header] + to_add + body
    csv_path.write_text("\n".join(merged) + "\n", encoding="utf-8")
    return len(to_add)


def max_date(csv_path: Path) -> Optional[str]:
    mx: Optional[str] = None
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        first = True
        for ln in f:
            if first:
                first = False
                continue
            k = line_key(ln)
            if not k:
                continue
            d = k[1] if len(k) == 2 else k[0]
            if mx is None or d > mx:
                mx = d
    return mx


# ===== MAIN =====
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    run_time = now_gmt7()

    # 1) Ngày giao dịch cuối (probe web)
    last = probe_last_trade_date()
    target_iso = last.date_iso
    target_date = dt.date.fromisoformat(target_iso)

    # 2) Tải Upto
    upto = find_latest_upto()
    upto_sol_zip = WORK_DIR / "upto_solieu.zip"
    upto_idx_zip = WORK_DIR / "upto_index.zip"
    download(upto.solieu_url, upto_sol_zip)
    download(upto.index_url, upto_idx_zip)

    # 3) Giải nén Upto -> normalize 4 CSV
    upto_dir = WORK_DIR / "upto_extract"
    unzip_to(upto_sol_zip, upto_dir)
    unzip_to(upto_idx_zip, upto_dir)
    upto_csv = normalize_4_csv(upto_dir)

    # 4) Tải bộ theo ngày giao dịch cuối (2 zip daily) -> normalize 4 CSV
    daily_sol_zip = WORK_DIR / f"CafeF.SolieuGD.{ddmmyyyy(target_date)}.zip"
    daily_idx_zip = WORK_DIR / f"CafeF.Index.{ddmmyyyy(target_date)}.zip"
    download(last.solieu_url, daily_sol_zip)
    download(last.index_url, daily_idx_zip)

    daily_dir = WORK_DIR / "daily_extract"
    unzip_to(daily_sol_zip, daily_dir)
    unzip_to(daily_idx_zip, daily_dir)
    daily_csv = normalize_4_csv(daily_dir)

    # 5) Nếu Upto thiếu ngày target -> ghép (insert ngay sau header)
    patch_report: Dict[str, Dict[str, object]] = {}
    for k in ["HSX", "HNX", "UPCOM", "INDEX"]:
        u = upto_csv[k]
        d = daily_csv[k]

        before_max = max_date(u)
        had = csv_has_date(u, target_iso)
        inserted = 0

        if not had:
            lines = collect_date_lines(d, target_iso)
            inserted = insert_after_header(u, lines)
            had = csv_has_date(u, target_iso)

        patch_report[k] = {
            "max_before": before_max,
            "max_after": max_date(u),
            "target_date": target_iso,
            "had_target_after": bool(had),
            "inserted_lines": int(inserted),
        }

    # 6) Xuất 4 CSV chuẩn ra out/ (text endpoint cho Pages/ChatGPT)
    for k in ["HSX", "HNX", "UPCOM", "INDEX"]:
        shutil.copyfile(upto_csv[k], OUT_DIR / f"CafeF.{k}.csv")

    # 7) latest.json
    latest = {
        "run_time_gmt7": run_time.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": "GMT+7",
        "last_trade_date_web_probe": target_iso,
        "upto_bundle": {
            "found_date": upto.date_iso,
            "kind": upto.kind,
            "solieu_url": upto.solieu_url,
            "index_url": upto.index_url,
        },
        "daily_pair": {
            "date": target_iso,
            "solieu_url": last.solieu_url,
            "index_url": last.index_url,
        },
        "patch_report": patch_report,
        "outputs": {
            "pages_csv": [
                "CafeF.HSX.csv",
                "CafeF.HNX.csv",
                "CafeF.UPCOM.csv",
                "CafeF.INDEX.csv",
                "latest.json",
            ],
            "zip": "cafef.zip",
        },
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    # 8) Tạo cafef.zip (tùy chọn giữ binary cho người dùng/PC)
    zip_out = OUT_DIR / "cafef.zip"
    if zip_out.exists():
        zip_out.unlink()

    with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # 4 CSV chuẩn
        for k in ["HSX", "HNX", "UPCOM", "INDEX"]:
            p = OUT_DIR / f"CafeF.{k}.csv"
            z.write(p, arcname=p.name)

        # kèm latest.json
        z.write(OUT_DIR / "latest.json", arcname="latest.json")

        # kèm 2 zip daily (audit)
        z.write(daily_sol_zip, arcname=daily_sol_zip.name)
        z.write(daily_idx_zip, arcname=daily_idx_zip.name)

    print("OK: out/ generated")
    print("last_trade_date_web_probe:", target_iso)
    print("patch_report:", json.dumps(patch_report, ensure_ascii=False))


if __name__ == "__main__":
    main()
