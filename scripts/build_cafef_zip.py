import io
import re
import json
import zipfile
import shutil
import datetime as dt
from pathlib import Path

import requests

TZ_OFFSET = 7  # GMT+7
BASE_URL = "https://cafef1.mediacdn.vn/data/ami_data"
MAX_BACK_DAYS = 7

OUT_DIR = Path("out")
WORK_DIR = Path("work")

def now_gmt7() -> dt.datetime:
    return dt.datetime.utcnow() + dt.timedelta(hours=TZ_OFFSET)

def ddmmyyyy(d: dt.date) -> str:
    return d.strftime("%d%m%Y")

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def head_ok(url: str, timeout=20) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False

def download(url: str, path: Path, timeout=120):
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)

def find_latest_urls():
    today = now_gmt7().date()
    patterns = [
        ("CafeF.SolieuGD.Upto{d}.zip", "CafeF.Index.Upto{d}.zip"),
        ("CafeF.SolieuGD.{d}.zip", "CafeF.Index.{d}.zip"),
    ]

    for back in range(0, MAX_BACK_DAYS + 1):
        d = today - dt.timedelta(days=back)
        d_str = ddmmyyyy(d)
        folder = yyyymmdd(d)

        for p1, p2 in patterns:
            f1 = p1.format(d=d_str)
            f2 = p2.format(d=d_str)
            u1 = f"{BASE_URL}/{folder}/{f1}"
            u2 = f"{BASE_URL}/{folder}/{f2}"

            if head_ok(u1) and head_ok(u2):
                return {
                    "found_package_date": d.isoformat(),
                    "folder": folder,
                    "solieu_url": u1,
                    "index_url": u2,
                }

    raise RuntimeError("Không tìm thấy bộ file CafeF trong phạm vi lùi 7 ngày.")

def unzip_to(src_zip: Path, dest_dir: Path):
    with zipfile.ZipFile(src_zip, "r") as z:
        z.extractall(dest_dir)

def normalize_files(extract_dir: Path):
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
        shutil.copyfile(cand[0], extract_dir / out_name)

def read_data_date_from_index(index_csv: Path) -> str:
    lines = index_csv.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        return "N/A"
    row1 = lines[1]

    tokens = re.split(r"[,\t;]", row1)
    for t in tokens[:6]:
        t = t.strip()

        if re.match(r"^\d{4}-\d{2}-\d{2}$", t):
            return t

        m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{4})$", t)
        if m:
            dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
            return f"{yyyy}-{mm}-{dd}"

    return "N/A"

def make_zip(src_dir: Path, zip_path: Path):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for name in ["CafeF.HSX.csv", "CafeF.HNX.csv", "CafeF.UPCOM.csv", "CafeF.INDEX.csv"]:
            p = src_dir / name
            if not p.exists():
                raise RuntimeError(f"Thiếu {name} để đóng gói cafef.zip")
            z.write(p, arcname=name)

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    info = find_latest_urls()

    solieu_zip = WORK_DIR / "solieu.zip"
    index_zip = WORK_DIR / "index.zip"
    download(info["solieu_url"], solieu_zip)
    download(info["index_url"], index_zip)

    extract_dir = WORK_DIR / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    unzip_to(solieu_zip, extract_dir)
    unzip_to(index_zip, extract_dir)

    normalize_files(extract_dir)

    cafef_zip = OUT_DIR / "cafef.zip"
    make_zip(extract_dir, cafef_zip)

    data_date = read_data_date_from_index(extract_dir / "CafeF.INDEX.csv")
    latest = {
        "run_time_gmt7": now_gmt7().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "CafeF ami_data (cafef1.mediacdn.vn)",
        "found_package_date": info["found_package_date"],
        "data_date_from_index": data_date,
        "assets": {
            "cafef_zip": "cafef.zip"
        }
    }
    (OUT_DIR / "latest.json").write_text(json.dumps(latest, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK: out/cafef.zip, out/latest.json")

if __name__ == "__main__":
    main()