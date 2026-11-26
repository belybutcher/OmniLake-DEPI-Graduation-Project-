from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.config_loader import load_config
from src.utils.logger import get_logger


class BinanceIngestor:
    def __init__(
        self,
        config_path: str | None = "config/config.yamL",
        chunk_size_bytes: int = 4 * 1024 * 1024,
        max_workers: int = 4,
        extract_csv: bool = True,
        keep_zip: bool = False,
        overwrite: bool = False,
    ):
        self.logger = get_logger("BinanceIngestor")
        self.config = load_config(config_path) if config_path else load_config()

        project_cfg = self.config.get("project", {})
        env = project_cfg.get("environment", "local")
        storage_cfg = self.config.get("storage", {}).get(env, {})
        bronze_raw = storage_cfg.get("bronze", "data_lake/bronze")

        project_root = Path(project_cfg.get("root_path", Path.cwd()))
        bronze_path = Path(bronze_raw)
        self.bronze_path = bronze_path if bronze_path.is_absolute() else (project_root / bronze_path)
        if not str(self.bronze_path).startswith(("abfss://", "s3://", "gs://")):
            self.bronze_path.mkdir(parents=True, exist_ok=True)

        ing_cfg = self.config.get("ingestion", {})
        self.base_url = ing_cfg.get(
            "base_url", "https://data.binance.vision/data/spot/monthly/trades"
        ).rstrip("/")
        self.symbols_cfg: list[str] = list(ing_cfg.get("symbols", ["BTCUSDT"]))
        self.periods_cfg: list[str] = list(ing_cfg.get("periods", ["2026-09"]))
        self.max_retries = int(ing_cfg.get("max_retries", 3))

        self.chunk_size = int(ing_cfg.get("chunk_size_bytes", chunk_size_bytes))
        self.connect_timeout = float(ing_cfg.get("connect_timeout_sec", 15))
        self.read_timeout = float(ing_cfg.get("read_timeout_sec", 120))
        self.backoff_factor = float(ing_cfg.get("backoff_factor_sec", 2.0))
        self.max_workers = int(ing_cfg.get("max_workers", max_workers))
        self.extract_csv = bool(ing_cfg.get("extract_csv", extract_csv))
        self.keep_zip = bool(ing_cfg.get("keep_zip", keep_zip))
        self.overwrite = bool(ing_cfg.get("overwrite", overwrite))

    def _build_session(self, pool_size: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.max_retries,
            connect=self.max_retries,
            read=self.max_retries,
            status=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("HEAD", "GET"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _build_url(self, symbol: str, period: str, granularity: str | None = None) -> str:
        sym = symbol.upper().strip()
        prd = period.strip()
        # يدعم base_url الجاهز مثل .../monthly/trades أو تحويل monthly/daily لو تم تحديد granularity
        if granularity in ("monthly", "daily") and "/monthly/" in self.base_url:
            base = self.base_url.replace("/monthly/", f"/{granularity}/", 1)
        elif granularity in ("monthly", "daily") and "/daily/" in self.base_url:
            base = self.base_url.replace("/daily/", f"/{granularity}/", 1)
        else:
            base = self.base_url
        return f"{base}/{sym}/{sym}-trades-{prd}.zip"

    def _download_stream_atomic(self, session: requests.Session, url: str, dest_zip: Path) -> None:
        dest_zip.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".part_", suffix=".zip", dir=str(dest_zip.parent))
        os.close(fd)
        tmp_path = Path(tmp_name)

        try:
            with session.get(
                url,
                stream=True,
                timeout=(self.connect_timeout, self.read_timeout),
            ) as resp:
                resp.raise_for_status()
                with tmp_path.open("wb") as out_f:
                    for chunk in resp.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            out_f.write(chunk)
                    out_f.flush()
                    os.fsync(out_f.fileno())

            if tmp_path.stat().st_size == 0:
                raise RuntimeError(f"Downloaded empty file from {url}")

            os.replace(tmp_path, dest_zip)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def _extract_zip_atomic(self, zip_path: Path, target_dir: Path, overwrite: bool) -> list[Path]:
        extracted_files: list[Path] = []
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [m for m in zf.infolist() if not m.is_dir()]
            for member in members:
                member_name = Path(member.filename).name
                final_file = target_dir / member_name

                if final_file.exists() and final_file.stat().st_size > 0 and not overwrite:
                    extracted_files.append(final_file)
                    continue

                fd, tmp_name = tempfile.mkstemp(
                    prefix=".part_", suffix=Path(member_name).suffix, dir=str(target_dir)
                )
                os.close(fd)
                tmp_file = Path(tmp_name)

                try:
                    with zf.open(member, "r") as src_f, tmp_file.open("wb") as dst_f:
                        shutil.copyfileobj(src_f, dst_f, length=self.chunk_size)
                        dst_f.flush()
                        os.fsync(dst_f.fileno())
                    os.replace(tmp_file, final_file)
                    extracted_files.append(final_file)
                except Exception:
                    tmp_file.unlink(missing_ok=True)
                    raise
        return extracted_files

    def _ingest_one(
        self,
        session: requests.Session,
        symbol: str,
        period: str,
        granularity: str | None,
        extract_csv: bool,
        keep_zip: bool,
        overwrite: bool,
    ) -> dict:
        sym = symbol.upper().strip()
        prd = period.strip()
        symbol_dir = self.bronze_path / sym
        zip_path = symbol_dir / f"{sym}-trades-{prd}.zip"
        expected_csv = symbol_dir / f"{sym}-trades-{prd}.csv"

        if not overwrite:
            if extract_csv and expected_csv.exists() and expected_csv.stat().st_size > 0:
                self.logger.info(f"[{sym} | {prd}] Skip (already exists): {expected_csv.name}")
                return {"symbol": sym, "period": prd, "status": "skipped", "path": str(expected_csv)}
            if not extract_csv and zip_path.exists() and zip_path.stat().st_size > 0:
                self.logger.info(f"[{sym} | {prd}] Skip (zip already exists): {zip_path.name}")
                return {"symbol": sym, "period": prd, "status": "skipped", "path": str(zip_path)}

        url = self._build_url(sym, prd, granularity)
        t0 = time.perf_counter()
        self.logger.info(f"[{sym} | {prd}] Downloading: {url}")

        self._download_stream_atomic(session, url, zip_path)
        dl_sec = time.perf_counter() - t0

        result_path = zip_path
        if extract_csv:
            extracted = self._extract_zip_atomic(zip_path, symbol_dir, overwrite=overwrite)
            if extracted:
                result_path = extracted[0]
            if not keep_zip and zip_path.exists():
                zip_path.unlink(missing_ok=True)

        total_sec = time.perf_counter() - t0
        size_mb = result_path.stat().st_size / (1024 * 1024) if result_path.exists() else 0.0
        self.logger.info(
            f"[{sym} | {prd}] Done -> {result_path.name} ({size_mb:.2f} MB) | dl={dl_sec:.2f}s, total={total_sec:.2f}s"
        )
        return {
            "symbol": sym,
            "period": prd,
            "status": "ingested",
            "path": str(result_path),
            "size_mb": round(size_mb, 2),
            "elapsed_sec": round(total_sec, 2),
        }

    def fetch_historical_data(
        self,
        symbols: Sequence[str] | None = None,
        periods: Sequence[str] | None = None,
        date: str | None = None,
        granularity: str | None = None,
        extract_csv: bool | None = None,
        keep_zip: bool | None = None,
        overwrite: bool | None = None,
    ) -> list[dict]:
        self.logger.info("Starting ingestion process...")

        target_symbols = list(symbols) if symbols else self.symbols_cfg
        if not target_symbols:
            raise ValueError("No symbols provided or found in config.")

        if date:
            target_periods: Iterable[str] = [date]
        elif periods:
            target_periods = periods
        else:
            target_periods = self.periods_cfg

        period_list = [str(p).strip() for p in target_periods if str(p).strip()]
        if not period_list:
            raise ValueError("No periods or date provided.")

        use_extract = self.extract_csv if extract_csv is None else extract_csv
        use_keep_zip = self.keep_zip if keep_zip is None else keep_zip
        use_overwrite = self.overwrite if overwrite is None else overwrite

        tasks = [(s.upper().strip(), p) for s in target_symbols for p in period_list]
        workers = min(self.max_workers, max(1, len(tasks)))
        session = self._build_session(pool_size=max(workers * 2, 8))
        results: list[dict] = []
        failures: list[str] = []

        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {
                    pool.submit(
                        self._ingest_one,
                        session,
                        sym,
                        prd,
                        granularity,
                        use_extract,
                        use_keep_zip,
                        use_overwrite,
                    ): (sym, prd)
                    for sym, prd in tasks
                }

                for future in as_completed(future_map):
                    sym, prd = future_map[future]
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        msg = f"{sym} ({prd}): {exc}"
                        self.logger.error(f"Ingestion failed for {msg}")
                        failures.append(msg)
        finally:
            session.close()

        if failures:
            raise RuntimeError(f"Ingestion completed with {len(failures)} error(s): " + " | ".join(failures))

        self.logger.info(f"Ingestion finished successfully ({len(results)} task(s) processed).")
        return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance Vision Bronze Historical Ingestor")
    parser.add_argument("--config", type=str, default="config/config.yamL", help="Path to config YAML")
    parser.add_argument("--symbols", nargs="+", default=None, help="Override symbols e.g. BTCUSDT ETHUSDT")
    parser.add_argument("--periods", nargs="+", default=None, help="Override periods e.g. 2026-09 2026-10")
    parser.add_argument("--date", type=str, default=None, help="Single period/date e.g. 2024-01 or 2024-01-30")
    parser.add_argument("--granularity", choices=["monthly", "daily"], default=None)
    parser.add_argument("--no-extract", action="store_true", help="Keep ZIP without extracting CSV")
    parser.add_argument("--keep-zip", action="store_true", help="Keep ZIP after extracting CSV")
    parser.add_argument("--overwrite", action="store_true", help="Re-download and overwrite existing files")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        ingestor = BinanceIngestor(config_path=args.config)
        ingestor.fetch_historical_data(
            symbols=args.symbols,
            periods=args.periods,
            date=args.date,
            granularity=args.granularity,
            extract_csv=False if args.no_extract else None,
            keep_zip=True if args.keep_zip else None,
            overwrite=True if args.overwrite else None,
        )
        sys.exit(0)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
