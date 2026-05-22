from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List


SAMPLES = ("100", "200", "400", "1000", "full")
GATE_SAMPLES = ("100", "200", "400", "1000")


@dataclass
class StrategyRow:
    variant_key: str
    variant_label: str
    band_group: str
    source_report: str
    source_family: str
    full: Dict[str, str]
    samples: Dict[str, Dict[str, str]]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _to_float(row: Dict[str, str], key: str) -> float:
    try:
        return float(row[key])
    except Exception:
        return 0.0


def _winner_midband_rows(report_path: Path) -> List[StrategyRow]:
    rows = _read_csv(report_path)
    grouped: Dict[str, Dict[str, Dict[str, str]]] = {}
    labels: Dict[str, str] = {}
    for row in rows:
        label = row["variant_label"]
        if "close<=100" in label:
            continue
        if "0.45-0.60" in label:
            band_group = "core_0.45_0.60"
        elif "0.45-0.70" in label:
            band_group = "extension_0.45_0.70"
        else:
            continue
        key = row["variant_key"]
        labels[key] = label
        grouped.setdefault(key, {})
        grouped[key][row["sample_size"]] = row | {"band_group": band_group}

    survivors: List[StrategyRow] = []
    for key, sample_rows in grouped.items():
        if not all(sample in sample_rows for sample in SAMPLES):
            continue
        if not all(_to_float(sample_rows[sample], "win_rate_pct") > 55.0 for sample in GATE_SAMPLES):
            continue
        full = sample_rows["full"]
        survivors.append(
            StrategyRow(
                variant_key=key,
                variant_label=labels[key],
                band_group=full["band_group"],
                source_report=report_path.name,
                source_family="winner_midband",
                full=full,
                samples=sample_rows,
            )
        )

    # Prefer the exact requested zone before wider extensions, then rank by 1000-window PnL.
    survivors.sort(
        key=lambda item: (
            1 if item.band_group == "core_0.45_0.60" else 0,
            _to_float(item.samples["1000"], "total_pnl_usd"),
            _to_float(item.samples["full"], "win_rate_pct"),
            -_to_float(item.samples["1000"], "trade_rate_pct"),
        ),
        reverse=True,
    )
    return survivors


def _best_row(rows: Iterable[Dict[str, str]], sort_key: str = "total_pnl_usd") -> Dict[str, str] | None:
    best = None
    best_score = None
    for row in rows:
        score = _to_float(row, sort_key)
        if best is None or score > best_score:
            best = row
            best_score = score
    return best


def _family_check(repo_root: Path) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []

    midband_path = repo_root / "reports" / "winner_midband_search_sheet.csv"
    survivors = _winner_midband_rows(midband_path)
    best_core = next((row for row in survivors if row.band_group == "core_0.45_0.60"), None)
    best_ext = next((row for row in survivors if row.band_group == "extension_0.45_0.70"), None)
    out.append(
        {
            "family": "winner_midband",
            "status": "PASS",
            "zone_fit": "Exact 0.45-0.60 core band plus 0.45-0.70 extension",
            "best_core_variant": best_core.variant_label if best_core else "",
            "best_core_1000_wr": best_core.samples["1000"]["win_rate_pct"] if best_core else "",
            "best_core_full_wr": best_core.samples["full"]["win_rate_pct"] if best_core else "",
            "best_core_1000_pnl": best_core.samples["1000"]["total_pnl_usd"] if best_core else "",
            "notes": "Repo-native start-window winner-side family; this is the only existing repo search that directly targets the requested mid-price zone.",
        }
    )
    if best_ext:
        out.append(
            {
                "family": "winner_midband_extension",
                "status": "PASS",
                "zone_fit": "0.45-0.70 extension",
                "best_core_variant": best_ext.variant_label,
                "best_core_1000_wr": best_ext.samples["1000"]["win_rate_pct"],
                "best_core_full_wr": best_ext.samples["full"]["win_rate_pct"],
                "best_core_1000_pnl": best_ext.samples["1000"]["total_pnl_usd"],
                "notes": "Useful wider-band extension, but less aligned than the 0.45-0.60 core ask.",
            }
        )

    dominant_rows = _read_csv(repo_root / "reports" / "dominant_side_realistic_sheet.csv")
    dominant_best = _best_row((r for r in dominant_rows if r["sample_size"] == "1000"))
    out.append(
        {
            "family": "dominant_side_realistic",
            "status": "REJECT",
            "zone_fit": "Mostly 0.30-0.45",
            "best_core_variant": dominant_best["variant_label"] if dominant_best else "",
            "best_core_1000_wr": dominant_best["win_rate_pct"] if dominant_best else "",
            "best_core_full_wr": "",
            "best_core_1000_pnl": dominant_best["total_pnl_usd"] if dominant_best else "",
            "notes": "Existing repo family, but it searches below the requested 0.4-0.6 zone and trade counts are much thinner.",
        }
    )

    live_rows = _read_csv(repo_root / "reports" / "signal_strategy_compare_delay2_sheet.csv")
    by_strategy: Dict[str, Dict[str, Dict[str, str]]] = {}
    for row in live_rows:
        by_strategy.setdefault(row["strategy_key"], {})[row["sample_size"]] = row
    for strategy_key, sample_rows in sorted(by_strategy.items()):
        full_row = sample_rows.get("full")
        row_1000 = sample_rows.get("1000")
        if not full_row or not row_1000:
            continue
        status = "PASS" if all(
            sample in sample_rows and _to_float(sample_rows[sample], "win_rate_pct") > 55.0
            for sample in GATE_SAMPLES
        ) else "REJECT"
        out.append(
            {
                "family": f"live_compare::{strategy_key}",
                "status": status if strategy_key == "reclaim" else "REJECT",
                "zone_fit": "Cheap-side <=0.30 family",
                "best_core_variant": full_row["strategy_label"],
                "best_core_1000_wr": row_1000["win_rate_pct"],
                "best_core_full_wr": full_row["win_rate_pct"],
                "best_core_1000_pnl": row_1000["total_pnl_usd"],
                "notes": "Existing repo live-rule comparison family; outside the requested 0.4-0.6 entry zone.",
            }
        )
    return out


def _best_realistic_rows(strategies: List[StrategyRow]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for rank, item in enumerate(strategies, start=1):
        row: Dict[str, str] = {
            "rank": str(rank),
            "variant_key": item.variant_key,
            "variant_label": item.variant_label,
            "band_group": item.band_group,
            "source_family": item.source_family,
            "source_report": item.source_report,
            "report_bucket": "core" if item.band_group == "core_0.45_0.60" else "extension",
            "full_windows": item.full["windows"],
            "full_trades": item.full["trades"],
            "full_trade_rate_pct": item.full["trade_rate_pct"],
            "full_win_rate_pct": item.full["win_rate_pct"],
            "full_total_pnl_usd": item.full["total_pnl_usd"],
            "full_avg_entry_px": item.full["avg_entry_px"],
            "notes": item.full["notes"],
        }
        for sample in SAMPLES:
            sample_row = item.samples[sample]
            row[f"wr_{sample}"] = sample_row["win_rate_pct"]
            row[f"pnl_{sample}"] = sample_row["total_pnl_usd"]
            row[f"trades_{sample}"] = sample_row["trades"]
            row[f"trade_rate_{sample}"] = sample_row["trade_rate_pct"]
            row[f"avg_entry_{sample}"] = sample_row["avg_entry_px"]
        out.append(row)
    return out


def _write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise SystemExit(f"No rows to write for {path}")
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_xlsx(path: Path, sheets: Dict[str, List[Dict[str, str]]]) -> None:
    try:
        from openpyxl import Workbook
    except Exception:
        return

    wb = Workbook()
    first = True
    for sheet_name, rows in sheets.items():
        ws = wb.active if first else wb.create_sheet(sheet_name)
        ws.title = sheet_name
        first = False
        if not rows:
            continue
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row.get(header, "") for header in headers])
        ws.freeze_panes = "A2"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    repo_root = _repo_root()
    report_dir = repo_root / "reports"

    strategies = _winner_midband_rows(report_dir / "winner_midband_search_sheet.csv")
    best_rows = _best_realistic_rows(strategies)
    family_rows = _family_check(repo_root)

    csv_path = report_dir / "repo_start_window_best_realistic.csv"
    xlsx_path = report_dir / "repo_start_window_best_realistic.xlsx"
    family_csv_path = report_dir / "repo_start_window_family_check.csv"

    _write_csv(csv_path, best_rows)
    _write_csv(family_csv_path, family_rows)
    _write_xlsx(
        xlsx_path,
        {
            "best_realistic": best_rows,
            "family_check": family_rows,
        },
    )

    core = next((row for row in best_rows if row["report_bucket"] == "core"), None)
    ext = next((row for row in best_rows if row["report_bucket"] == "extension"), None)
    print(f"Wrote {csv_path}")
    print(f"Wrote {family_csv_path}")
    print(f"Wrote {xlsx_path}")
    if core:
        print(
            "Best core:",
            core["variant_key"],
            core["variant_label"],
            f"1000 wr={core['wr_1000']} pnl={core['pnl_1000']}",
            f"full wr={core['full_win_rate_pct']} pnl={core['full_total_pnl_usd']}",
        )
    if ext:
        print(
            "Best extension:",
            ext["variant_key"],
            ext["variant_label"],
            f"1000 wr={ext['wr_1000']} pnl={ext['pnl_1000']}",
            f"full wr={ext['full_win_rate_pct']} pnl={ext['full_total_pnl_usd']}",
        )


if __name__ == "__main__":
    main()
