"""Daily report: today's ranked list as CSV and a small HTML page (ADR-029).

    python -m jobpipe.report                       # from data/warehouse.duckdb
    python -m jobpipe.report --top 50

Writes, for the latest fetch date in the warehouse (the scoring "as of" date,
ADR-025), not the wall clock:

    data/reports/dt=<YYYY-MM-DD>/top_matches.csv
    data/reports/dt=<YYYY-MM-DD>/top_matches.html

Same partition convention as the raw layer, so re-running on the same day
overwrites (idempotent), and each day's list is kept for comparison.

It only READS the warehouse (read_only=True). Building the tables is dbt's job;
this step just presents them. Run it after `dbt build` succeeds.
"""
from __future__ import annotations

import argparse
import csv
import html
import io
import logging
import os
import sys
from pathlib import Path

import duckdb

log = logging.getLogger("jobpipe.report")

COLUMNS = [
    "rank", "score", "is_new", "company_slug", "title", "location_match", "seniority",
    "years_min", "skill_score", "experience_score", "level_score", "freshness_score",
    "matched_skills", "missing_required_skills", "first_seen_date", "why", "posting_url",
]


def fetch(con: duckdb.DuckDBPyConnection, top: int) -> tuple[str, dict, list[dict]]:
    as_of = con.execute("select max(as_of_date) from int_posting_scores").fetchone()[0]
    if as_of is None:
        raise SystemExit("int_posting_scores is empty: run ingest and dbt build first")

    summary = dict(zip(
        ["current_postings", "pass_title", "pass_location", "matches", "new_matches"],
        con.execute("""
            select
                count(*),
                count(*) filter (where passes_title),
                count(*) filter (where passes_location),
                count(*) filter (where passes_title and passes_location),
                count(*) filter (where passes_title and passes_location and first_seen_date = as_of_date)
            from int_posting_scores
        """).fetchone()))

    rel = con.execute(f"""
        select
            m.rank, m.score, m.first_seen_date = s.as_of_date as is_new,
            m.company_slug, m.title, m.location_match, m.seniority, m.years_min,
            m.skill_score, m.experience_score, m.level_score, m.freshness_score,
            array_to_string(m.matched_skills, ', ')          as matched_skills,
            array_to_string(m.missing_required_skills, ', ') as missing_required_skills,
            m.first_seen_date, m.why, m.posting_url
        from mart_top_matches as m
        join int_posting_scores as s using (posting_key)
        order by m.rank
        limit ?
    """, [top])
    rows = [dict(zip(COLUMNS, r)) for r in rel.fetchall()]
    return as_of.isoformat(), summary, rows


def to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


def to_html(as_of: str, summary: dict, rows: list[dict]) -> bytes:
    """A single self-contained page: no scripts, no external assets. Every
    value from the data is escaped, since titles and URLs come from the
    job boards (untrusted text)."""
    e = html.escape

    def part(v):
        return "" if v is None else f"{round(100 * v)}"

    body = []
    for r in rows:
        url = r["posting_url"] or ""
        link = (f'<a href="{e(url)}" rel="noopener noreferrer">{e(r["title"])}</a>'
                if url.startswith(("https://", "http://")) else e(r["title"]))
        body.append(
            "<tr>"
            f'<td class="n">{r["rank"]}</td>'
            f'<td class="n score">{r["score"]:.1f}</td>'
            f'<td>{"<span class=new>new</span>" if r["is_new"] else ""}</td>'
            f"<td>{e(r['company_slug'])}</td>"
            f"<td>{link}<div class=why>{e(r['why'] or '')}</div></td>"
            f"<td>{e(r['location_match'] or '')}</td>"
            f'<td class="n">{part(r["skill_score"])}</td>'
            f'<td class="n">{part(r["experience_score"])}</td>'
            f'<td class="n">{part(r["level_score"])}</td>'
            f'<td class="n">{part(r["freshness_score"])}</td>'
            "</tr>")
    if not body:
        body.append('<tr><td colspan="10" class="empty">No postings pass both filters today.</td></tr>')

    s = summary
    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Top matches {e(as_of)}</title>
<style>
  :root {{ color-scheme: light dark; --muted: #6b7280; --line: #e5e7eb; --accent: #2563eb; }}
  @media (prefers-color-scheme: dark) {{ :root {{ --line: #374151; --accent: #60a5fa; }} }}
  body {{ font: 14px/1.45 system-ui, sans-serif; margin: 24px auto; max-width: 1200px; padding: 0 16px; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  .meta {{ color: var(--muted); margin-bottom: 16px; }}
  .table {{ overflow-x: auto; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }}
  th {{ font-weight: 600; font-size: 12px; color: var(--muted); }}
  .n {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .score {{ font-weight: 600; }}
  .why {{ color: var(--muted); font-size: 12px; margin-top: 2px; }}
  .new {{ font-size: 11px; padding: 1px 6px; border-radius: 9px; border: 1px solid var(--accent); color: var(--accent); }}
  .empty {{ color: var(--muted); text-align: center; padding: 24px; }}
  a {{ color: var(--accent); text-decoration: none; }}
</style></head><body>
<h1>Top matches · {e(as_of)}</h1>
<div class="meta">{s['matches']} matches ({s['new_matches']} new) out of {s['current_postings']} open postings ·
{s['pass_title']} pass the title filter · {s['pass_location']} pass the location filter.
Parts are 0–100: skills, experience, level, freshness.</div>
<div class="table"><table>
<thead><tr><th class="n">#</th><th class="n">Score</th><th></th><th>Company</th><th>Role</th><th>Location</th>
<th class="n">Skills</th><th class="n">Exp.</th><th class="n">Level</th><th class="n">Fresh</th></tr></thead>
<tbody>
{chr(10).join(body)}
</tbody></table></div>
</body></html>
"""
    return page.encode()


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def run(db: Path, out_root: Path, top: int) -> Path:
    con = duckdb.connect(str(db), read_only=True)
    try:
        as_of, summary, rows = fetch(con, top)
    finally:
        con.close()

    out = out_root / f"dt={as_of}"
    out.mkdir(parents=True, exist_ok=True)
    _atomic_write(out / "top_matches.csv", to_csv(rows))
    _atomic_write(out / "top_matches.html", to_html(as_of, summary, rows))
    log.info("report for %s: %d matches (%d new) -> %s",
             as_of, summary["matches"], summary["new_matches"], out)
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path,
                   default=Path(os.environ.get("JOBPIPE_DUCKDB_PATH", "data/warehouse.duckdb")))
    p.add_argument("--out", type=Path, default=Path("data/reports"))
    p.add_argument("--top", type=int, default=100, help="rows to include")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(args.db, args.out, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
