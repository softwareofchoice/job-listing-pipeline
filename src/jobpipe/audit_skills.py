"""Audit the skill dictionary (ADR-015) against an LLM, on a sample of postings.

    python -m jobpipe.audit_skills --estimate     # token count + cost, free
    python -m jobpipe.audit_skills                # run (asks before spending)

The dictionary is cheap and deterministic but only finds what it lists, and
its regexes can match the wrong thing. This sends a fixed sample of current
postings to Claude, asks it for every technical skill with its requirement
level, and compares the two answers posting by posting:

    both found it          -> agreement (and do the requirement levels agree?)
    dictionary only        -> possible FALSE MATCH (tighten the pattern)
    LLM only, in the seed  -> possible MISSED MATCH (widen the pattern)
    LLM only, not in seed  -> MISSING SKILL (add a row to skills.csv)

The LLM is not ground truth: every disagreement is a lead for a human to
check, which is why the report shows the evidence for each one. The report
is written to data/audit/ (git-ignored); LLM responses are not stored
(ADR-019).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv

log = logging.getLogger("jobpipe.audit_skills")

DEFAULT_MODEL = "claude-haiku-5-5"
# USD per million tokens, prompts up to 100k tokens (pricing page, 2026-11-19).
PRICES = {"claude-haiku-5-5": (0.10, 0.50)}
PROMPT_VERSION = "v1"
EXPECTED_OUTPUT_TOKENS = 500  # per posting, for the estimate only
LEVELS = ("required", "preferred", "unspecified")

# Sample (ADR-019): 60 data-tech, 25 other engineering, 15 non-tech titles.
SAMPLE_GROUPS = (("data-tech", 60), ("engineering", 25), ("other", 15))
SAMPLE_SQL = r"""
with current as (
    select f.posting_version_key, f.posting_key, f.company_slug, f.title, s.description_text,
        case
            when regexp_matches(lower(f.title), '\bdata\b|analytics|machine learning|\bml\b|\bai\b')
             and regexp_matches(lower(f.title), 'engineer|scientist|analyst|architect|developer') then 'data-tech'
            when regexp_matches(lower(f.title), 'engineer|developer|\bsre\b|devops|architect') then 'engineering'
            else 'other'
        end as sample_group
    from int_posting_features f
    join int_postings__scd2 s
      on s.posting_key = f.posting_key and s.version_number = f.version_number
    where f.is_current and s.description_text is not null
)
select * from (
    select *, row_number() over (
        partition by sample_group
        -- hash() instead of random(): the same seed always gives the same sample
        order by hash(posting_version_key || ?)
    ) as rn
    from current
)
where (sample_group = 'data-tech' and rn <= ?)
   or (sample_group = 'engineering' and rn <= ?)
   or (sample_group = 'other' and rn <= ?)
order by sample_group, rn
"""

SYSTEM_PROMPT = """You extract technical skills from job postings. Your answers are \
used to audit a keyword-based extractor, so be literal and precise.

A skill is a programming language, framework, library, database, cloud platform \
or service, tool, or technical practice (for example data modeling, CI/CD, \
distributed systems). NOT skills: soft skills, degrees, certifications, \
languages spoken, business domains (sales, payments), or job functions.

Include a skill only if the role asks for it or uses it. Exclude:
- the employer's own products when described as what the company sells
- company boilerplate: "about us", benefits, equal-opportunity text

requirement_level:
- required: stated as a requirement or qualification, or in the job title
- preferred: "nice to have", "a plus", "preferred", "bonus", "ideally"
- unspecified: mentioned in the role description or responsibilities, \
without being stated as a requirement

Naming: if a skill is one of the CANONICAL NAMES below, use that exact name \
(for example Postgres -> PostgreSQL, k8s -> Kubernetes, ML -> Machine learning, \
data pipelines / ETL -> ETL/ELT, data lake / data warehouse -> Data warehousing). \
PySpark counts as both Python and Spark. Otherwise use the skill's common name.

CANONICAL NAMES: {canonical}"""

TOOL = {
    "name": "record_skills",
    "description": "Record every technical skill found in the job posting.",
    "input_schema": {
        "type": "object",
        "properties": {
            "skills": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "requirement_level": {"type": "string", "enum": list(LEVELS)},
                        "evidence": {"type": "string", "description": "Short verbatim quote, at most 15 words."},
                    },
                    "required": ["name", "requirement_level", "evidence"],
                },
            }
        },
        "required": ["skills"],
    },
}


# --------------------------------------------------------------------------
# Pure comparison logic (unit-tested without the API)
# --------------------------------------------------------------------------

@dataclass
class PostingComparison:
    posting_version_key: str
    title: str
    agreed: dict[str, tuple[str, str]] = field(default_factory=dict)   # skill -> (dict level, llm level)
    dictionary_only: dict[str, str] = field(default_factory=dict)     # skill -> dictionary example match
    llm_only_in_seed: dict[str, str] = field(default_factory=dict)    # skill -> llm evidence
    llm_only_not_in_seed: dict[str, str] = field(default_factory=dict)


def canonicalize(name: str, canonical: list[str]) -> str:
    """Map an LLM skill name onto the seed's spelling when it's the same skill."""
    lookup = {c.lower(): c for c in canonical}
    key = name.strip().lower()
    return lookup.get(key, name.strip())


def compare(posting_version_key: str, title: str, dictionary: dict[str, tuple[str, str]],
            llm: list[dict], canonical: list[str]) -> PostingComparison:
    """dictionary: skill -> (requirement_level, example_match). llm: tool output items."""
    llm_by_skill: dict[str, tuple[str, str]] = {}
    for item in llm:
        name = canonicalize(item["name"], canonical)
        # If the LLM lists a skill twice, keep the strongest level.
        level = item["requirement_level"]
        prev = llm_by_skill.get(name)
        if prev is None or LEVELS.index(level) < LEVELS.index(prev[0]):
            llm_by_skill[name] = (level, item.get("evidence", ""))

    seed = set(canonical)
    result = PostingComparison(posting_version_key, title)
    for skill, (level, example) in dictionary.items():
        if skill in llm_by_skill:
            result.agreed[skill] = (level, llm_by_skill[skill][0])
        else:
            result.dictionary_only[skill] = example
    for skill, (_, evidence) in llm_by_skill.items():
        if skill in dictionary:
            continue
        if skill in seed:
            result.llm_only_in_seed[skill] = evidence
        else:
            result.llm_only_not_in_seed[skill] = evidence
    return result


def summarize(comparisons: list[PostingComparison]) -> dict:
    tp = sum(len(c.agreed) for c in comparisons)
    fp = sum(len(c.dictionary_only) for c in comparisons)
    fn = sum(len(c.llm_only_in_seed) for c in comparisons)
    per_skill = defaultdict(Counter)
    for c in comparisons:
        for s in c.agreed:
            per_skill[s]["agreed"] += 1
        for s in c.dictionary_only:
            per_skill[s]["dictionary_only"] += 1
        for s in c.llm_only_in_seed:
            per_skill[s]["llm_only"] += 1
    levels = Counter(v for c in comparisons for v in c.agreed.values())
    missing = Counter(s for c in comparisons for s in c.llm_only_not_in_seed)
    return {
        "agreed": tp, "dictionary_only": fp, "llm_only_in_seed": fn,
        # Treating the LLM as the reference. It isn't ground truth, so these are
        # "agreement" rates, not true precision/recall.
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "level_agreement": sum(n for (a, b), n in levels.items() if a == b) / tp if tp else None,
        "level_pairs": levels,
        "per_skill": per_skill,
        "missing": missing,
    }


# --------------------------------------------------------------------------
# Data access and API calls
# --------------------------------------------------------------------------

def load_sample(warehouse: Path, seed: int) -> tuple[list[dict], dict[str, dict], list[str]]:
    con = duckdb.connect(str(warehouse), read_only=True)
    counts = dict(SAMPLE_GROUPS)
    rows = con.execute(SAMPLE_SQL, [str(seed), counts["data-tech"], counts["engineering"], counts["other"]])
    cols = [d[0] for d in rows.description]
    sample = [dict(zip(cols, r)) for r in rows.fetchall()]

    keys = [s["posting_version_key"] for s in sample]
    dictionary: dict[str, dict] = defaultdict(dict)
    for key, skill, level, example in con.execute(
            "select posting_version_key, skill, requirement_level, example_match "
            "from int_posting_skills where list_contains(?, posting_version_key)", [keys]).fetchall():
        dictionary[key][skill] = (level, example)
    canonical = [r[0] for r in con.execute("select skill from skills order by skill").fetchall()]
    con.close()
    return sample, dictionary, canonical


def build_request(posting: dict, canonical: list[str], model: str) -> dict:
    return {
        "model": model,
        "system": SYSTEM_PROMPT.format(canonical=", ".join(canonical)),
        "tools": [TOOL],
        # Forcing the tool makes the answer always arrive as JSON matching the schema.
        "tool_choice": {"type": "tool", "name": TOOL["name"]},
        "messages": [{"role": "user", "content":
                      f"Company: {posting['company_slug']}\nTitle: {posting['title']}\n\n{posting['description_text']}"}],
    }


def extract(client, request: dict) -> tuple[list[dict], int, int]:
    msg = client.messages.create(max_tokens=2000, **request)
    for block in msg.content:
        if block.type == "tool_use":
            return block.input.get("skills", []), msg.usage.input_tokens, msg.usage.output_tokens
    raise ValueError(f"no tool_use block (stop_reason={msg.stop_reason})")


def cost(model: str, input_tokens: int, output_tokens: int) -> float:
    price_in, price_out = PRICES[model]
    return (input_tokens * price_in + output_tokens * price_out) / 1_000_000


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def pct(x):
    return "n/a" if x is None else f"{100 * x:.0f}%"


def write_report(path: Path, comparisons: list[PostingComparison], summary: dict, meta: dict) -> None:
    lines = [
        f"# Skill dictionary audit, {meta['date']}",
        "",
        f"Model `{meta['model']}` (prompt {PROMPT_VERSION}), {meta['n']} postings "
        f"({', '.join(f'{n} {g}' for g, n in SAMPLE_GROUPS)}), seed {meta['seed']}. "
        f"Tokens: {meta['input_tokens']:,} in / {meta['output_tokens']:,} out, cost ${meta['cost']:.3f}. "
        f"Failed requests: {meta['failed']}.",
        "",
        "The LLM is a second opinion, not ground truth: check the evidence before changing the seed.",
        "",
        "## Summary",
        "",
        "| Measure | Value | Meaning |",
        "|---|---|---|",
        f"| Agreed | {summary['agreed']} | both found the skill |",
        f"| Dictionary only | {summary['dictionary_only']} | possible false matches |",
        f"| LLM only, skill in seed | {summary['llm_only_in_seed']} | possible missed matches |",
        f"| Precision vs LLM | {pct(summary['precision'])} | agreed / dictionary found |",
        f"| Recall vs LLM | {pct(summary['recall'])} | agreed / LLM found (seed skills only) |",
        f"| Requirement level agreement | {pct(summary['level_agreement'])} | same level, where both found it |",
        "",
        "## Skills missing from the dictionary",
        "",
        "Found by the LLM, not in `skills.csv`. Candidates for new rows.",
        "",
        "| Skill | Postings | Example evidence |",
        "|---|---|---|",
    ]
    evidence = defaultdict(list)
    for c in comparisons:
        for s, ev in c.llm_only_not_in_seed.items():
            evidence[s].append(ev)
    for skill, n in summary["missing"].most_common(40):
        lines.append(f"| {skill} | {n} | {evidence[skill][0][:90]} |")

    lines += ["", "## Per-skill disagreement", "",
              "Seed skills where the two disagree at least once.", "",
              "| Skill | Agreed | Dictionary only | LLM only |", "|---|---|---|---|"]
    per = summary["per_skill"]
    disagreeing = [s for s in per if per[s]["dictionary_only"] or per[s]["llm_only"]]
    for s in sorted(disagreeing, key=lambda s: -(per[s]["dictionary_only"] + per[s]["llm_only"])):
        lines.append(f"| {s} | {per[s]['agreed']} | {per[s]['dictionary_only']} | {per[s]['llm_only']} |")

    lines += ["", "## Possible false matches (dictionary only)", "",
              "| Skill | Posting | Dictionary matched |", "|---|---|---|"]
    for c in comparisons:
        for s, ex in sorted(c.dictionary_only.items()):
            lines.append(f"| {s} | {c.title[:50]} | `{ex}` |")

    lines += ["", "## Possible missed matches (LLM only, skill in seed)", "",
              "| Skill | Posting | LLM evidence |", "|---|---|---|"]
    for c in comparisons:
        for s, ev in sorted(c.llm_only_in_seed.items()):
            lines.append(f"| {s} | {c.title[:50]} | {ev[:90]} |")

    lines += ["", "## Requirement level: dictionary (rows) vs LLM (columns)", "",
              "| | " + " | ".join(LEVELS) + " |", "|---|" + "---|" * len(LEVELS)]
    for a in LEVELS:
        lines.append(f"| {a} | " + " | ".join(str(summary["level_pairs"][(a, b)]) for b in LEVELS) + " |")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse.duckdb"))
    p.add_argument("--out-dir", type=Path, default=Path("data/audit"))
    p.add_argument("--model", default=DEFAULT_MODEL, choices=sorted(PRICES))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--estimate", action="store_true", help="count tokens and estimate cost; no LLM calls")
    p.add_argument("--yes", action="store_true", help="don't ask for confirmation before spending")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(override=False)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.error("ANTHROPIC_API_KEY is not set. Add it to .env (see .env.example).")
        return 2

    import anthropic  # imported here so the pure functions above don't need it
    client = anthropic.Anthropic()

    sample, dictionary, canonical = load_sample(args.warehouse, args.seed)
    requests = [build_request(s, canonical, args.model) for s in sample]
    log.info("sample: %d postings %s", len(sample), dict(Counter(s["sample_group"] for s in sample)))

    # Token counting is free and returns the exact input size for this model.
    # It still needs a working account, so it doubles as a check of the key
    # and billing before any paid call.
    try:
        input_estimate = sum(client.messages.count_tokens(**r).input_tokens for r in requests)
    except anthropic.APIStatusError as e:
        message = e.body.get("error", {}).get("message") if isinstance(e.body, dict) else None
        log.error("Anthropic API refused the request (HTTP %s): %s", e.status_code, message or e)
        return 2
    estimate = cost(args.model, input_estimate, EXPECTED_OUTPUT_TOKENS * len(requests))
    log.info("estimate: %d input tokens, ~%d output tokens, ~$%.3f",
             input_estimate, EXPECTED_OUTPUT_TOKENS * len(requests), estimate)
    if args.estimate:
        return 0
    if not args.yes and input(f"Spend about ${estimate:.3f}? [y/N] ").strip().lower() != "y":
        log.info("cancelled")
        return 1

    def run_one(i):
        try:
            return i, *extract(client, requests[i])
        except Exception as e:  # one bad response shouldn't sink the audit
            log.warning("posting %s failed: %s", sample[i]["posting_version_key"], e)
            return i, None, 0, 0

    comparisons, tokens_in, tokens_out, failed = [], 0, 0, 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for i, skills, t_in, t_out in pool.map(run_one, range(len(requests))):
            tokens_in, tokens_out = tokens_in + t_in, tokens_out + t_out
            if skills is None:
                failed += 1
                continue
            s = sample[i]
            comparisons.append(compare(s["posting_version_key"], s["title"],
                                       dictionary.get(s["posting_version_key"], {}), skills, canonical))

    summary = summarize(comparisons)
    date = datetime.now(timezone.utc).date().isoformat()
    path = args.out_dir / f"skill_audit_{date}_{args.model}.md"
    write_report(path, comparisons, summary, {
        "date": date, "model": args.model, "n": len(comparisons), "seed": args.seed,
        "input_tokens": tokens_in, "output_tokens": tokens_out,
        "cost": cost(args.model, tokens_in, tokens_out), "failed": failed})
    log.info("precision vs LLM %s, recall vs LLM %s, level agreement %s. report: %s",
             pct(summary["precision"]), pct(summary["recall"]), pct(summary["level_agreement"]), path)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
