#!/usr/bin/env python3
"""Generate charts for docs/scenario-report from results.json."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "scenario-report"
DATA = json.loads((OUT / "results.json").read_text(encoding="utf-8-sig"))

plt.rcParams.update(
    {
        "figure.facecolor": "white",
        "axes.facecolor": "#fafafa",
        "axes.grid": True,
        "grid.alpha": 0.35,
        "font.size": 10,
    }
)


def save(fig, name: str) -> None:
    path = OUT / name
    fig.tight_layout()
    fig.savefig(path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def chart_pass_fail() -> None:
    ids = [s["id"] for s in DATA["scenarios"]]
    passed = [1 if s["passed"] else 0 for s in DATA["scenarios"]]
    colors = ["#2a9d8f" if p else "#e76f51" for p in passed]
    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.bar(ids, passed, color=colors)
    ax.set_ylim(0, 1.2)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["fail", "pass"])
    ax.set_title(f"Scenario outcomes ({DATA['pass_count']}/{DATA['total_count']} passed)")
    ax.set_xlabel("Scenario")
    save(fig, "chart-pass-fail.png")


def chart_elapsed() -> None:
    ids = [s["id"] for s in DATA["scenarios"]]
    ms = [s["elapsed_ms"] for s in DATA["scenarios"]]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(ids, ms, color="#264653")
    ax.set_ylabel("Elapsed (ms)")
    ax.set_title("Wall time per scenario (includes waits)")
    save(fig, "chart-elapsed.png")


def chart_suite_deltas() -> None:
    d = DATA["suite_metrics_delta"]
    labels = [
        "accounts",
        "accept\nnormal",
        "accept\nexpress",
        "reject\n402",
        "reject\n400",
        "campaigns\nok",
        "campaigns\n402",
        "credits\nsingle",
        "credits\ncampaign",
    ]
    keys = [
        "accounts_created",
        "messages_accepted_normal",
        "messages_accepted_express",
        "messages_rejected_insufficient",
        "messages_rejected_validation",
        "campaigns_accepted",
        "campaigns_rejected_insufficient",
        "credits_spent_single_normal",
        "credits_spent_campaign",
    ]
    vals = [d.get(k, 0) for k in keys]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(labels, vals, color="#e9c46a")
    ax.set_ylabel("Delta over suite")
    ax.set_title("api-gateway sms_* metric deltas (baseline → final)")
    save(fig, "chart-metric-deltas.png")


def chart_burst_latency() -> None:
    s7 = next(s for s in DATA["scenarios"] if s["id"] == "S7")
    samples = s7["http"]["latency_ms"]["samples"]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(range(1, len(samples) + 1), samples, marker="o", color="#1d3557")
    ax.axhline(s7["http"]["latency_ms"]["p95"], color="#e63946", linestyle="--", label=f"p95={s7['http']['latency_ms']['p95']} ms")
    ax.axhline(s7["http"]["latency_ms"]["avg"], color="#457b9d", linestyle=":", label=f"avg={s7['http']['latency_ms']['avg']} ms")
    ax.set_xlabel("Message #")
    ax.set_ylabel("Client latency (ms)")
    ax.set_title("S7 burst: sequential accept latency")
    ax.legend()
    save(fig, "chart-burst-latency.png")


def chart_credit_flow() -> None:
    # Approximate from suite deltas + known scenario economics
    labels = ["Topped up", "Spent (single)", "Spent (campaign)", "Rejected (no spend)"]
    d = DATA["suite_metrics_delta"]
    vals = [
        d.get("topup_credits", 0),
        d.get("credits_spent_single_normal", 0) + d.get("messages_accepted_express", 0),  # express also spent
        d.get("credits_spent_campaign", 0),
        d.get("messages_rejected_insufficient", 0) + d.get("campaigns_rejected_insufficient", 0),
    ]
    # Fix express credits: suite may not have express in credits_spent_single_normal key
    # Recompute from scenarios for accuracy
    spent_single = 0
    spent_campaign = 0
    topped = 0
    for s in DATA["scenarios"]:
        md = s.get("metrics_delta", {})
        for k, v in md.items():
            if "topup_credits" in k:
                topped += v
            if "credits_spent" in k and "campaign" in k:
                spent_campaign += v
            elif "credits_spent" in k:
                spent_single += v
    if topped == 0:
        topped = d.get("topup_credits", 0)
    if spent_single == 0:
        spent_single = d.get("credits_spent_single_normal", 0)
    if spent_campaign == 0:
        spent_campaign = d.get("credits_spent_campaign", 0)
    vals = [topped, spent_single, spent_campaign, d.get("messages_rejected_insufficient", 0) + d.get("campaigns_rejected_insufficient", 0)]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(labels, vals, color=["#2a9d8f", "#e9c46a", "#f4a261", "#e76f51"])
    ax.set_ylabel("Credits / events")
    ax.set_title("Credit flow across suite")
    save(fig, "chart-credit-flow.png")


def chart_worker_dispatch() -> None:
    workers = DATA.get("workers") or {}
    totals = {}
    for svc, metrics in workers.items():
        if not isinstance(metrics, dict):
            continue
        for k, v in metrics.items():
            if k.startswith("sms_dispatch_total"):
                # extract status
                status = "unknown"
                if 'status="' in k:
                    status = k.split('status="')[1].split('"')[0]
                totals[status] = totals.get(status, 0) + v
    if not totals:
        # fallback empty chart
        fig, ax = plt.subplots(figsize=(6, 3.5))
        ax.text(0.5, 0.5, "No dispatcher metrics scraped", ha="center", va="center")
        ax.axis("off")
        save(fig, "chart-dispatch-outcomes.png")
        return
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    ax.bar(list(totals.keys()), list(totals.values()), color="#457b9d")
    ax.set_title("Dispatcher sms_dispatch_total by status (worker scrape)")
    ax.set_ylabel("Count")
    save(fig, "chart-dispatch-outcomes.png")


if __name__ == "__main__":
    chart_pass_fail()
    chart_elapsed()
    chart_suite_deltas()
    chart_burst_latency()
    chart_credit_flow()
    chart_worker_dispatch()
    print("done")
