from __future__ import annotations

from typing import Optional, Dict

from src.vinv.vinv_crisis_windows import CrisisWindowResult


def format_percentage(x: Optional[float]) -> str:
    if x is None or x != x:  # NaN check
        return "N/A"
    return f"{x:.2%}"


def format_number(x: Optional[float], decimals: int = 2) -> str:
    if x is None or x != x:
        return "N/A"
    return f"{x:.{decimals}f}"


def vinv_leaf_report_markdown(
    name: str,
    benchmark: str,
    summary_stats: Dict,
    crisis_stats: Optional[CrisisWindowResult] = None,
    regime_means: Optional[Dict[str, Dict[str, float]]] = None,
) -> str:
    """
    Build a markdown VinV Leaf Report for a basket or ticker.

    Parameters
    ----------
    name : str
        Name of the VinV object, e.g. 'VinV Equal-Weight Basket' or 'AFL'.
    benchmark : str
        Benchmark ticker symbol, e.g. 'SPY' or 'IWD'.
    summary_stats : dict
        Output from vinv_basic_stats.basic_outperformance_summary, e.g.:
            {
                "n_months": ...,
                "vinv_cum_return": ...,
                "benchmark_cum_return": ...,
                "pct_months_vinv_outperforms": ...,
                "vinv_ann_return": ...,
                "bench_ann_return": ...,
                "vinv_ann_vol": ...,
                "bench_ann_vol": ...,
                "vinv_sharpe": ...,
                "excess_mean": ...,
                "excess_t_stat": ...
            }
    crisis_stats : Optional[CrisisWindowResult]
        Optional crisis window summary (e.g. COVID).
    regime_means : Optional[Dict[str, Dict[str, float]]]
        Optional dict of regime -> {"vinv_mean_ret": float, "bench_mean_ret": float}.

    Returns
    -------
    str
        Markdown string describing the VinV leaf.
    """
    n_months = summary_stats.get("n_months", 0)
    vinv_cum = format_percentage(summary_stats.get("vinv_cum_return"))
    bench_cum = format_percentage(summary_stats.get("benchmark_cum_return"))
    pct_out = format_number(summary_stats.get("pct_months_vinv_outperforms"))
    vinv_ann = format_percentage(summary_stats.get("vinv_ann_return"))
    bench_ann = format_percentage(summary_stats.get("bench_ann_return"))
    vinv_vol = format_percentage(summary_stats.get("vinv_ann_vol"))
    bench_vol = format_percentage(summary_stats.get("bench_ann_vol"))
    vinv_sharpe = format_number(summary_stats.get("vinv_sharpe"))
    excess_mean = format_percentage(summary_stats.get("excess_mean"))
    excess_t = format_number(summary_stats.get("excess_t_stat"))

    lines = []

    lines.append(f"# VinV Leaf Report — {name}")
    lines.append("")
    lines.append(f"**Benchmark:** {benchmark}")
    lines.append(f"**Sample length:** {n_months} months")
    lines.append("")

    # High-level performance
    lines.append("## 1. Performance vs Benchmark")
    lines.append("")
    lines.append("| Metric | VinV | Benchmark |")
    lines.append("|--------|------|-----------|")
    lines.append(f"| Cumulative return | {vinv_cum} | {bench_cum} |")
    lines.append(f"| Annualized return | {vinv_ann} | {bench_ann} |")
    lines.append(f"| Annualized volatility | {vinv_vol} | {bench_vol} |")
    lines.append("")
    lines.append(f"- % of months VinV outperforms {benchmark}: **{pct_out}%**")
    lines.append(f"- VinV Sharpe (no RF): **{vinv_sharpe}**")
    lines.append(f"- Mean excess return (VinV − {benchmark}): **{excess_mean}** per month")
    lines.append(f"- t-stat of excess mean: **{excess_t}**")
    lines.append("")

    # Crisis section (optional)
    if crisis_stats is not None:
        lines.append("## 2. Crisis-Window Behavior")
        lines.append("")
        lines.append(f"**Window:** {crisis_stats.name} "
                     f"({crisis_stats.start.date()} → {crisis_stats.end.date()})")
        lines.append("")
        lines.append("| Metric | VinV | Benchmark |")
        lines.append("|--------|------|-----------|")
        lines.append(
            f"| Cumulative return | "
            f"{format_percentage(crisis_stats.vinv_cum_return)} | "
            f"{format_percentage(crisis_stats.bench_cum_return)} |"
        )
        lines.append(
            f"| Max drawdown | "
            f"{format_percentage(crisis_stats.vinv_max_dd)} | "
            f"{format_percentage(crisis_stats.bench_max_dd)} |"
        )
        lines.append("")
        lines.append(
            f"- Months in window: **{crisis_stats.n_months}**"
        )
        lines.append("")

    # Regime section (optional)
    if regime_means:
        lines.append("## 3. Regime-Conditioned Averages")
        lines.append("")
        lines.append("| Regime | VinV mean monthly return | Benchmark mean monthly return |")
        lines.append("|--------|---------------------------|--------------------------------|")
        for reg_name, stats in regime_means.items():
            vinv_m = format_percentage(stats.get("vinv_mean_ret"))
            bench_m = format_percentage(stats.get("bench_mean_ret"))
            lines.append(f"| {reg_name} | {vinv_m} | {bench_m} |")
        lines.append("")

    # Interpretation placeholder
    lines.append("## 4. Interpretive Notes (to be completed by analyst)")
    lines.append("")
    lines.append("- How does VinV behave in stress vs calm periods?")
    lines.append("- Are excess returns concentrated in specific regimes?")
    lines.append("- Does the Sharpe / t-stat suggest a durable effect or noise?")
    lines.append("")

    return "\n".join(lines)
