#!/usr/bin/env python3
"""获取实时板块资金流入/流出 Top10."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="通过 AkShare 获取实时板块资金流入/流出前 N 名。"
    )
    parser.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "eastmoney", "ths"],
        help="数据源，默认: auto（先同花顺，失败后回退东方财富）",
    )
    parser.add_argument(
        "--indicator",
        default="今日",
        choices=["今日", "3日", "5日", "10日"],
        help="统计周期，默认: 今日",
    )
    parser.add_argument(
        "--sector-type",
        default="行业资金流",
        choices=["行业资金流", "概念资金流", "地域资金流"],
        help="板块类型，默认: 行业资金流",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="显示前 N 条数据，默认: 10",
    )
    parser.add_argument(
        "--output",
        default="sector_fund_flow_top10.png",
        help="图表输出路径，默认: sector_fund_flow_top10.png",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="仅输出终端结果，不生成图表",
    )
    return parser.parse_args()


def find_first_existing_column(columns: Iterable[str], candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise KeyError(f"未找到可用列，候选列: {candidates}")


def find_column_by_keywords(columns: Iterable[str], keywords: list[str]) -> str:
    for column in columns:
        if all(keyword in column for keyword in keywords):
            return column
    raise KeyError(f"未找到包含关键字 {keywords} 的列")


def normalize_money_column(df: "pd.DataFrame", column: str) -> "pd.Series":
    import pandas as pd

    series = (
        df[column]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace(" ", "", regex=False)
        .replace({"-": None, "nan": None, "None": None})
    )
    return pd.to_numeric(series, errors="coerce")


def load_sector_fund_flow_eastmoney(indicator: str, sector_type: str) -> "pd.DataFrame":
    import akshare as ak
    import pandas as pd

    last_error = None
    for _ in range(3):
        try:
            df = ak.stock_sector_fund_flow_rank(
                indicator=indicator,
                sector_type=sector_type,
            )
            break
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    else:
        raise RuntimeError(last_error) from last_error

    if df.empty:
        raise ValueError("接口返回空数据，请稍后重试。")

    name_col = find_first_existing_column(df.columns, ["名称", "板块名称"])
    try:
        change_col = find_first_existing_column(df.columns, ["今日涨跌幅", "涨跌幅"])
    except KeyError:
        change_col = find_column_by_keywords(df.columns, ["涨跌幅"])

    try:
        net_inflow_col = find_first_existing_column(
            df.columns,
            ["今日主力净流入-净额", "主力净流入-净额"],
        )
    except KeyError:
        net_inflow_col = find_column_by_keywords(df.columns, ["主力净流入", "净额"])

    try:
        net_ratio_col = find_first_existing_column(
            df.columns,
            ["今日主力净流入-净占比", "主力净流入-净占比"],
        )
    except KeyError:
        net_ratio_col = find_column_by_keywords(df.columns, ["主力净流入", "净占比"])

    result = df[[name_col, change_col, net_inflow_col, net_ratio_col]].copy()
    result.columns = ["板块名称", "涨跌幅", "主力净流入", "主力净流入占比"]
    result["主力净流入"] = normalize_money_column(result, "主力净流入")
    result["涨跌幅"] = pd.to_numeric(result["涨跌幅"], errors="coerce")
    return result.dropna(subset=["主力净流入"])


def load_sector_fund_flow_ths(indicator: str, sector_type: str) -> "pd.DataFrame":
    import akshare as ak
    import pandas as pd

    if sector_type == "地域资金流":
        raise ValueError("同花顺暂不支持地域资金流")

    symbol_map = {
        "今日": "即时",
        "3日": "3日排行",
        "5日": "5日排行",
        "10日": "10日排行",
    }
    if indicator not in symbol_map:
        raise ValueError(f"同花顺暂不支持该周期: {indicator}")

    api_map = {
        "行业资金流": ak.stock_fund_flow_industry,
        "概念资金流": ak.stock_fund_flow_concept,
    }
    if sector_type not in api_map:
        raise ValueError(f"同花顺暂不支持该板块类型: {sector_type}")

    last_error = None
    for _ in range(3):
        try:
            df = api_map[sector_type](symbol=symbol_map[indicator])
            break
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    else:
        raise RuntimeError(last_error) from last_error

    if df.empty:
        raise ValueError("同花顺接口返回空数据，请稍后重试。")

    name_col = find_first_existing_column(df.columns, ["行业", "概念", "名称"])
    change_col = find_first_existing_column(
        df.columns,
        ["行业-涨跌幅", "涨跌幅", "阶段涨跌幅"],
    )
    net_inflow_col = find_first_existing_column(df.columns, ["净额"])
    result = df[[name_col, change_col, net_inflow_col]].copy()
    result.columns = ["板块名称", "涨跌幅", "主力净流入"]
    result["主力净流入"] = pd.to_numeric(result["主力净流入"], errors="coerce") * 100000000
    result["涨跌幅"] = pd.to_numeric(result["涨跌幅"], errors="coerce")
    result["主力净流入占比"] = "-"
    return result.dropna(subset=["主力净流入"])


def fetch_sector_fund_flow(indicator: str, sector_type: str, provider: str = "auto") -> tuple["pd.DataFrame", str]:
    errors: list[str] = []
    loaders: list[tuple[str, callable]] = []

    if provider == "auto":
        loaders = [
            ("ths", load_sector_fund_flow_ths),
            ("eastmoney", load_sector_fund_flow_eastmoney),
        ]
    elif provider == "eastmoney":
        loaders = [("eastmoney", load_sector_fund_flow_eastmoney)]
    elif provider == "ths":
        loaders = [("ths", load_sector_fund_flow_ths)]
    else:
        raise ValueError(f"不支持的数据源: {provider}")

    for name, loader in loaders:
        try:
            return loader(indicator, sector_type), name
        except Exception as exc:
            errors.append(f"{name}: {exc}")

    raise RuntimeError(" | ".join(errors))


def format_money(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 100000000:
        return f"{value / 100000000:.2f} 亿"
    if abs_value >= 10000:
        return f"{value / 10000:.2f} 万"
    return f"{value:.2f}"


def pretty_print(title: str, df: "pd.DataFrame") -> None:
    print(f"\n{title}")
    print("-" * len(title))
    for idx, row in enumerate(df.itertuples(index=False), start=1):
        print(
            f"{idx:>2}. {row.板块名称:<10} "
            f"主力净流入: {format_money(row.主力净流入):>10}  "
            f"涨跌幅: {row.涨跌幅:>6.2f}%  "
            f"净占比: {row.主力净流入占比}"
        )


def disable_system_proxy() -> None:
    import requests

    original = requests.sessions.Session.merge_environment_settings

    def merge_environment_settings(self, url, proxies, stream, verify, cert):
        settings = original(self, url, {}, stream, verify, cert)
        settings["proxies"] = {}
        return settings

    requests.sessions.Session.merge_environment_settings = merge_environment_settings


def plot_fund_flow(
    inflow_top: "pd.DataFrame",
    outflow_top: "pd.DataFrame",
    indicator: str,
    sector_type: str,
    output_path: str,
) -> Path:
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = [
        "PingFang SC",
        "Heiti SC",
        "Arial Unicode MS",
        "Microsoft YaHei",
        "SimHei",
        "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False

    inflow_chart = inflow_top.copy().sort_values("主力净流入", ascending=True)
    outflow_chart = outflow_top.copy().sort_values("主力净流入", ascending=False)

    inflow_chart["主力净流入(亿)"] = inflow_chart["主力净流入"] / 100000000
    outflow_chart["主力净流入(亿)"] = outflow_chart["主力净流入"] / 100000000

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle(f"板块资金流监控 | {indicator} | {sector_type}", fontsize=16)

    axes[0].barh(
        inflow_chart["板块名称"],
        inflow_chart["主力净流入(亿)"],
        color="#d62728",
    )
    axes[0].set_title(f"主力资金净流入 Top{len(inflow_chart)}")
    axes[0].set_xlabel("净流入（亿元）")

    axes[1].barh(
        outflow_chart["板块名称"],
        outflow_chart["主力净流入(亿)"],
        color="#1f77b4",
    )
    axes[1].set_title(f"主力资金净流出 Top{len(outflow_chart)}")
    axes[1].set_xlabel("净流入（亿元，负值为流出）")

    for axis in axes:
        axis.grid(axis="x", linestyle="--", alpha=0.3)

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return output


def main() -> int:
    args = parse_args()
    if args.top <= 0:
        print("--top 必须大于 0", file=sys.stderr)
        return 1

    try:
        import akshare  # noqa: F401
        import matplotlib  # noqa: F401
        import pandas  # noqa: F401
        import requests  # noqa: F401
    except ModuleNotFoundError:
        print(
            "缺少依赖，请先执行: python3 -m pip install akshare pandas matplotlib requests",
            file=sys.stderr,
        )
        return 1

    disable_system_proxy()

    try:
        df, actual_provider = fetch_sector_fund_flow(
            indicator=args.indicator,
            sector_type=args.sector_type,
            provider=args.provider,
        )
    except Exception as exc:
        print(f"获取板块资金流失败: {exc}", file=sys.stderr)
        return 1

    inflow_top = df.sort_values("主力净流入", ascending=False).head(args.top)
    outflow_top = df.sort_values("主力净流入", ascending=True).head(args.top)

    print(
        f"板块资金流监控 | 数据源: {actual_provider} | 周期: {args.indicator} | 类型: {args.sector_type} | 条数: {args.top}"
    )
    pretty_print(f"主力资金净流入 Top{args.top}", inflow_top)
    pretty_print(f"主力资金净流出 Top{args.top}", outflow_top)

    if not args.no_plot:
        try:
            output = plot_fund_flow(
                inflow_top=inflow_top,
                outflow_top=outflow_top,
                indicator=args.indicator,
                sector_type=args.sector_type,
                output_path=args.output,
            )
        except Exception as exc:
            print(f"\n生成图表失败: {exc}", file=sys.stderr)
            return 1
        print(f"\n图表已生成: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
