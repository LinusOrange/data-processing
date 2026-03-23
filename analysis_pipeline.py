from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats


DEFAULT_CONFIG: Dict[str, Any] = {
    "file_path": "./data.xlsx",
    "sheet_name": 0,
    "output_dir": "./outputs",
    "id_col": "firm_id",
    "time_col": "year",
    "dependent_var": "Y",
    "main_independent_var": "Media",
    "mediator_var": "SA",
    "moderator_var": "ROA",
    "controls": ["Size", "Lev", "Growth"],
    "numeric_columns": [],
    "dropna_subset": [],
    "winsorize": {
        "enabled": True,
        "columns": [],
        "lower": 0.01,
        "upper": 0.99,
        "by_time": True,
    },
    "cleaning": {
        "drop_duplicates": True,
        "sort": True,
    },
    "subgroup": {
        "enabled": False,
        "column": "Property",
        "groups": [0, 1],
    },
    "robustness": {
        "enabled": False,
        "models": [
            {
                "name": "alt_y",
                "dependent_var": "Y_alt",
            },
            {
                "name": "lag_x",
                "lag_main_independent_var": 1,
            },
        ],
    },
}


class AnalysisError(Exception):
    """Raised when the input data or config is invalid."""


def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    if not config_path:
        return DEFAULT_CONFIG.copy()

    with open(config_path, "r", encoding="utf-8") as fh:
        user_config = json.load(fh)

    config = json.loads(json.dumps(DEFAULT_CONFIG))
    merge_dict(config, user_config)
    return config


def merge_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_dict(base[key], value)
        else:
            base[key] = value


def ensure_columns(df: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [col for col in columns if col and col not in df.columns]
    if missing:
        raise AnalysisError(f"{label} 缺少列: {missing}")


def read_data(config: Dict[str, Any]) -> pd.DataFrame:
    file_path = Path(config["file_path"])
    if not file_path.exists():
        raise AnalysisError(
            f"未找到数据文件: {file_path}. 请把 Excel 文件放到仓库内，或修改 config.json 中的 file_path。"
        )

    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, sheet_name=config.get("sheet_name", 0))
    if suffix == ".csv":
        return pd.read_csv(file_path)
    raise AnalysisError(f"暂不支持的文件格式: {suffix}")


def clean_data(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cleaning_cfg = config.get("cleaning", {})
    id_col = config["id_col"]
    time_col = config["time_col"]
    y = config["dependent_var"]
    x = config["main_independent_var"]
    mediator = config["mediator_var"]
    moderator = config["moderator_var"]
    controls = config.get("controls", [])

    required_cols = [id_col, time_col, y, x, mediator, moderator, *controls]
    ensure_columns(df, required_cols, "核心分析")

    if cleaning_cfg.get("drop_duplicates", True):
        df = df.drop_duplicates().copy()

    numeric_columns = set(config.get("numeric_columns") or [])
    numeric_columns.update([time_col, y, x, mediator, moderator, *controls])
    numeric_columns.update(config.get("winsorize", {}).get("columns") or [])

    subgroup_cfg = config.get("subgroup", {})
    if subgroup_cfg.get("enabled"):
        numeric_columns.discard(subgroup_cfg.get("column", ""))

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    dropna_subset = config.get("dropna_subset") or [id_col, time_col, y, x]
    ensure_columns(df, dropna_subset, "缺失值处理")
    before_rows = len(df)
    df = df.dropna(subset=dropna_subset).copy()

    if cleaning_cfg.get("sort", True):
        df = df.sort_values([id_col, time_col]).reset_index(drop=True)

    report = pd.DataFrame(
        [
            {"step": "raw_rows", "value": before_rows},
            {"step": "clean_rows", "value": len(df)},
            {"step": "dropped_rows", "value": before_rows - len(df)},
            {"step": "n_entities", "value": df[id_col].nunique()},
            {"step": "n_periods", "value": df[time_col].nunique()},
        ]
    )
    return df, report


def winsorize_series(series: pd.Series, lower: float, upper: float) -> pd.Series:
    valid = series.dropna()
    if valid.empty:
        return series
    low_val = valid.quantile(lower)
    high_val = valid.quantile(upper)
    return series.clip(lower=low_val, upper=high_val)


def apply_winsorize(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    winsor_cfg = config.get("winsorize", {})
    if not winsor_cfg.get("enabled", True):
        return df

    columns = winsor_cfg.get("columns") or [
        config["dependent_var"],
        config["main_independent_var"],
        config["mediator_var"],
        config["moderator_var"],
        *config.get("controls", []),
    ]
    ensure_columns(df, columns, "缩尾处理")

    lower = winsor_cfg.get("lower", 0.01)
    upper = winsor_cfg.get("upper", 0.99)
    by_time = winsor_cfg.get("by_time", True)
    time_col = config["time_col"]

    out = df.copy()
    for col in columns:
        if by_time:
            out[col] = out.groupby(time_col, group_keys=False)[col].apply(
                lambda s: winsorize_series(s, lower, upper)
            )
        else:
            out[col] = winsorize_series(out[col], lower, upper)
    return out


def descriptive_statistics(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    stats_df = df[list(columns)].describe(percentiles=[0.25, 0.5, 0.75]).T
    stats_df["skew"] = df[list(columns)].skew(numeric_only=True)
    stats_df["kurtosis"] = df[list(columns)].kurt(numeric_only=True)
    stats_df["missing"] = df[list(columns)].isna().sum()
    return stats_df.reset_index().rename(columns={"index": "variable"})


def pearson_pvalue(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns
    pvals = pd.DataFrame(np.ones((len(cols), len(cols))), columns=cols, index=cols)
    for i, col_i in enumerate(cols):
        for j, col_j in enumerate(cols):
            if i >= j:
                continue
            pair = df[[col_i, col_j]].dropna()
            if len(pair) < 3:
                pval = np.nan
            else:
                _, pval = stats.pearsonr(pair[col_i], pair[col_j])
            pvals.loc[col_i, col_j] = pval
            pvals.loc[col_j, col_i] = pval
    np.fill_diagonal(pvals.values, 0)
    return pvals


def significance_stars(pvalue: float) -> str:
    if pd.isna(pvalue):
        return ""
    if pvalue < 0.01:
        return "***"
    if pvalue < 0.05:
        return "**"
    if pvalue < 0.1:
        return "*"
    return ""


def correlation_outputs(df: pd.DataFrame, columns: Sequence[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    corr = df[list(columns)].corr(method="pearson")
    pvals = pearson_pvalue(df[list(columns)])
    star_corr = corr.copy().astype(object)
    for r in corr.index:
        for c in corr.columns:
            star_corr.loc[r, c] = f"{corr.loc[r, c]:.4f}{significance_stars(pvals.loc[r, c])}"
    return corr, pvals, star_corr


def build_formula(dependent_var: str, regressors: Sequence[str], id_col: str, time_col: str) -> str:
    rhs = " + ".join(regressors)
    return f"{dependent_var} ~ {rhs} + C({id_col}) + C({time_col})"


def run_fe_regression(
    df: pd.DataFrame,
    dependent_var: str,
    regressors: Sequence[str],
    id_col: str,
    time_col: str,
    cluster_col: Optional[str] = None,
):
    use_cols = [dependent_var, *regressors, id_col, time_col]
    ensure_columns(df, use_cols, "固定效应回归")
    model_df = df[use_cols].dropna().copy()
    formula = build_formula(dependent_var, regressors, id_col, time_col)
    model = smf.ols(formula=formula, data=model_df)
    if cluster_col and cluster_col in model_df.columns:
        result = model.fit(cov_type="cluster", cov_kwds={"groups": model_df[cluster_col]})
    else:
        result = model.fit(cov_type="HC1")
    return result, model_df, formula


def tidy_result(result, model_name: str) -> pd.DataFrame:
    table = pd.DataFrame(
        {
            "term": result.params.index,
            "coef": result.params.values,
            "std_err": result.bse.values,
            "t_value": result.tvalues.values,
            "p_value": result.pvalues.values,
        }
    )
    table["stars"] = table["p_value"].map(significance_stars)
    table["model"] = model_name
    table["nobs"] = int(result.nobs)
    table["r_squared"] = result.rsquared
    table["adj_r_squared"] = result.rsquared_adj
    return table


def summarize_model(result, model_name: str, formula: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "model": model_name,
                "formula": formula,
                "nobs": int(result.nobs),
                "r_squared": result.rsquared,
                "adj_r_squared": result.rsquared_adj,
                "fvalue": getattr(result, "fvalue", np.nan),
                "f_pvalue": getattr(result, "f_pvalue", np.nan),
            }
        ]
    )


def mediation_analysis(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    y = config["dependent_var"]
    x = config["main_independent_var"]
    m = config["mediator_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]

    step1, _, formula1 = run_fe_regression(df, y, [x, *controls], id_col, time_col)
    step2, _, formula2 = run_fe_regression(df, m, [x, *controls], id_col, time_col)
    step3, _, formula3 = run_fe_regression(df, y, [x, m, *controls], id_col, time_col)

    a = step2.params.get(x, np.nan)
    sa = step2.bse.get(x, np.nan)
    b = step3.params.get(m, np.nan)
    sb = step3.bse.get(m, np.nan)
    indirect = a * b
    sobel_se = math.sqrt((b ** 2) * (sa ** 2) + (a ** 2) * (sb ** 2)) if pd.notna(a) and pd.notna(b) else np.nan
    sobel_z = indirect / sobel_se if sobel_se not in (0, np.nan) and pd.notna(sobel_se) else np.nan
    sobel_p = 2 * (1 - stats.norm.cdf(abs(sobel_z))) if pd.notna(sobel_z) else np.nan

    summary = pd.concat(
        [
            summarize_model(step1, "mediation_step1_y_on_x", formula1),
            summarize_model(step2, "mediation_step2_m_on_x", formula2),
            summarize_model(step3, "mediation_step3_y_on_x_m", formula3),
        ],
        ignore_index=True,
    )
    effect = pd.DataFrame(
        [
            {
                "path_a_x_to_m": a,
                "path_b_m_to_y": b,
                "indirect_effect": indirect,
                "sobel_se": sobel_se,
                "sobel_z": sobel_z,
                "sobel_p": sobel_p,
                "sobel_stars": significance_stars(sobel_p),
                "direct_effect_after_mediator": step3.params.get(x, np.nan),
                "total_effect_without_mediator": step1.params.get(x, np.nan),
            }
        ]
    )
    return summary, effect


def moderation_analysis(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    y = config["dependent_var"]
    x = config["main_independent_var"]
    z = config["moderator_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]

    mod_df = df.copy()
    interaction = f"{x}_x_{z}"
    mod_df[interaction] = mod_df[x] * mod_df[z]
    result, _, formula = run_fe_regression(mod_df, y, [x, z, interaction, *controls], id_col, time_col)
    summary = summarize_model(result, "moderation_main", formula)
    table = tidy_result(result, "moderation_main")
    return summary, table


def subgroup_regressions(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    subgroup_cfg = config.get("subgroup", {})
    if not subgroup_cfg.get("enabled"):
        return pd.DataFrame(), pd.DataFrame()

    group_col = subgroup_cfg["column"]
    groups = subgroup_cfg.get("groups")
    ensure_columns(df, [group_col], "分组回归")

    y = config["dependent_var"]
    x = config["main_independent_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]

    summaries: List[pd.DataFrame] = []
    tables: List[pd.DataFrame] = []

    iterable = groups if groups else sorted(df[group_col].dropna().unique().tolist())
    for group in iterable:
        subset = df[df[group_col] == group].copy()
        if subset.empty:
            continue
        result, _, formula = run_fe_regression(subset, y, [x, *controls], id_col, time_col)
        model_name = f"subgroup_{group_col}_{group}"
        summaries.append(summarize_model(result, model_name, formula))
        table = tidy_result(result, model_name)
        table.insert(0, "group", group)
        tables.append(table)

    return pd.concat(summaries, ignore_index=True), pd.concat(tables, ignore_index=True)


def robustness_analysis(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    robust_cfg = config.get("robustness", {})
    if not robust_cfg.get("enabled"):
        return pd.DataFrame(), pd.DataFrame()

    y_default = config["dependent_var"]
    x_default = config["main_independent_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]

    summaries: List[pd.DataFrame] = []
    tables: List[pd.DataFrame] = []

    for model_cfg in robust_cfg.get("models", []):
        model_df = df.copy()
        model_name = model_cfg.get("name", "robustness")
        dependent_var = model_cfg.get("dependent_var", y_default)
        main_x = x_default
        if model_cfg.get("lag_main_independent_var"):
            lag = int(model_cfg["lag_main_independent_var"])
            lag_col = f"{x_default}_lag{lag}"
            model_df[lag_col] = model_df.groupby(id_col)[x_default].shift(lag)
            main_x = lag_col
        regressors = model_cfg.get("regressors", [main_x, *controls])
        result, _, formula = run_fe_regression(model_df, dependent_var, regressors, id_col, time_col)
        summaries.append(summarize_model(result, model_name, formula))
        tables.append(tidy_result(result, model_name))

    return pd.concat(summaries, ignore_index=True), pd.concat(tables, ignore_index=True)


def write_outputs(output_dir: Path, results: Dict[str, pd.DataFrame]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    excel_path = output_dir / "analysis_results.xlsx"
    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for sheet_name, df in results.items():
                if df is not None and not df.empty:
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    except Exception:
        pass

    for name, df in results.items():
        if df is not None and not df.empty:
            df.to_csv(output_dir / f"{name}.csv", index=False, encoding="utf-8-sig")


def main() -> None:
    parser = argparse.ArgumentParser(description="面板数据分析流水线：清洗、缩尾、描述统计、相关性、固定效应、中介、调节、分组和稳健性。")
    parser.add_argument("--config", help="JSON 配置文件路径", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    df_raw = read_data(config)
    df_clean, clean_report = clean_data(df_raw, config)
    df_win = apply_winsorize(df_clean, config)

    base_columns = [
        config["dependent_var"],
        config["main_independent_var"],
        config["mediator_var"],
        config["moderator_var"],
        *config.get("controls", []),
    ]
    analysis_columns = [col for col in dict.fromkeys(base_columns) if col in df_win.columns]

    desc = descriptive_statistics(df_win, analysis_columns)
    corr, corr_p, corr_star = correlation_outputs(df_win, analysis_columns)

    main_result, _, main_formula = run_fe_regression(
        df_win,
        config["dependent_var"],
        [config["main_independent_var"], *config.get("controls", [])],
        config["id_col"],
        config["time_col"],
    )
    main_summary = summarize_model(main_result, "main_fe", main_formula)
    main_table = tidy_result(main_result, "main_fe")

    mediation_summary, mediation_effect = mediation_analysis(df_win, config)
    moderation_summary, moderation_table = moderation_analysis(df_win, config)
    subgroup_summary, subgroup_table = subgroup_regressions(df_win, config)
    robustness_summary, robustness_table = robustness_analysis(df_win, config)

    results = {
        "clean_report": clean_report,
        "clean_data_preview": df_clean.head(50),
        "winsorized_data_preview": df_win.head(50),
        "descriptive_stats": desc,
        "correlation": corr.reset_index().rename(columns={"index": "variable"}),
        "correlation_pvalues": corr_p.reset_index().rename(columns={"index": "variable"}),
        "correlation_stars": corr_star.reset_index().rename(columns={"index": "variable"}),
        "main_fe_summary": main_summary,
        "main_fe_coefficients": main_table,
        "mediation_summary": mediation_summary,
        "mediation_effect": mediation_effect,
        "moderation_summary": moderation_summary,
        "moderation_coefficients": moderation_table,
        "subgroup_summary": subgroup_summary,
        "subgroup_coefficients": subgroup_table,
        "robustness_summary": robustness_summary,
        "robustness_coefficients": robustness_table,
    }

    output_dir = Path(config["output_dir"])
    write_outputs(output_dir, results)

    print(f"分析完成，结果已输出到: {output_dir.resolve()}")
    print("已生成文件：")
    for path in sorted(output_dir.glob("*")):
        print(f"- {path.name}")


if __name__ == "__main__":
    main()
