from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats


DEFAULT_CONFIG: Dict[str, Any] = {
    "file_path": "./3.17筛选后数据.xlsx",
    "sheet_name": 0,
    "output_dir": "./outputs",
    "id_col": "id",
    "time_col": "year",
    "dependent_var": "CrashRisk",
    "main_independent_var": "Media",
    "mediator_var": "SA",
    "moderator_var": "ROA",
    "controls": [
        "Size",
        "Lev",
        "Inst",
        "Dual",
        "TopMgmtSize",
        "Turnover",
        "FirmAge",
        "ESG",
    ],
    "dropna_subset": ["id", "year", "CrashRisk", "Media"],
    "winsorize": {
        "enabled": True,
        "columns": [
            "CrashRisk",
            "Media",
            "SA",
            "ROA",
            "Size",
            "Lev",
            "Inst",
            "TopMgmtSize",
            "Turnover",
            "FirmAge",
            "ESG",
        ],
        "lower": 0.01,
        "upper": 0.99,
        "by_time": True,
    },
    "cleaning": {
        "drop_duplicates": True,
        "sort": True,
    },
    "regression": {
        "cov_type": "cluster",
        "cluster_col": "id",
    },
    "subgroup": {
        "enabled": True,
        "column": "Property",
        "groups": [0, 1],
    },
    "robustness": {
        "enabled": True,
        "models": [
            {
                "name": "alt_duvol",
                "dependent_var": "CrashRisk_DUVOL",
            },
            {
                "name": "lag_media",
                "lag_main_independent_var": 1,
            },
        ],
    },
    "variable_aliases": {
        "CrashRisk": "NCSKEW综合市场总市值平均法",
        "CrashRisk_DUVOL": "DUVOL综合市场总市值平均法",
        "Media": "管理层中女性成员占比",
        "ROA": "总资产净利润率ROAA",
        "Size": "企业规模",
        "Lev": "资产负债率",
        "Inst": "机构投资者持股比例",
        "Dual": "两职合一",
        "TopMgmtSize": "董监高对数",
        "Turnover": "年换手率总股数对数",
        "FirmAge": "企业年龄",
        "ESG": "ESG得分年均值",
        "Property": "产权性质",
        "id": "id",
        "year": "year",
    },
    "derived_variables": {
        "SA": {
            "formula": "-0.737 * Size + 0.043 * Size * Size - 0.040 * FirmAge"
        }
    },
}


class AnalysisError(Exception):
    """Raised when input data or config is invalid."""


def merge_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> None:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge_dict(base[key], value)
        else:
            base[key] = value


def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if config_path:
        with open(config_path, "r", encoding="utf-8") as fh:
            user_config = json.load(fh)
        merge_dict(config, user_config)
    return config


def read_data(config: Dict[str, Any]) -> pd.DataFrame:
    file_path = Path(config["file_path"])
    if not file_path.exists():
        raise AnalysisError(f"未找到数据文件: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, sheet_name=config.get("sheet_name", 0))
    if suffix == ".csv":
        return pd.read_csv(file_path)
    raise AnalysisError(f"暂不支持的文件格式: {suffix}")


def ensure_columns(df: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [col for col in columns if col and col not in df.columns]
    if missing:
        raise AnalysisError(f"{label} 缺少列: {missing}")


def prepare_alias_columns(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    aliases = config.get("variable_aliases", {})
    out = df.copy()
    for alias, actual in aliases.items():
        if alias in out.columns:
            continue
        if actual not in out.columns:
            raise AnalysisError(f"别名映射缺少原始列: {actual} -> {alias}")
        out[alias] = out[actual]
    return out


def derive_variables(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for target, spec in (config.get("derived_variables") or {}).items():
        formula = spec.get("formula") if isinstance(spec, dict) else None
        if not formula:
            continue
        out[target] = out.eval(formula, engine="python")
    return out


def to_numeric_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def clean_data(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = prepare_alias_columns(df, config)
    df = derive_variables(df, config)

    id_col = config["id_col"]
    time_col = config["time_col"]
    dependent_var = config["dependent_var"]
    main_independent_var = config["main_independent_var"]
    mediator_var = config["mediator_var"]
    moderator_var = config["moderator_var"]
    controls = config.get("controls", [])
    subgroup_cfg = config.get("subgroup", {})

    ensure_columns(
        df,
        [id_col, time_col, dependent_var, main_independent_var, mediator_var, moderator_var, *controls],
        "核心分析",
    )

    numeric_cols = {
        time_col,
        dependent_var,
        main_independent_var,
        mediator_var,
        moderator_var,
        *controls,
        *(config.get("winsorize", {}).get("columns") or []),
    }
    numeric_cols.update(
        col
        for col in (config.get("dropna_subset") or [])
        if col != subgroup_cfg.get("column")
    )
    df = to_numeric_columns(df, sorted(numeric_cols))

    cleaning_cfg = config.get("cleaning", {})
    before_rows = len(df)
    if cleaning_cfg.get("drop_duplicates", True):
        df = df.drop_duplicates().copy()

    dropna_subset = config.get("dropna_subset") or [id_col, time_col, dependent_var, main_independent_var]
    ensure_columns(df, dropna_subset, "缺失值处理")
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
        return df.copy()

    columns = winsor_cfg.get("columns") or [
        config["dependent_var"],
        config["main_independent_var"],
        config["mediator_var"],
        config["moderator_var"],
        *config.get("controls", []),
    ]
    ensure_columns(df, columns, "缩尾处理")

    lower = float(winsor_cfg.get("lower", 0.01))
    upper = float(winsor_cfg.get("upper", 0.99))
    by_time = bool(winsor_cfg.get("by_time", True))
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
    for row in corr.index:
        for col in corr.columns:
            star_corr.loc[row, col] = f"{corr.loc[row, col]:.4f}{significance_stars(pvals.loc[row, col])}"
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
    cov_type: str,
    cluster_col: Optional[str],
):
    use_cols = [dependent_var, *regressors, id_col, time_col]
    if cluster_col:
        use_cols.append(cluster_col)
    use_cols = list(dict.fromkeys(use_cols))
    ensure_columns(df, use_cols, "固定效应回归")

    model_df = df[use_cols].dropna().copy()
    formula = build_formula(dependent_var, regressors, id_col, time_col)
    model = smf.ols(formula=formula, data=model_df)

    if cov_type == "cluster" and cluster_col:
        result = model.fit(cov_type="cluster", cov_kwds={"groups": model_df[cluster_col]})
    else:
        result = model.fit(cov_type=cov_type)
    return result, model_df, formula


def display_name_map(config: Dict[str, Any]) -> Dict[str, str]:
    alias_map = config.get("variable_aliases", {})
    display = {alias: actual for alias, actual in alias_map.items()}
    display[config["mediator_var"]] = config["mediator_var"]
    return display


def display_term(term: str, mapping: Dict[str, str]) -> str:
    if term in mapping:
        return mapping[term]
    if "_x_" in term:
        parts = term.split("_x_")
        return " × ".join(mapping.get(part, part) for part in parts)
    if term.endswith("_lag1"):
        base = term[:-5]
        return f"{mapping.get(base, base)}_lag1"
    return term


def tidy_result(result, model_name: str, config: Dict[str, Any]) -> pd.DataFrame:
    mapping = display_name_map(config)
    table = pd.DataFrame(
        {
            "term": result.params.index,
            "coef": result.params.values,
            "std_err": result.bse.values,
            "t_value": result.tvalues.values,
            "p_value": result.pvalues.values,
        }
    )
    table = table[~table["term"].isin(["Intercept"])]
    table["term"] = table["term"].map(lambda x: display_term(x, mapping))
    table["stars"] = table["p_value"].map(significance_stars)
    table["model"] = model_name
    table["nobs"] = int(result.nobs)
    table["r_squared"] = result.rsquared
    table["adj_r_squared"] = result.rsquared_adj
    return table.reset_index(drop=True)


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


def regression_settings(config: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    reg_cfg = config.get("regression", {})
    cov_type = reg_cfg.get("cov_type", "HC1")
    cluster_col = reg_cfg.get("cluster_col") if cov_type == "cluster" else None
    return cov_type, cluster_col


def mediation_analysis(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    y = config["dependent_var"]
    x = config["main_independent_var"]
    m = config["mediator_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]
    cov_type, cluster_col = regression_settings(config)

    step1, _, formula1 = run_fe_regression(df, y, [x, *controls], id_col, time_col, cov_type, cluster_col)
    step2, _, formula2 = run_fe_regression(df, m, [x, *controls], id_col, time_col, cov_type, cluster_col)
    step3, _, formula3 = run_fe_regression(df, y, [x, m, *controls], id_col, time_col, cov_type, cluster_col)

    a = step2.params.get(x, np.nan)
    sa = step2.bse.get(x, np.nan)
    b = step3.params.get(m, np.nan)
    sb = step3.bse.get(m, np.nan)
    indirect = a * b
    sobel_se = math.sqrt((b**2) * (sa**2) + (a**2) * (sb**2)) if pd.notna(a) and pd.notna(b) else np.nan
    sobel_z = indirect / sobel_se if pd.notna(sobel_se) and sobel_se != 0 else np.nan
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
    cov_type, cluster_col = regression_settings(config)

    mod_df = df.copy()
    interaction = f"{x}_x_{z}"
    mod_df[interaction] = mod_df[x] * mod_df[z]

    result, _, formula = run_fe_regression(
        mod_df,
        y,
        [x, z, interaction, *controls],
        id_col,
        time_col,
        cov_type,
        cluster_col,
    )
    summary = summarize_model(result, "moderation_main", formula)
    table = tidy_result(result, "moderation_main", config)
    return summary, table


def subgroup_regressions(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    subgroup_cfg = config.get("subgroup", {})
    if not subgroup_cfg.get("enabled"):
        return pd.DataFrame(), pd.DataFrame()

    group_col = subgroup_cfg["column"]
    ensure_columns(df, [group_col], "分组回归")

    y = config["dependent_var"]
    x = config["main_independent_var"]
    controls = config.get("controls", [])
    id_col = config["id_col"]
    time_col = config["time_col"]
    cov_type, cluster_col = regression_settings(config)

    groups = subgroup_cfg.get("groups")
    iterable = groups if groups else sorted(df[group_col].dropna().unique().tolist())
    summaries: List[pd.DataFrame] = []
    tables: List[pd.DataFrame] = []

    for group in iterable:
        subset = df[df[group_col].astype(str) == str(group)].copy()
        if subset.empty:
            continue
        result, _, formula = run_fe_regression(
            subset,
            y,
            [x, *controls],
            id_col,
            time_col,
            cov_type,
            cluster_col,
        )
        model_name = f"subgroup_{group_col}_{group}"
        summary = summarize_model(result, model_name, formula)
        summary.insert(0, "group", group)
        table = tidy_result(result, model_name, config)
        table.insert(0, "group", group)
        summaries.append(summary)
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
    cov_type, cluster_col = regression_settings(config)

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
        result, _, formula = run_fe_regression(
            model_df,
            dependent_var,
            regressors,
            id_col,
            time_col,
            cov_type,
            cluster_col,
        )
        summaries.append(summarize_model(result, model_name, formula))
        tables.append(tidy_result(result, model_name, config))

    return pd.concat(summaries, ignore_index=True), pd.concat(tables, ignore_index=True)


def write_outputs(output_dir: Path, results: Dict[str, pd.DataFrame]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    excel_path = output_dir / "analysis_results.xlsx"
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        for sheet_name, df in results.items():
            if df is not None and not df.empty:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    for name, df in results.items():
        if df is not None and not df.empty:
            df.to_csv(output_dir / f"{name}.csv", index=False, encoding="utf-8-sig")


def write_result_summary(output_dir: Path, clean_report: pd.DataFrame, main_table: pd.DataFrame, mediation_effect: pd.DataFrame, moderation_table: pd.DataFrame) -> None:
    summary_path = output_dir / "analysis_results_summary.md"
    media_row = main_table[main_table["term"] == "管理层中女性成员占比"].head(1)
    interaction_row = moderation_table[moderation_table["term"].str.contains("×", na=False)].head(1)

    sample_rows = {row["step"]: row["value"] for _, row in clean_report.iterrows()}
    lines = [
        "# 实证结果摘要",
        "",
        "## 样本信息",
        f"- 清洗后样本量：{int(sample_rows.get('clean_rows', 0))}",
        f"- 个体数：{int(sample_rows.get('n_entities', 0))}",
        f"- 年份数：{int(sample_rows.get('n_periods', 0))}",
        "",
    ]
    if not media_row.empty:
        row = media_row.iloc[0]
        lines.extend(
            [
                "## 主回归（Media）",
                f"- 系数：{row['coef']:.6f}",
                f"- p 值：{row['p_value']:.6f}",
                f"- R²：{row['r_squared']:.6f}",
                "",
            ]
        )
    if not mediation_effect.empty:
        row = mediation_effect.iloc[0]
        lines.extend(
            [
                "## 中介效应（SA）",
                f"- 间接效应：{row['indirect_effect']:.6f}",
                f"- Sobel p 值：{row['sobel_p']:.6f}",
                "",
            ]
        )
    if not interaction_row.empty:
        row = interaction_row.iloc[0]
        lines.extend(
            [
                "## 调节效应（Media × ROA）",
                f"- 交互项系数：{row['coef']:.6f}",
                f"- p 值：{row['p_value']:.6f}",
                "",
            ]
        )

    summary_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="基于 pandas/scipy/statsmodels 的面板数据分析流水线。")
    parser.add_argument("--config", help="JSON 配置文件路径", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    df_raw = read_data(config)
    df_clean, clean_report = clean_data(df_raw, config)
    df_win = apply_winsorize(df_clean, config)

    analysis_columns = [
        config["dependent_var"],
        config["main_independent_var"],
        config["mediator_var"],
        config["moderator_var"],
        *config.get("controls", []),
    ]
    analysis_columns = [col for col in dict.fromkeys(analysis_columns) if col in df_win.columns]

    desc = descriptive_statistics(df_win, analysis_columns)
    corr, corr_p, corr_star = correlation_outputs(df_win, analysis_columns)

    cov_type, cluster_col = regression_settings(config)
    main_result, _, main_formula = run_fe_regression(
        df_win,
        config["dependent_var"],
        [config["main_independent_var"], *config.get("controls", [])],
        config["id_col"],
        config["time_col"],
        cov_type,
        cluster_col,
    )
    main_summary = summarize_model(main_result, "main_fe", main_formula)
    main_table = tidy_result(main_result, "main_fe", config)

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
    write_result_summary(output_dir, clean_report, main_table, mediation_effect, moderation_table)

    print(f"分析完成，结果已输出到: {output_dir.resolve()}")
    print("已生成文件：")
    for path in sorted(output_dir.glob("*")):
        print(f"- {path.name}")


if __name__ == "__main__":
    main()
