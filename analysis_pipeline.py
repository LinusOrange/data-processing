from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET
from zipfile import ZipFile
import re


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
    },
    "derived_variables": {
        "SA": {
            "formula": "-0.737 * Size + 0.043 * Size * Size - 0.040 * FirmAge"
        }
    },
}


class AnalysisError(Exception):
    pass


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


def col_letters_to_index(col: str) -> int:
    value = 0
    for ch in col:
        value = value * 26 + (ord(ch.upper()) - 64)
    return value - 1


def parse_shared_strings(zf: ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: List[str] = []
    for si in root:
        text = "".join(node.text or "" for node in si.iter(f"{ns}t"))
        strings.append(text)
    return strings


def get_sheet_path(zf: ZipFile, sheet_name_or_index: Any) -> str:
    ns_main = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    ns_rel = {"r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets = workbook.find("a:sheets", ns_main)
    if sheets is None:
        raise AnalysisError("工作簿中没有可读取的 sheet。")

    sheet_elems = list(sheets)
    if isinstance(sheet_name_or_index, int):
        if sheet_name_or_index >= len(sheet_elems):
            raise AnalysisError(f"sheet 索引超出范围: {sheet_name_or_index}")
        selected = sheet_elems[sheet_name_or_index]
    else:
        selected = next((s for s in sheet_elems if s.attrib.get("name") == sheet_name_or_index), None)
        if selected is None:
            raise AnalysisError(f"未找到 sheet: {sheet_name_or_index}")

    rel_id = selected.attrib.get(f"{{{ns_rel['r']}}}id")
    rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    for rel in rel_root:
        if rel.attrib.get("Id") == rel_id:
            return "xl/" + rel.attrib["Target"]
    raise AnalysisError("无法定位 sheet 的 XML 文件。")


def read_excel_records(file_path: Path, sheet_name_or_index: Any = 0) -> List[Dict[str, Any]]:
    with ZipFile(file_path) as zf:
        shared = parse_shared_strings(zf)
        sheet_path = get_sheet_path(zf, sheet_name_or_index)
        ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        root = ET.fromstring(zf.read(sheet_path))
        sheet_data = root.find("a:sheetData", ns)
        if sheet_data is None:
            return []

        rows: List[Dict[int, Any]] = []
        for row in sheet_data:
            row_map: Dict[int, Any] = {}
            for cell in row:
                ref = cell.attrib.get("r", "")
                match = re.match(r"([A-Z]+)", ref)
                if not match:
                    continue
                idx = col_letters_to_index(match.group(1))
                value_elem = cell.find("a:v", ns)
                value = value_elem.text if value_elem is not None else ""
                cell_type = cell.attrib.get("t")
                if cell_type == "s" and value != "":
                    value = shared[int(value)]
                row_map[idx] = value
            rows.append(row_map)

    if not rows:
        return []
    header_map = rows[0]
    max_idx = max(header_map)
    headers = [str(header_map.get(i, "")).strip() or f"col_{i+1}" for i in range(max_idx + 1)]
    records: List[Dict[str, Any]] = []
    for row_map in rows[1:]:
        record = {headers[i]: row_map.get(i, "") for i in range(len(headers))}
        records.append(record)
    return records


def coerce_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def ensure_columns(records: List[Dict[str, Any]], columns: Sequence[str], label: str) -> None:
    if not records:
        raise AnalysisError("数据为空。")
    missing = [col for col in columns if col and col not in records[0]]
    if missing:
        raise AnalysisError(f"{label} 缺少列: {missing}")


def resolve_name(name: str, aliases: Dict[str, str]) -> str:
    return aliases.get(name, name)


def compile_expression(expr: str):
    code = compile(expr, "<expr>", "eval")
    for name in code.co_names:
        if name not in {"abs", "min", "max", "math"}:
            pass
    return code


def derive_variables(records: List[Dict[str, Any]], config: Dict[str, Any]) -> None:
    aliases = config.get("variable_aliases", {})
    derived = config.get("derived_variables", {})
    compiled: Dict[str, Any] = {}
    for target, spec in derived.items():
        if isinstance(spec, dict) and spec.get("formula"):
            compiled[target] = compile_expression(spec["formula"])
    for row in records:
        env: Dict[str, Any] = {"math": math, "abs": abs, "min": min, "max": max}
        for alias, original in aliases.items():
            env[alias] = coerce_number(row.get(original))
        for target, code in compiled.items():
            try:
                value = eval(code, {"__builtins__": {}}, env)
            except Exception:
                value = None
            row[target] = value
            env[target] = value
        for alias, original in aliases.items():
            if alias not in row:
                row[alias] = row.get(original)


def sort_records(records: List[Dict[str, Any]], id_col: str, time_col: str) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda r: (str(r.get(id_col, "")), coerce_number(r.get(time_col)) or -10**9))


def clean_data(records: List[Dict[str, Any]], config: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    aliases = config.get("variable_aliases", {})
    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)

    required = [
        id_col,
        time_col,
        resolve_name(config["main_independent_var"], aliases),
        resolve_name(config["moderator_var"], aliases),
    ]
    ensure_columns(records, required, "核心变量")

    before_rows = len(records)
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for row in records:
        key = tuple(sorted(row.items()))
        if key not in seen:
            seen.add(key)
            deduped.append(dict(row))

    derive_variables(deduped, config)
    dropna_subset = [resolve_name(name, aliases) for name in config.get("dropna_subset", [])]
    cleaned: List[Dict[str, Any]] = []
    for row in deduped:
        keep = True
        for col in dropna_subset:
            if col in row and (row[col] == "" or row[col] is None):
                keep = False
                break
        if keep:
            cleaned.append(row)

    for row in cleaned:
        for col in set(config.get("winsorize", {}).get("columns", [])) | set(config.get("controls", [])) | {
            config["dependent_var"],
            config["main_independent_var"],
            config["mediator_var"],
            config["moderator_var"],
            config["time_col"],
        }:
            actual = resolve_name(col, aliases)
            if actual in row:
                row[actual] = coerce_number(row.get(actual))

    cleaned = [r for r in cleaned if r.get(id_col) not in (None, "") and r.get(time_col) is not None]
    cleaned = sort_records(cleaned, id_col, time_col)

    report = [
        {"step": "raw_rows", "value": before_rows},
        {"step": "clean_rows", "value": len(cleaned)},
        {"step": "dropped_rows", "value": before_rows - len(cleaned)},
        {"step": "n_entities", "value": len({r[id_col] for r in cleaned})},
        {"step": "n_periods", "value": len({r[time_col] for r in cleaned})},
    ]
    return cleaned, report


def quantile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        raise AnalysisError("无法对空序列计算分位数。")
    pos = (len(sorted_vals) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return sorted_vals[lower]
    return sorted_vals[lower] + (sorted_vals[upper] - sorted_vals[lower]) * (pos - lower)


def apply_winsorize(records: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    winsor_cfg = config.get("winsorize", {})
    if not winsor_cfg.get("enabled", True):
        return [dict(r) for r in records]

    aliases = config.get("variable_aliases", {})
    columns = [resolve_name(c, aliases) for c in winsor_cfg.get("columns", [])]
    time_col = resolve_name(config["time_col"], aliases)
    lower = float(winsor_cfg.get("lower", 0.01))
    upper = float(winsor_cfg.get("upper", 0.99))
    by_time = bool(winsor_cfg.get("by_time", True))

    groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    if by_time:
        for row in records:
            groups[row[time_col]].append(row)
    else:
        groups["all"] = list(records)

    clipped_records = [dict(r) for r in records]
    by_group_idx: Dict[Any, List[int]] = defaultdict(list)
    if by_time:
        for idx, row in enumerate(clipped_records):
            by_group_idx[row[time_col]].append(idx)
    else:
        by_group_idx["all"] = list(range(len(clipped_records)))

    for key, indices in by_group_idx.items():
        for col in columns:
            vals = [coerce_number(clipped_records[i].get(col)) for i in indices]
            valid = sorted(v for v in vals if v is not None)
            if not valid:
                continue
            low, high = quantile(valid, lower), quantile(valid, upper)
            for i in indices:
                val = coerce_number(clipped_records[i].get(col))
                if val is None:
                    continue
                clipped_records[i][col] = min(max(val, low), high)
    return clipped_records


def mean(vals: Sequence[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def variance(vals: Sequence[float], sample: bool = True) -> float:
    if len(vals) < (2 if sample else 1):
        return float("nan")
    mu = mean(vals)
    denom = len(vals) - 1 if sample else len(vals)
    return sum((v - mu) ** 2 for v in vals) / denom


def stddev(vals: Sequence[float], sample: bool = True) -> float:
    var = variance(vals, sample)
    return math.sqrt(var) if var == var and var >= 0 else float("nan")


def skewness(vals: Sequence[float]) -> float:
    n = len(vals)
    if n < 3:
        return float("nan")
    mu = mean(vals)
    s = stddev(vals)
    if not s:
        return 0.0
    return (n / ((n - 1) * (n - 2))) * sum(((v - mu) / s) ** 3 for v in vals)


def kurtosis_excess(vals: Sequence[float]) -> float:
    n = len(vals)
    if n < 4:
        return float("nan")
    mu = mean(vals)
    s = stddev(vals)
    if not s:
        return 0.0
    term1 = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3)) * sum(((v - mu) / s) ** 4 for v in vals)
    term2 = (3 * (n - 1) ** 2) / ((n - 2) * (n - 3))
    return term1 - term2


def percentile(vals: Sequence[float], p: float) -> float:
    return quantile(sorted(vals), p)


def descriptive_statistics(records: List[Dict[str, Any]], columns: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for col in columns:
        vals_all = [coerce_number(r.get(col)) for r in records]
        vals = [v for v in vals_all if v is not None]
        out.append(
            {
                "variable": col,
                "count": len(vals),
                "mean": mean(vals),
                "std": stddev(vals),
                "min": min(vals) if vals else None,
                "p25": percentile(vals, 0.25) if vals else None,
                "p50": percentile(vals, 0.5) if vals else None,
                "p75": percentile(vals, 0.75) if vals else None,
                "max": max(vals) if vals else None,
                "skew": skewness(vals),
                "kurtosis": kurtosis_excess(vals),
                "missing": len(vals_all) - len(vals),
            }
        )
    return out


def normal_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def pearson_corr(x: Sequence[float], y: Sequence[float]) -> float:
    mx, my = mean(x), mean(y)
    sx = math.sqrt(sum((v - mx) ** 2 for v in x))
    sy = math.sqrt(sum((v - my) ** 2 for v in y))
    if sx == 0 or sy == 0:
        return float("nan")
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy)


def correlation_outputs(records: List[Dict[str, Any]], columns: Sequence[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    corr_rows: List[Dict[str, Any]] = []
    p_rows: List[Dict[str, Any]] = []
    star_rows: List[Dict[str, Any]] = []
    for c1 in columns:
        corr_row = {"variable": c1}
        p_row = {"variable": c1}
        star_row = {"variable": c1}
        for c2 in columns:
            pairs = [
                (coerce_number(r.get(c1)), coerce_number(r.get(c2)))
                for r in records
                if coerce_number(r.get(c1)) is not None and coerce_number(r.get(c2)) is not None
            ]
            if len(pairs) < 3:
                corr = pval = float("nan")
            else:
                xs = [a for a, _ in pairs]
                ys = [b for _, b in pairs]
                corr = pearson_corr(xs, ys)
                if corr == corr and abs(corr) < 1 and len(xs) > 3:
                    z = 0.5 * math.log((1 + corr) / (1 - corr)) * math.sqrt(len(xs) - 3)
                    pval = 2 * (1 - normal_cdf(abs(z)))
                else:
                    pval = 0.0 if abs(corr) == 1 else float("nan")
            corr_row[c2] = corr
            p_row[c2] = pval
            stars = "***" if pval == pval and pval < 0.01 else "**" if pval == pval and pval < 0.05 else "*" if pval == pval and pval < 0.1 else ""
            star_row[c2] = f"{corr:.4f}{stars}" if corr == corr else ""
        corr_rows.append(corr_row)
        p_rows.append(p_row)
        star_rows.append(star_row)
    return corr_rows, p_rows, star_rows


def transpose(matrix: List[List[float]]) -> List[List[float]]:
    return [list(row) for row in zip(*matrix)]


def matmul(a: List[List[float]], b: List[List[float]]) -> List[List[float]]:
    bt = transpose(b)
    return [[sum(x * y for x, y in zip(row, col)) for col in bt] for row in a]


def matvec(a: List[List[float]], x: List[float]) -> List[float]:
    return [sum(v * xv for v, xv in zip(row, x)) for row in a]


def invert_matrix(matrix: List[List[float]]) -> List[List[float]]:
    n = len(matrix)
    aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(aug[r][col]))
        if abs(aug[pivot][col]) < 1e-12:
            raise AnalysisError("回归矩阵不可逆，请检查变量是否完全共线。")
        aug[col], aug[pivot] = aug[pivot], aug[col]
        factor = aug[col][col]
        aug[col] = [v / factor for v in aug[col]]
        for r in range(n):
            if r == col:
                continue
            ratio = aug[r][col]
            if ratio == 0:
                continue
            aug[r] = [rv - ratio * cv for rv, cv in zip(aug[r], aug[col])]
    return [row[n:] for row in aug]


def demean_two_way(values: List[float], entity: List[Any], time: List[Any], max_iter: int = 200, tol: float = 1e-10) -> List[float]:
    v = values[:]
    overall = mean(v)
    v = [x - overall for x in v]
    for _ in range(max_iter):
        max_change = 0.0
        groups_e: Dict[Any, List[int]] = defaultdict(list)
        groups_t: Dict[Any, List[int]] = defaultdict(list)
        for i, (e, t) in enumerate(zip(entity, time)):
            groups_e[e].append(i)
            groups_t[t].append(i)
        for groups in (groups_e, groups_t):
            for idxs in groups.values():
                gmean = mean([v[i] for i in idxs])
                for i in idxs:
                    new_val = v[i] - gmean
                    max_change = max(max_change, abs(new_val - v[i]))
                    v[i] = new_val
        if max_change < tol:
            break
    return v


def significance_stars(p: float) -> str:
    if p != p:
        return ""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.1:
        return "*"
    return ""


def run_fe_regression(records: List[Dict[str, Any]], dependent_var: str, regressors: Sequence[str], id_col: str, time_col: str) -> Dict[str, Any]:
    model_rows = []
    for row in records:
        y = coerce_number(row.get(dependent_var))
        xs = [coerce_number(row.get(col)) for col in regressors]
        entity = row.get(id_col)
        time = row.get(time_col)
        if y is None or any(v is None for v in xs) or entity in (None, "") or time is None:
            continue
        model_rows.append((y, xs, entity, time))
    if len(model_rows) <= len(regressors):
        raise AnalysisError(f"模型 {dependent_var} ~ {regressors} 有效样本不足。")

    y = [row[0] for row in model_rows]
    Xcols = list(zip(*[row[1] for row in model_rows]))
    entity = [row[2] for row in model_rows]
    time = [row[3] for row in model_rows]

    y_dm = demean_two_way(y, entity, time)
    X_dm = [demean_two_way(list(col), entity, time) for col in Xcols]
    X = transpose(X_dm)
    Xt = transpose(X)
    XtX = matmul(Xt, X)
    XtX_inv = invert_matrix(XtX)
    Xty = [sum(col[i] * y_dm[i] for i in range(len(y_dm))) for col in Xt]
    beta = matvec(XtX_inv, Xty)
    fitted = [sum(beta[j] * X[i][j] for j in range(len(beta))) for i in range(len(X))]
    resid = [y_dm[i] - fitted[i] for i in range(len(y_dm))]
    n = len(y_dm)
    k = len(beta)
    dof = max(n - k, 1)

    meat = [[0.0 for _ in range(k)] for _ in range(k)]
    for i in range(n):
        xi = X[i]
        scale = resid[i] ** 2
        for a in range(k):
            for b in range(k):
                meat[a][b] += xi[a] * xi[b] * scale
    cov = matmul(matmul(XtX_inv, meat), XtX_inv)
    hc1 = n / dof
    cov = [[v * hc1 for v in row] for row in cov]
    se = [math.sqrt(max(cov[i][i], 0.0)) for i in range(k)]
    tvals = [beta[i] / se[i] if se[i] else float("nan") for i in range(k)]
    pvals = [2 * (1 - normal_cdf(abs(t))) if t == t else float("nan") for t in tvals]

    sst = sum(val ** 2 for val in y_dm)
    sse = sum(e ** 2 for e in resid)
    r2 = 1 - sse / sst if sst else float("nan")
    adj_r2 = 1 - (1 - r2) * (n - 1) / dof if r2 == r2 and dof else float("nan")

    coef_rows = []
    for idx, term in enumerate(regressors):
        coef_rows.append(
            {
                "term": term,
                "coef": beta[idx],
                "std_err": se[idx],
                "t_value": tvals[idx],
                "p_value": pvals[idx],
                "stars": significance_stars(pvals[idx]),
            }
        )

    return {
        "nobs": n,
        "r_squared": r2,
        "adj_r_squared": adj_r2,
        "coefficients": coef_rows,
        "formula": f"{dependent_var} ~ {' + '.join(regressors)} | entity FE + time FE",
        "residual_ss": sse,
    }


def summarize_model(result: Dict[str, Any], model_name: str) -> List[Dict[str, Any]]:
    return [{
        "model": model_name,
        "formula": result["formula"],
        "nobs": result["nobs"],
        "r_squared": result["r_squared"],
        "adj_r_squared": result["adj_r_squared"],
    }]


def tidy_result(result: Dict[str, Any], model_name: str) -> List[Dict[str, Any]]:
    rows = []
    for row in result["coefficients"]:
        rows.append({**row, "model": model_name, "nobs": result["nobs"], "r_squared": result["r_squared"], "adj_r_squared": result["adj_r_squared"]})
    return rows


def mediation_analysis(records: List[Dict[str, Any]], config: Dict[str, Any], aliases: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    y = resolve_name(config["dependent_var"], aliases)
    x = resolve_name(config["main_independent_var"], aliases)
    m = resolve_name(config["mediator_var"], aliases)
    controls = [resolve_name(c, aliases) for c in config.get("controls", [])]
    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)

    step1 = run_fe_regression(records, y, [x, *controls], id_col, time_col)
    step2 = run_fe_regression(records, m, [x, *controls], id_col, time_col)
    step3 = run_fe_regression(records, y, [x, m, *controls], id_col, time_col)

    coef2 = {row["term"]: row for row in step2["coefficients"]}
    coef3 = {row["term"]: row for row in step3["coefficients"]}
    coef1 = {row["term"]: row for row in step1["coefficients"]}
    a = coef2[x]["coef"]
    sa = coef2[x]["std_err"]
    b = coef3[m]["coef"]
    sb = coef3[m]["std_err"]
    indirect = a * b
    sobel_se = math.sqrt((b * b * sa * sa) + (a * a * sb * sb)) if sa == sa and sb == sb else float("nan")
    sobel_z = indirect / sobel_se if sobel_se not in (0, float("nan")) and sobel_se == sobel_se else float("nan")
    sobel_p = 2 * (1 - normal_cdf(abs(sobel_z))) if sobel_z == sobel_z else float("nan")

    summary = []
    summary.extend(summarize_model(step1, "mediation_step1_y_on_x"))
    summary.extend(summarize_model(step2, "mediation_step2_m_on_x"))
    summary.extend(summarize_model(step3, "mediation_step3_y_on_x_m"))
    effect = [{
        "path_a_x_to_m": a,
        "path_b_m_to_y": b,
        "indirect_effect": indirect,
        "sobel_se": sobel_se,
        "sobel_z": sobel_z,
        "sobel_p": sobel_p,
        "sobel_stars": significance_stars(sobel_p),
        "direct_effect_after_mediator": coef3[x]["coef"],
        "total_effect_without_mediator": coef1[x]["coef"],
    }]
    return summary, effect


def moderation_analysis(records: List[Dict[str, Any]], config: Dict[str, Any], aliases: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    y = resolve_name(config["dependent_var"], aliases)
    x = resolve_name(config["main_independent_var"], aliases)
    z = resolve_name(config["moderator_var"], aliases)
    controls = [resolve_name(c, aliases) for c in config.get("controls", [])]
    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)

    rows = [dict(r) for r in records]
    interaction = f"{x}_x_{z}"
    for row in rows:
        xv = coerce_number(row.get(x))
        zv = coerce_number(row.get(z))
        row[interaction] = xv * zv if xv is not None and zv is not None else None

    result = run_fe_regression(rows, y, [x, z, interaction, *controls], id_col, time_col)
    return summarize_model(result, "moderation_main"), tidy_result(result, "moderation_main")


def subgroup_regressions(records: List[Dict[str, Any]], config: Dict[str, Any], aliases: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    subgroup = config.get("subgroup", {})
    if not subgroup.get("enabled"):
        return [], []
    group_col = resolve_name(subgroup["column"], aliases)
    y = resolve_name(config["dependent_var"], aliases)
    x = resolve_name(config["main_independent_var"], aliases)
    controls = [resolve_name(c, aliases) for c in config.get("controls", [])]
    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)

    summaries: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    groups = subgroup.get("groups") or sorted({r.get(group_col) for r in records if r.get(group_col) not in (None, "")})
    for group in groups:
        subset = [
            r
            for r in records
            if r.get(group_col) == group or str(r.get(group_col)) == str(group)
        ]
        if not subset:
            continue
        result = run_fe_regression(subset, y, [x, *controls], id_col, time_col)
        model_name = f"subgroup_{group_col}_{group}"
        for row in summarize_model(result, model_name):
            row["group"] = group
            summaries.append(row)
        for row in tidy_result(result, model_name):
            row["group"] = group
            tables.append(row)
    return summaries, tables


def robustness_analysis(records: List[Dict[str, Any]], config: Dict[str, Any], aliases: Dict[str, str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    robust = config.get("robustness", {})
    if not robust.get("enabled"):
        return [], []
    y_default = resolve_name(config["dependent_var"], aliases)
    x_default = resolve_name(config["main_independent_var"], aliases)
    controls = [resolve_name(c, aliases) for c in config.get("controls", [])]
    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)

    summaries: List[Dict[str, Any]] = []
    tables: List[Dict[str, Any]] = []
    for spec in robust.get("models", []):
        model_name = spec.get("name", "robustness")
        dependent_var = resolve_name(spec.get("dependent_var", y_default), aliases)
        regressors = [x_default, *controls]
        model_records = [dict(r) for r in records]
        if spec.get("lag_main_independent_var"):
            lag = int(spec["lag_main_independent_var"])
            lag_col = f"{x_default}_lag{lag}"
            grouped: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
            for row in model_records:
                grouped[row[id_col]].append(row)
            for grp in grouped.values():
                grp.sort(key=lambda r: r[time_col])
                values = [coerce_number(r.get(x_default)) for r in grp]
                for idx, row in enumerate(grp):
                    row[lag_col] = values[idx - lag] if idx >= lag else None
            regressors = [lag_col, *controls]
        result = run_fe_regression(model_records, dependent_var, regressors, id_col, time_col)
        summaries.extend(summarize_model(result, model_name))
        tables.extend(tidy_result(result, model_name))
    return summaries, tables


def preview_rows(records: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    return records[:limit]


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_outputs(output_dir: Path, results: Dict[str, List[Dict[str, Any]]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in results.items():
        if rows:
            write_csv(output_dir / f"{name}.csv", rows)


def print_key_results(main_table: List[Dict[str, Any]], mediation_effect: List[Dict[str, Any]], moderation_table: List[Dict[str, Any]], subgroup_summary: List[Dict[str, Any]], robustness_summary: List[Dict[str, Any]]) -> None:
    print("\n=== 主回归核心系数 ===")
    for row in main_table:
        if row["term"] == DEFAULT_CONFIG["main_independent_var"] or row["term"] == resolve_name(DEFAULT_CONFIG["main_independent_var"], DEFAULT_CONFIG["variable_aliases"]):
            print(json.dumps(row, ensure_ascii=False))
    print("\n=== 中介效应 ===")
    for row in mediation_effect:
        print(json.dumps(row, ensure_ascii=False))
    print("\n=== 调节效应交互项 ===")
    for row in moderation_table:
        if "_x_" in row["term"]:
            print(json.dumps(row, ensure_ascii=False))
    print("\n=== 分组回归摘要 ===")
    for row in subgroup_summary:
        print(json.dumps(row, ensure_ascii=False))
    print("\n=== 稳健性回归摘要 ===")
    for row in robustness_summary:
        print(json.dumps(row, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Excel 面板数据分析：清洗、缩尾、描述统计、相关性、固定效应、中介、调节、分组和稳健性。")
    parser.add_argument("--config", default=None, help="可选 JSON 配置路径")
    args = parser.parse_args()

    config = load_config(args.config)
    file_path = Path(config["file_path"])
    if not file_path.exists():
        raise AnalysisError(f"未找到数据文件: {file_path}")

    records = read_excel_records(file_path, config.get("sheet_name", 0))
    cleaned, clean_report = clean_data(records, config)
    winsorized = apply_winsorize(cleaned, config)

    aliases = config.get("variable_aliases", {})
    analysis_columns = [resolve_name(name, aliases) for name in [config["dependent_var"], config["main_independent_var"], config["mediator_var"], config["moderator_var"], *config.get("controls", [])]]

    desc = descriptive_statistics(winsorized, analysis_columns)
    corr, corr_p, corr_star = correlation_outputs(winsorized, analysis_columns)

    id_col = resolve_name(config["id_col"], aliases)
    time_col = resolve_name(config["time_col"], aliases)
    dep = resolve_name(config["dependent_var"], aliases)
    x = resolve_name(config["main_independent_var"], aliases)
    controls = [resolve_name(c, aliases) for c in config.get("controls", [])]

    main_result = run_fe_regression(winsorized, dep, [x, *controls], id_col, time_col)
    main_summary = summarize_model(main_result, "main_fe")
    main_table = tidy_result(main_result, "main_fe")

    mediation_summary, mediation_effect = mediation_analysis(winsorized, config, aliases)
    moderation_summary, moderation_table = moderation_analysis(winsorized, config, aliases)
    subgroup_summary, subgroup_table = subgroup_regressions(winsorized, config, aliases)
    robustness_summary, robustness_table = robustness_analysis(winsorized, config, aliases)

    results = {
        "clean_report": clean_report,
        "clean_data_preview": preview_rows(cleaned),
        "winsorized_data_preview": preview_rows(winsorized),
        "descriptive_stats": desc,
        "correlation": corr,
        "correlation_pvalues": corr_p,
        "correlation_stars": corr_star,
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
    for path in sorted(output_dir.glob("*.csv")):
        print(f"- {path.name}")
    print_key_results(main_table, mediation_effect, moderation_table, subgroup_summary, robustness_summary)


if __name__ == "__main__":
    main()
