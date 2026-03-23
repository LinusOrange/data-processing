# 面板数据实证分析脚本

这个仓库当前提供的是一个**基于第三方科学计算库**的标准实证分析脚本，使用 `pandas + numpy + scipy + statsmodels + openpyxl` 来完成以下流程：

1. 导入数据 + 清洗
2. 缩尾（winsorize）
3. 描述性统计
4. 相关系数矩阵
5. 固定效应回归（主回归）
6. 中介效应（SA）
7. 调节效应（Media × ROA）
8. 分组回归 + 稳健性检验

## 当前默认口径

脚本已经为 `3.17筛选后数据.xlsx` 预置了默认映射：

- `id_col`：`id`
- `time_col`：`year`
- `dependent_var`：`CrashRisk` → `NCSKEW综合市场总市值平均法`
- `main_independent_var`：`Media` → `管理层中女性成员占比`
- `mediator_var`：`SA` → 按公式推导的融资约束指标
- `moderator_var`：`ROA` → `总资产净利润率ROAA`
- 分组变量：`Property` → `产权性质`
- 稳健性：
  - 替换因变量为 `CrashRisk_DUVOL` → `DUVOL综合市场总市值平均法`
  - 使用 `Media` 的一期滞后项

其中 `SA` 会根据配置中的公式自动生成：

```text
SA = -0.737 * Size + 0.043 * Size^2 - 0.040 * FirmAge
```

## 安装依赖

建议使用 Python 3.10+。

```bash
pip install -r requirements.txt
```

如果你需要更完整的面板模型支持，也可以额外安装：

```bash
pip install linearmodels
```

## 直接运行

如果你直接沿用当前默认映射，只要运行：

```bash
python analysis_pipeline.py
```

如果你想自己调整变量定义，则修改 `config.template.json` 后运行：

```bash
python analysis_pipeline.py --config config.template.json
```

## 输出结果

程序会在 `outputs/` 中生成：

- `analysis_results.xlsx`
- `analysis_results_summary.md`
- `clean_report.csv`
- `descriptive_stats.csv`
- `correlation.csv`
- `correlation_pvalues.csv`
- `correlation_stars.csv`
- `main_fe_summary.csv` / `main_fe_coefficients.csv`
- `mediation_summary.csv` / `mediation_effect.csv`
- `moderation_summary.csv` / `moderation_coefficients.csv`
- `subgroup_summary.csv` / `subgroup_coefficients.csv`
- `robustness_summary.csv` / `robustness_coefficients.csv`

## 方法说明

### 1. 数据处理

- 使用 `pandas.read_excel()` 读取原始 Excel。
- 通过 `variable_aliases` 把中文原始列名映射为脚本内部统一变量名。
- 通过 `derived_variables` 自动构造 `SA` 等衍生变量。
- 对主要变量支持按年份进行分组 winsorize。

### 2. 固定效应回归

脚本使用 `statsmodels` 的公式接口估计：

```python
Y ~ X + Controls + C(id) + C(year)
```

默认使用企业层面聚类稳健标准误：

```json
"regression": {
  "cov_type": "cluster",
  "cluster_col": "id"
}
```

### 3. 中介与调节

- 中介效应：按三步法回归，并额外输出 Sobel 检验。
- 调节效应：自动构造 `Media_x_ROA` 交互项并回归。

### 4. 分组与稳健性

- 分组回归：按 `subgroup.column` 拆分样本后分别做固定效应回归。
- 稳健性检验：支持替换因变量、使用滞后核心解释变量、或自定义回归式中的自变量列表。

## 提示

如果你最终论文里的 `Media`、`CrashRisk` 或 `SA` 口径与当前默认映射不同，请优先修改 `config.template.json` 再运行脚本。
