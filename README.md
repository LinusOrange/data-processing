# 面板数据实证分析脚本

这个仓库现在包含一个**不依赖 pandas / numpy / scipy / statsmodels** 的纯 Python 分析脚本，可以直接读取 `3.17筛选后数据.xlsx` 并完成以下流程：

1. 导入数据 + 清洗
2. 缩尾（winsorize）
3. 描述性统计
4. 相关系数矩阵
5. 固定效应回归（主回归）
6. 中介效应（SA）
7. 调节效应（Media × ROA）
8. 分组回归 + 稳健性检验

## 当前默认口径

脚本已经按当前 Excel 数据做了默认映射：

- `id_col`：`id`
- `time_col`：`year`
- `dependent_var`：`CrashRisk` → `NCSKEW综合市场总市值平均法`
- `main_independent_var`：`Media` → `管理层中女性成员占比`
- `mediator_var`：`SA` → 按公式推导的融资约束指标
- `moderator_var`：`ROA` → `总资产净利润率ROAA`
- 分组变量：`Property` → `产权性质`
- 稳健性：
  - 替换被解释变量为 `CrashRisk_DUVOL` → `DUVOL综合市场总市值平均法`
  - 使用 `Media` 的一期滞后项

> `SA` 在原始 Excel 中不存在，脚本会根据配置里的公式自动生成：
>
> `SA = -0.737 * Size + 0.043 * Size^2 - 0.040 * FirmAge`

## 直接运行

```bash
python analysis_pipeline.py
```

如果你想自己调整变量映射：

```bash
python analysis_pipeline.py --config config.template.json
```

## 输出文件

运行后会在 `outputs/` 中生成：

- `clean_report.csv`
- `clean_data_preview.csv`
- `winsorized_data_preview.csv`
- `descriptive_stats.csv`
- `correlation.csv`
- `correlation_pvalues.csv`
- `correlation_stars.csv`
- `main_fe_summary.csv`
- `main_fe_coefficients.csv`
- `mediation_summary.csv`
- `mediation_effect.csv`
- `moderation_summary.csv`
- `moderation_coefficients.csv`
- `subgroup_summary.csv`
- `subgroup_coefficients.csv`
- `robustness_summary.csv`
- `robustness_coefficients.csv`

## 方法实现说明

### 1. 数据读取

脚本直接解析 `.xlsx` 文件底层 XML，因此在当前环境下不需要额外安装第三方包。

### 2. 缩尾

默认对主要变量按年份进行 1% / 99% 缩尾。

### 3. 固定效应回归

采用**双向固定效应去均值（entity FE + time FE）**方式估计主回归、分组回归与稳健性回归，并输出 HC1 异方差稳健标准误。

### 4. 中介效应

按三步法执行：

1. `Y ~ Media + Controls + FE`
2. `SA ~ Media + Controls + FE`
3. `Y ~ Media + SA + Controls + FE`

同时输出 Sobel 检验结果。

### 5. 调节效应

自动构造交互项：

`Media_x_ROA = Media * ROA`

然后估计：

`Y ~ Media + ROA + Media_x_ROA + Controls + FE`

## 当前样例结果摘要

基于当前 Excel 直接运行得到：

- 主回归中 `Media` 系数约为 `-0.0497`，显著性不强。
- 中介效应 `a×b` 约为 `-0.0136`，Sobel `p≈0.0557`，边际显著。
- 调节项 `Media × ROA` 系数约为 `-1.8975`，不显著。
- 分组回归已按 `产权性质 = 0/1` 输出。
- 稳健性检验已输出 `DUVOL` 替代口径和 `Media` 滞后一期口径。

## 可继续扩展

如果你后续要把变量口径改成论文中的其他定义，只需要改 `config.template.json` 的：

- `variable_aliases`
- `derived_variables`
- `controls`
- `robustness.models`

即可复用整套流程。
