# 面板数据实证分析脚本

这个仓库现在提供了一个可直接改配置后运行的 Python 分析脚本，用来完成你提到的整套实证流程：

1. 导入数据 + 清洗
2. 缩尾（winsorize）
3. 描述性统计
4. 相关系数矩阵
5. 固定效应回归（主回归）
6. 中介效应（SA）
7. 调节效应（Media × ROA）
8. 分组回归 + 稳健性检验

## 重要说明

我当前无法直接访问你 IDE 里提到的 Windows 本地路径：

`C:\Users\58452\OneDrive\研究生工作\3.17筛选后数据.xlsx`

因此，我已经把代码写成了**可直接读取 Excel 的通用分析流水线**。你只需要把 Excel 文件复制到当前仓库目录，或在配置文件里把 `file_path` 改成仓库内的实际路径，就可以一键跑出结果。

## 文件说明

- `analysis_pipeline.py`：主分析脚本。
- `config.template.json`：配置模板，请按你的真实变量名修改。

## 安装依赖

建议使用 Python 3.10+。

```bash
pip install pandas numpy scipy statsmodels openpyxl
```

## 使用步骤

### 1）准备数据

把 Excel 文件放到仓库根目录，例如：

```text
/workspace/data-processing/3.17筛选后数据.xlsx
```

### 2）修改配置

复制模板：

```bash
cp config.template.json config.json
```

然后按你的数据真实列名修改这些字段：

- `id_col`：企业/个体 ID
- `time_col`：年份
- `dependent_var`：被解释变量
- `main_independent_var`：核心解释变量（例如 `Media`）
- `mediator_var`：中介变量（例如 `SA`）
- `moderator_var`：调节变量（例如 `ROA`）
- `controls`：控制变量列表
- `subgroup.column`：分组变量
- `robustness.models`：稳健性检验设定

### 3）运行脚本

```bash
python analysis_pipeline.py --config config.json
```

## 输出结果

程序会在 `output_dir` 指定目录下生成：

- `analysis_results.xlsx`：整合后的 Excel 结果文件
- `clean_report.csv`：清洗报告
- `descriptive_stats.csv`：描述性统计
- `correlation.csv`：相关系数矩阵
- `correlation_pvalues.csv`：相关系数显著性 p 值
- `correlation_stars.csv`：带显著性星号的相关系数矩阵
- `main_fe_summary.csv` / `main_fe_coefficients.csv`：主回归
- `mediation_summary.csv` / `mediation_effect.csv`：中介效应
- `moderation_summary.csv` / `moderation_coefficients.csv`：调节效应
- `subgroup_summary.csv` / `subgroup_coefficients.csv`：分组回归
- `robustness_summary.csv` / `robustness_coefficients.csv`：稳健性检验

## 方法说明

### 固定效应回归

脚本使用以下方式控制双向固定效应：

- `C(id_col)`：个体固定效应
- `C(time_col)`：时间固定效应

即使用虚拟变量形式实现：

```python
Y ~ X + Controls + C(firm_id) + C(year)
```

### 中介效应（SA）

脚本自动完成三步法：

1. `Y ~ Media + Controls + FE`
2. `SA ~ Media + Controls + FE`
3. `Y ~ Media + SA + Controls + FE`

并额外给出：

- `indirect_effect = a × b`
- Sobel 检验统计量与 p 值

### 调节效应（Media × ROA）

脚本自动构造交互项：

```python
Media_x_ROA = Media * ROA
```

再回归：

```python
Y ~ Media + ROA + Media_x_ROA + Controls + FE
```

### 分组回归

按 `subgroup.column` 指定字段拆分样本，对每组分别做固定效应回归。

### 稳健性检验

模板里预置了两个例子：

- 更换被解释变量 `Y_alt`
- 使用核心解释变量的滞后一期 `Media_lag1`

你可以在 `robustness.models` 里继续扩展。

## 如果你希望我继续

如果你把 Excel 文件放进仓库，或者告诉我**真实列名**，我下一步可以继续帮你：

1. 直接把 `config.json` 按你的变量名配好；
2. 帮你进一步补充符合论文格式的回归表；
3. 如果仓库里能访问到 Excel，我也可以继续帮你把真实结果跑出来。
