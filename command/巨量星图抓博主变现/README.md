# 抖音KOL营收数据导出工具

## 功能特点

✨ **灵活的时间配置**：支持任意时间范围的数据导出
📊 **自动生成列**：根据指定的时间范围自动生成对应的月份列
📈 **多维度数据**：包含视频数量、营收金额、报价信息等
📋 **Excel导出**：生成包含多个工作表的Excel文件
🎯 **ID范围筛选**：支持按KOL ID范围筛选数据

## 文件说明

- `tongbu_douyin.py` - 主要的数据导出类
- `export_example.py` - 使用示例和交互式菜单
- `config.ini` - 配置文件（用于爬虫）
- `xingtu_spider.py` - 星图爬虫（数据采集）

## 快速开始

### 1. 基本使用

```python
from tongbu_douyin import DouYinDataExporter

# 创建导出器
exporter = DouYinDataExporter(output_dir="data")

# 导出最近6个月的数据
filepath = exporter.export_recent_months(months_count=6)
print(f"文件已保存到: {filepath}")
```

### 2. 指定时间范围

```python
# 导出2025年3月到7月的数据
filepath = exporter.export_kol_revenue_data(
    start_year=2025,
    start_month=3,
    end_year=2025,
    end_month=7
)
```

### 3. 限制KOL范围

```python
# 只导出ID在100-516之间的KOL数据
filepath = exporter.export_kol_revenue_data(
    start_year=2025,
    start_month=1,
    end_year=2025,
    end_month=7,
    kol_id_range=(100, 516)
)
```

### 4. 交互式使用

```bash
python export_example.py
```

运行后会出现菜单，可以选择不同的导出方式。

## 输出文件结构

导出的Excel文件包含3个工作表：

### 1. KOL营收数据（主表）
包含每个KOL的详细信息：
- KOL编号
- 博主名
- 各时长视频报价（1-20秒、21-60秒、60秒+）
- 每月视频商单数量
- 每月总营收
- 总计数据
- 创建/更新时间

### 2. 数据汇总
整体统计信息：
- KOL总数
- 处理成功/失败数
- 数据时间范围
- 总视频商单数量
- 总营收金额
- 平均每KOL营收
- 导出时间

### 3. 月度汇总
按月份的汇总数据：
- 每月总视频数量
- 每月总营收
- 每月平均每KOL数据

## 配置说明

### 时间配置
- `start_year`, `start_month`: 起始年月
- `end_year`, `end_month`: 结束年月（默认为当前年月）

### 筛选配置
- `kol_id_range`: KOL ID范围，格式为`(min_id, max_id)`，`None`表示全部

### 输出配置
- `output_dir`: 输出目录，默认为`data`

## 使用示例

### 示例1：导出最近数据
```python
# 导出最近3个月的数据
exporter = DouYinDataExporter()
filepath = exporter.export_recent_months(months_count=3)
```

### 示例2：导出年度数据
```python
# 导出2025年全年数据
exporter = DouYinDataExporter()
filepath = exporter.export_kol_revenue_data(
    start_year=2025,
    start_month=1,
    end_year=2025,
    end_month=12
)
```

### 示例3：导出特定KOL
```python
# 导出前100个KOL的数据
exporter = DouYinDataExporter()
filepath = exporter.export_kol_revenue_data(
    start_year=2025,
    start_month=1,
    kol_id_range=(1, 100)
)
```

## 数据计算说明

### 营收计算
- 月营收 = 当月视频数量 × 60秒+视频报价
- 总营收 = 所有月份营收之和

### 视频统计
- 统计指定时间范围内的所有视频
- 按发布日期进行月份归类

## 注意事项

1. **数据库连接**：确保数据库连接正常
2. **权限要求**：确保对输出目录有写权限
3. **内存使用**：大量数据可能占用较多内存
4. **文件覆盖**：同名文件会被覆盖，建议定期清理输出目录

## 错误处理

程序包含完善的错误处理机制：
- 数据库连接错误
- 文件写入权限错误
- 数据格式错误
- 网络超时错误

所有错误都会记录到日志中，方便排查问题。

## 依赖要求

```
pandas>=1.3.0
openpyxl>=3.0.0
loguru>=0.6.0
sqlalchemy>=1.4.0
```

## 更新日志

### v2.0.0 (当前版本)
- ✨ 支持灵活的时间范围配置
- 📊 自动生成月份列
- 📋 Excel多工作表导出
- 🎯 KOL ID范围筛选
- 📈 数据汇总和统计
- 🔧 移除飞书依赖

### v1.0.0 (原版本)
- 基础的数据导出功能
- 固定月份配置
- 飞书数据同步
 