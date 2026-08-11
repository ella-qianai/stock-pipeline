# 股票市场 ETL 管道(AWS)

![tests](https://github.com/ella-qianai/stock-pipeline/actions/workflows/tests.yml/badge.svg)

[English](README.md) | 中文

一个全自动、按计划运行的 ETL 管道:每天调用第三方 REST API,把原始响应暂存进 S3,校验并转换后加载进 PostgreSQL 数据仓库——带重试逻辑、逐条记录的失败隔离,外加一个独立的数据质量检查。

5支股票,一个API供应商、三个接口。它处理的具体失败模式列在下面。

---

## 架构

```
EventBridge(每天美东17:00,cron定时触发)
        ↓
Lambda① — 抓取(lambda_function.py)
  向 Twelve Data REST API 发 GET 请求(JSON,API key认证)
  瞬时性失败用指数退避重试
  区分"被限流" vs. "认证失败" vs. "请求本身无效"三种情况
  把原始 JSON 原封不动存进 S3 Bronze 层
        ↓
S3 上传事件(自动触发)
        ↓
Lambda② — 转换加载(lambda_transform.py)
  从 S3 读取原始 JSON
  校验schema(必需字段)和数值合理性(价格/成交量范围)
  一条坏记录只跳过并记录日志,不拖垮整批
  幂等 upsert 进 RDS PostgreSQL(可以安全重跑)
        ↓
RDS PostgreSQL
  dim_stocks        — 公司参考数据(行业、市值、PE、股息率)
  fact_stock_prices — 每日股价记录
        ↓
data_quality_check.py — 每日加载完之后运行
  新鲜度:今天该到的symbol都到齐了吗?
  完整性:过去一周有没有symbol数据突然变稀疏?
  合理性:表里的数值本身看起来正常吗?

EventBridge(每周一次,cron定时触发)
        ↓
Lambda③ — 公司信息刷新(lambda_overview.py)
  对每个symbol各发两次GET请求,打到Twelve Data
  两个不同的接口 —— /profile(公司名、行业)和
  /statistics(市值、PE、股息率)—— 复用同一套认证和客户端
  两份原始响应都先存进S3,校验后upsert进dim_stocks
  之所以是周频而不是日频:行业/市值/PE这些数据不会
  每天变,没必要天天花API调用额度去刷新一周前也一样准确的数据

支撑服务:
  twelvedata_client.py — 共享的认证、重试策略,以及
                          "限流 vs. 认证失败 vs. 请求无效"
                          这套分类逻辑,Lambda①和③都在用
  Secrets Manager  — 存API key和数据库凭证,绝不写在代码或环境变量里
  CloudWatch       — 日志收集和失败告警
  GitHub Actions   — 每次push自动跑pytest
```

---

## 用到的AWS服务

| 服务 | 作用 |
|---------|------|
| **Lambda** | 抓取、转换、质量检查的无服务器计算 |
| **S3** | 原始数据存储(Bronze层) |
| **RDS PostgreSQL** | 用于分析查询的结构化存储 |
| **EventBridge** | 每日定时触发(cron) |
| **Secrets Manager** | 加密存储API key和数据库凭证 |
| **CloudWatch** | 日志收集和失败告警 |
| **GitHub Actions** | CI——每次push自动跑测试 |

---

## 可靠性与数据质量

一个持续依赖第三方数据拉取的管道,最容易在哪里出问题,以及这里是怎么处理的:

| 失败模式 | 处理方式 |
|---|---|
| 第三方API超时/连接断开 | 用`urllib3.Retry`对5xx和连接错误做指数退避重试(2s→4s→8s)——刻意不包括429(见下一行),只针对请求本身 |
| API对这个key限流了(HTTP 429) | 抛出`ApiRateLimitError`,**不**自动重试——立刻重试一个被限流的请求只会更快花完同一份有限的额度。在运行汇总里单独统计,读起来是"下次定时运行再试",而不是"哪里坏了" |
| key本身无效、过期,或者没覆盖到这个接口(HTTP 401/403) | 抛出`ApiAuthenticationError`——刻意和"单个symbol的问题"区分开,因为循环里剩下的每个symbol都会以完全相同的方式失败。`lambda_handler`遇到这个直接跳出循环,而不是把同一个错误重复记录五次 |
| API拒绝了请求本身:股票代码错误/已退市、参数格式不对(HTTP 400/404) | 抛出`ApiInvalidRequestError`——对这一个symbol是终止性的,但不影响整个运行;循环继续处理下一个symbol。这里也覆盖了一种更少见的情况:HTTP返回200,但body里仍然写着`"status": "error"`(Twelve Data文档里提到的行为,这里显式检查,而不是想当然认为错误只会跟着4xx状态码出现) |
| API悄悄改名或丢掉了某个字段 | `validate_price_data()` / `validate_overview_data()` 会按名字逐个检查每个必需字段,所以schema变化会在这里就报出明确缺了哪个字段,而不是几个函数之后才冒出一个裸的`KeyError` |
| API返回的值格式上合法但内容不合理 | 在schema检查之上再加数值合理性检查:价格>0、high≥low、成交量≥0、市值≥0 |
| 某个数值字段因为真实业务原因缺失,不是bug | `/statistics`对于从没分过红的股票,会直接不返回`dividends_and_splits`这个块——`_dig()`会沿着嵌套结构往下走,取不到就返回`None`而不是报错,所以"没有分红"会变成一个真正的`NULL`,而不是程序崩溃 |
| 五条记录里有一条是坏的 | 每条记录独立校验、独立提交(`process_record`)——一个symbol的文件有问题,记录日志并跳过,另外四个照常入库。最初的版本是把整批包在一个事务里,一条坏记录会导致整批回滚 |
| 管道在同一天内重跑(重试、回补) | `ON CONFLICT ... DO UPDATE` upsert——天生幂等,`fact_stock_prices`和`dim_stocks`两张表都是 |
| 长期悄悄劣化(某个symbol不知不觉就不再更新了) | `data_quality_check.py`在加载完之后运行,检查新鲜度(今天每个symbol是不是都有一行数据)、完整性(过去7天有没有symbol数据量不正常地稀疏)、合理性(表里本身有没有不合理的值) |

## 数据模型

```sql
CREATE TABLE dim_stocks (
    symbol          VARCHAR(10) PRIMARY KEY,
    company_name    VARCHAR(100),
    sector          VARCHAR(50),
    market_cap      BIGINT,
    pe_ratio        DECIMAL(10,2),
    dividend_yield  DECIMAL(10,4),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE fact_stock_prices (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(10) REFERENCES dim_stocks(symbol),
    price_date      DATE,
    open_price      DECIMAL(10,2),
    high_price      DECIMAL(10,2),
    low_price       DECIMAL(10,2),
    close_price     DECIMAL(10,2),
    volume          BIGINT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(symbol, price_date)
);
```

遵循**Medallion架构**:
- **Bronze层**:S3里的原始JSON,按日期分区(`bronze/stock_prices/YYYY-MM-DD/`),原样保留,这样转换逻辑有bug时可以直接修复重放,不用重新调API
- **Gold层**:RDS里清洗、校验过的结构化数据,用于SQL查询

---

## 测试

核心逻辑(校验规则、质量检查)都写成不依赖AWS的纯函数,所以不需要真实的AWS凭证或数据库连接就能做单元测试:

```bash
pip install -r requirements.txt
pytest -v
```

29个测试覆盖:Twelve Data的响应分类(限流/429、认证失败/401/403、请求无效/400/404,以及"200但body里说error"这种边缘情况,全部针对伪造的HTTP响应测试)、股价和公司信息两类记录的schema漂移检测(字段缺失/改名)、数值合理性边界情况(负价格、high<low、负市值、非数字值)、"分红字段缺失变成NULL"这个情况,以及三项数据质量检查——全部针对伪造的HTTP响应/DB游标,不需要真实API key、AWS凭证或网络访问。CI在每次push时通过GitHub Actions自动运行。

---

## 部署步骤(真要跑起来的话)

### 前置条件
- 配置好CLI的AWS账号
- Python 3.12+
- Twelve Data API key(免费Basic计划——800次/天,8次/分钟;在 [twelvedata.com/pricing](https://twelvedata.com/pricing) 注册,不需要信用卡)。在搭这个管道之前,已经用测试symbol验证过`/time_series`、`/profile`、`/statistics`三个接口在免费计划下都能访问——有些供应商会把基本面数据锁在付费层,但Twelve Data的免费计划覆盖了这个管道用到的全部三个接口

### 步骤

1. **RDS** — 创建一个PostgreSQL `db.t3.micro`实例(免费层),开启公网访问,记下endpoint

2. **Secrets Manager** — 存两个secret:
   - `stock-pipeline/db-credentials`:host、port、用户名、密码、数据库名
   - `stock-pipeline/twelvedata-api-key`:API key

3. **S3** — 建一个桶用来存原始数据

4. **Lambda①** — 部署`lambda_function.py` + `twelvedata_client.py` + `symbols.py`(Python 3.12,60秒超时)。设置环境变量`S3_BUCKET`。执行角色要有S3、Secrets Manager、CloudWatch Logs权限

5. **Lambda②** — 把`lambda_transform.py`和`psycopg2-binary`打包(Lambda上需要Linux构建版本),用同一个执行角色部署

6. **S3触发器** — 配置S3在`bronze/stock_prices/`前缀下有`ObjectCreated`事件时触发Lambda②

7. **EventBridge(每日)** — 建一条cron规则(`cron(0 21 * * ? *)`),每天美东17:00触发Lambda①

8. **Lambda③** — 把`lambda_overview.py` + `twelvedata_client.py` + `symbols.py`和`psycopg2-binary`一起部署。和Lambda②用同一个执行角色,再加S3写权限。单独配一条**每周**的EventBridge cron规则——公司参考数据不需要每天刷新

9. **Lambda④(可选)** — 部署`data_quality_check.py`,在Lambda①/②运行完不久后单独触发,配一个针对非200响应的CloudWatch告警

10. **CloudWatch** — 给所有Lambda的`Errors`指标建告警,失败时用SNS发邮件通知
