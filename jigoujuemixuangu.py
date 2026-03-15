import pandas as pd
import numpy as np
import akshare as ak
import requests
import json
from datetime import datetime, timedelta
import time
import warnings

warnings.filterwarnings('ignore')

# ============================================================
#  ★★★ 配置区 ★★★
# ============================================================
PUSHPLUS_TOKEN = "70a87015756f483ab09f70a5ebe5d6ff"  # 登录 https://www.pushplus.plus 获取


# ============================================================
#  通达信函数 Python 实现
# ============================================================

def TDX_SMA(series, n, m):
    """通达信 SMA(X,N,M) 递推加权移动平均"""
    result = np.zeros(len(series))
    vals = series.values
    result[0] = vals[0] if not np.isnan(vals[0]) else 0
    for i in range(1, len(vals)):
        x = vals[i] if not np.isnan(vals[i]) else 0
        result[i] = (x * m + result[i - 1] * (n - m)) / n
    return pd.Series(result, index=series.index)


def EMA(series, n):
    return series.ewm(span=n, adjust=False).mean()


def MA(series, n):
    return series.rolling(window=n, min_periods=n).mean()


def REF(series, n):
    return series.shift(n)


def LLV(series, n):
    return series.rolling(window=n, min_periods=1).min()


def HHV(series, n):
    return series.rolling(window=n, min_periods=1).max()


def COUNT(condition, n):
    return condition.astype(int).rolling(window=n, min_periods=1).sum()


def CROSS(s1, s2):
    return (s1 > s2) & (s1.shift(1) <= s2.shift(1))


def FILTER(signal, n):
    result = pd.Series(False, index=signal.index)
    last_signal_pos = -n - 1
    for i in range(len(signal)):
        if signal.iloc[i] and (i - last_signal_pos > n):
            result.iloc[i] = True
            last_signal_pos = i
    return result


# ============================================================
#  核心选股逻辑
# ============================================================

def calculate_signal(df):
    """对单只股票计算选股信号"""
    if len(df) < 35:
        return pd.Series(False, index=df.index), \
               pd.Series(False, index=df.index), \
               pd.Series(0.0, index=df.index)

    C = df['close'].astype(float)
    H = df['high'].astype(float)
    L = df['low'].astype(float)

    公式 = MA(C, 10)
    百分百准 = MA(C, 30)

    # VSDD系列
    low9 = LLV(L, 9)
    high9 = HHV(H, 9)
    diff = high9 - low9
    diff = diff.replace(0, np.nan)
    VSDD2 = ((C - low9) / diff * 100).fillna(0)
    VSDD3 = TDX_SMA(VSDD2, 3, 1)
    VSDD4 = TDX_SMA(VSDD3, 3, 1)
    VSDD5 = 3 * VSDD3 - 2 * VSDD4

    VSDD6 = C - REF(C, 1)
    VSDD6 = VSDD6.fillna(0)

    ema_ema_v6 = EMA(EMA(VSDD6, 6), 6)
    ema_ema_abs_v6 = EMA(EMA(VSDD6.abs(), 6), 6)
    ema_ema_abs_v6 = ema_ema_abs_v6.replace(0, np.nan)
    VSDD7 = (100 * ema_ema_v6 / ema_ema_abs_v6).fillna(0)

    cond1 = LLV(VSDD7, 2) == LLV(VSDD7, 7)
    cond2 = COUNT(VSDD7 < 0, 2) > 0
    cond3 = CROSS(VSDD7, MA(VSDD7, 2))
    VSDD8 = cond1 & cond2 & cond3

    # 均线多头
    FSDFSD1 = MA(C, 1)
    FSDFSD2 = MA(C, 3)
    FSDFSD3 = MA(C, 5)
    ma21 = MA(C, 21)

    DTPL = (
        (MA(C, 1) > MA(C, 3)) &
        (MA(C, 5) > ma21) &
        (C > 0) &
        (FSDFSD1 > FSDFSD2) &
        (FSDFSD2 > FSDFSD3)
    )

    VVV = FILTER(VSDD8, 5)

    return VVV, DTPL, VSDD7


# ============================================================
#  基础过滤
# ============================================================

def is_st(name):
    if name is None or pd.isna(name):
        return True
    return 'ST' in str(name).upper()


def basic_filter(spot_df):
    filtered = spot_df[
        (spot_df['最新价'] > 0) &
        (spot_df['涨跌幅'] > 0) &
        (spot_df['最新价'] < 50) &
        (~spot_df['名称'].apply(is_st)) &
        (~spot_df['代码'].str.startswith('8')) &
        (~spot_df['代码'].str.startswith('4'))
    ].copy()
    return filtered


# ============================================================
#  获取股票历史数据
# ============================================================

def get_stock_data(code, start_date, end_date):
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_date,
            adjust="qfq"
        )
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            '日期': 'date', '开盘': 'open', '最高': 'high',
            '最低': 'low', '收盘': 'close', '成交量': 'volume'
        })
        return df.sort_values('date').reset_index(drop=True)
    except Exception:
        return None


# ============================================================
#  ★ PushPlus 推送模块 ★
# ============================================================

def build_html_message(selected_list, scan_total, filtered_total, elapsed):
    """
    构建美观的 HTML 消息内容
    """
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date_str = datetime.now().strftime('%Y年%m月%d日')
    count = len(selected_list)

    # --- 顶部统计卡片 ---
    html = f'''
    <div style="font-family:'Microsoft YaHei',Arial,sans-serif;max-width:680px;margin:0 auto;">

      <!-- 标题 -->
      <div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
                  color:#fff;padding:20px 24px;border-radius:12px 12px 0 0;
                  text-align:center;">
        <h2 style="margin:0;font-size:22px;">📊 机构绝密选股 — 每日精选</h2>
        <p style="margin:8px 0 0;font-size:13px;opacity:0.85;">{date_str}</p>
      </div>

      <!-- 统计概览 -->
      <div style="display:flex;background:#f8f9fe;padding:16px 10px;
                  border-left:1px solid #e8e8e8;border-right:1px solid #e8e8e8;">
        <div style="flex:1;text-align:center;">
          <div style="font-size:24px;font-weight:bold;color:#667eea;">{count}</div>
          <div style="font-size:12px;color:#999;margin-top:2px;">入选股票</div>
        </div>
        <div style="flex:1;text-align:center;border-left:1px solid #e0e0e0;border-right:1px solid #e0e0e0;">
          <div style="font-size:24px;font-weight:bold;color:#333;">{scan_total}</div>
          <div style="font-size:12px;color:#999;margin-top:2px;">扫描总数</div>
        </div>
        <div style="flex:1;text-align:center;border-right:1px solid #e0e0e0;">
          <div style="font-size:24px;font-weight:bold;color:#333;">{filtered_total}</div>
          <div style="font-size:12px;color:#999;margin-top:2px;">初筛通过</div>
        </div>
        <div style="flex:1;text-align:center;">
          <div style="font-size:24px;font-weight:bold;color:#333;">{elapsed}s</div>
          <div style="font-size:12px;color:#999;margin-top:2px;">耗时</div>
        </div>
      </div>
    '''

    if count == 0:
        # 无结果
        html += '''
          <div style="padding:40px 20px;text-align:center;background:#fff;
                      border:1px solid #e8e8e8;border-top:none;border-radius:0 0 12px 12px;">
            <div style="font-size:48px;">😴</div>
            <p style="color:#999;font-size:15px;margin-top:10px;">今日暂无符合条件的股票</p>
            <p style="color:#ccc;font-size:12px;">请明日再来查看</p>
          </div>
        </div>
        '''
        return html

    # --- 股票卡片列表 ---
    html += '''
      <div style="background:#fff;border:1px solid #e8e8e8;border-top:none;padding:6px 14px 14px;">
    '''

    for i, stock in enumerate(selected_list, 1):
        code = stock['代码']
        name = stock['名称']
        price = stock['最新价']
        chg = stock['涨跌幅%']
        momentum = stock['动量值']
        ma_status = stock['均线多头']
        ma5 = stock.get('MA5', '--')
        ma10 = stock.get('MA10', '--')
        ma30 = stock.get('MA30', '--')

        # 涨跌颜色
        if chg > 0:
            chg_color = '#e74c3c'
            chg_bg = '#fef0f0'
            chg_str = f'+{chg}%'
            arrow = '▲'
        elif chg < 0:
            chg_color = '#27ae60'
            chg_bg = '#f0fef4'
            chg_str = f'{chg}%'
            arrow = '▼'
        else:
            chg_color = '#999'
            chg_bg = '#f5f5f5'
            chg_str = f'{chg}%'
            arrow = '—'

        # 均线多头标记
        if ma_status == '✔':
            ma_badge = '<span style="background:#27ae60;color:#fff;padding:2px 8px;border-radius:10px;font-size:11px;">多头排列 ✔</span>'
        else:
            ma_badge = '<span style="background:#e0e0e0;color:#666;padding:2px 8px;border-radius:10px;font-size:11px;">未多头 ✘</span>'

        # 动量颜色
        if momentum > 0:
            m_color = '#e74c3c'
        else:
            m_color = '#27ae60'

        html += f'''
        <div style="border:1px solid #eee;border-radius:10px;padding:14px 16px;
                    margin-top:10px;background:#fafbff;">

          <!-- 第一行：序号+名称+代码+涨跌 -->
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
              <span style="background:#667eea;color:#fff;padding:2px 8px;border-radius:8px;
                           font-size:12px;font-weight:bold;margin-right:8px;">No.{i}</span>
              <span style="font-size:17px;font-weight:bold;color:#333;">{name}</span>
              <span style="font-size:13px;color:#999;margin-left:6px;">{code}</span>
            </div>
            <div style="background:{chg_bg};color:{chg_color};padding:4px 12px;
                        border-radius:8px;font-weight:bold;font-size:15px;">
              {arrow} {chg_str}
            </div>
          </div>

          <!-- 第二行：价格和指标 -->
          <div style="display:flex;justify-content:space-between;margin-top:12px;
                      padding-top:10px;border-top:1px dashed #eee;">
            <div style="text-align:center;flex:1;">
              <div style="font-size:12px;color:#999;">最新价</div>
              <div style="font-size:18px;font-weight:bold;color:#333;margin-top:2px;">¥{price}</div>
            </div>
            <div style="text-align:center;flex:1;border-left:1px solid #f0f0f0;">
              <div style="font-size:12px;color:#999;">动量值</div>
              <div style="font-size:18px;font-weight:bold;color:{m_color};margin-top:2px;">{momentum}</div>
            </div>
            <div style="text-align:center;flex:1;border-left:1px solid #f0f0f0;">
              <div style="font-size:12px;color:#999;">均线状态</div>
              <div style="margin-top:4px;">{ma_badge}</div>
            </div>
          </div>

          <!-- 第三行：均线数据 -->
          <div style="display:flex;margin-top:10px;padding-top:8px;border-top:1px dashed #eee;">
            <div style="flex:1;text-align:center;">
              <span style="font-size:11px;color:#999;">MA5</span>
              <span style="font-size:12px;color:#555;margin-left:4px;">{ma5}</span>
            </div>
            <div style="flex:1;text-align:center;">
              <span style="font-size:11px;color:#999;">MA10</span>
              <span style="font-size:12px;color:#555;margin-left:4px;">{ma10}</span>
            </div>
            <div style="flex:1;text-align:center;">
              <span style="font-size:11px;color:#999;">MA30</span>
              <span style="font-size:12px;color:#555;margin-left:4px;">{ma30}</span>
            </div>
          </div>
        </div>
        '''

    # --- 底部 ---
    html += f'''
      </div>

      <!-- 底部声明 -->
      <div style="background:#f8f8f8;padding:14px 20px;border:1px solid #e8e8e8;
                  border-top:none;border-radius:0 0 12px 12px;text-align:center;">
        <p style="font-size:11px;color:#bbb;margin:0;">
          ⚠ 本结果仅供学习参考，不构成投资建议 | 扫描时间: {now_str}
        </p>
      </div>
    </div>
    '''

    return html


def send_pushplus(title, html_content, token=None):
    """
    通过 PushPlus 发送消息到微信
    """
    if token is None:
        token = PUSHPLUS_TOKEN

    if not token or token == "你的PushPlus的Token":
        print("\n  ⚠ 未配置 PushPlus Token，跳过推送")
        print("    请前往 https://www.pushplus.plus 注册获取 Token")
        return False

    url = "http://www.pushplus.plus/send"
    payload = {
        "token": token,
        "title": title,
        "content": html_content,
        "template": "html",
        "channel": "wechat"
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, data=json.dumps(payload),
                                 headers=headers, timeout=30)
        result = response.json()

        if result.get("code") == 200:
            print(f"\n  ✅ PushPlus 推送成功！请查看微信消息")
            return True
        else:
            print(f"\n  ❌ PushPlus 推送失败: {result.get('msg', '未知错误')}")
            return False
    except Exception as e:
        print(f"\n  ❌ PushPlus 推送异常: {e}")
        return False


# ============================================================
#  主选股程序
# ============================================================

def run_stock_screener():
    """运行选股主程序"""

    print("=" * 65)
    print("           📊 机构绝密选股 — Python 实现")
    print("=" * 65)
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 65)

    start_time = time.time()

    # ---- Step 1: 获取全市场行情 ----
    print("\n▶ [1/5] 获取A股实时行情...")
    spot_df = ak.stock_zh_a_spot_em()
    scan_total = len(spot_df)
    print(f"  全市场共 {scan_total} 只股票")

    # ---- Step 2: 基础过滤 ----
    print("\n▶ [2/5] 基础条件过滤...")
    filtered = basic_filter(spot_df)
    filtered_total = len(filtered)
    print(f"  过滤后剩余 {filtered_total} 只股票")

    stock_codes = filtered['代码'].tolist()
    name_map = dict(zip(filtered['代码'], filtered['名称']))
    price_map = dict(zip(filtered['代码'], filtered['最新价']))
    chg_map = dict(zip(filtered['代码'], filtered['涨跌幅']))

    # ---- Step 3: 逐只计算信号 ----
    print("\n▶ [3/5] 计算选股信号...")
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=150)).strftime('%Y%m%d')

    selected = []
    total = len(stock_codes)

    for idx, code in enumerate(stock_codes):
        if (idx + 1) % 100 == 0 or idx == 0 or (idx + 1) == total:
            pct = (idx + 1) / total * 100
            print(f"  进度: {idx + 1}/{total}  ({pct:.1f}%)")

        df = get_stock_data(code, start_date, end_date)
        if df is None or len(df) < 35:
            continue

        try:
            VVV, DTPL, VSDD7 = calculate_signal(df)

            if VVV.iloc[-1]:
                C = df['close'].astype(float)
                ma5_val = round(MA(C, 5).iloc[-1], 2) if len(C) >= 5 else '--'
                ma10_val = round(MA(C, 10).iloc[-1], 2) if len(C) >= 10 else '--'
                ma30_val = round(MA(C, 30).iloc[-1], 2) if len(C) >= 30 else '--'

                selected.append({
                    '代码': code,
                    '名称': name_map.get(code, ''),
                    '最新价': price_map.get(code, 0),
                    '涨跌幅%': round(chg_map.get(code, 0), 2),
                    '动量值': round(VSDD7.iloc[-1], 2),
                    '均线多头': '✔' if DTPL.iloc[-1] else '✘',
                    'MA5': ma5_val,
                    'MA10': ma10_val,
                    'MA30': ma30_val,
                })
        except Exception:
            continue

        time.sleep(0.05)

    # 按涨跌幅排序
    selected.sort(key=lambda x: x['涨跌幅%'], reverse=True)

    elapsed = round(time.time() - start_time)

    # ---- Step 4: 控制台输出 ----
    print("\n" + "=" * 65)
    print("▶ [4/5] 选股结果")
    print("=" * 65)

    if selected:
        result_df = pd.DataFrame(selected)
        result_df.index = range(1, len(result_df) + 1)
        result_df.index.name = '序号'
        display_cols = ['代码', '名称', '最新价', '涨跌幅%', '动量值', '均线多头']
        print(f"\n  共选出 {len(result_df)} 只股票:\n")
        print(result_df[display_cols].to_string())

        filename = f"机构绝密选股_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(filename, encoding='utf-8-sig')
        print(f"\n  💾 已保存: {filename}")
    else:
        print("\n  ⚠ 今日无符合条件的股票")

    # ---- Step 5: PushPlus 推送 ----
    print("\n" + "-" * 65)
    print("▶ [5/5] PushPlus 推送...")

    title = f"📊 机构绝密选股 {datetime.now().strftime('%m/%d')} | 精选 {len(selected)} 只"
    html_content = build_html_message(selected, scan_total, filtered_total, elapsed)
    send_pushplus(title, html_content)

    print("\n" + "=" * 65)
    print("  全部完成！")
    print("=" * 65)

    return selected


# ============================================================
if __name__ == '__main__':
    run_stock_screener()
