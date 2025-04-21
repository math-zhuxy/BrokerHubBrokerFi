# 全局画图风格设置
FIGSIZE = (8, 6)                    # 统一图片大小
LEGEND_POSITION = 'upper right'     # 图例位置
STEP = 50                           # X轴刻度间隔
RATIO_STEP = 0.2                    # Y轴刻度间隔
AXIS_FONTSIZE = 25                  # 轴标签字号
TICK_FONTSIZE = 25                  # 刻度字号
SCIENTIFIC_FONTSIZE = 20            # 科学计数法字号
SUPTITLE_FONTSIZE = 24              # 总标题字号
SUBTITLE_FONTSIZE = 24              # 子标题字号
LEGEND_SIZE = 20                    # 图例字号
LINE_WIDTH = 2                    # 线宽度
SUBLINE_WIDTH = 1                   # 次要线宽度
LINE_STYLE1 = '--'
LINE_STYLE2 = '-'
MARKER = 'o'                        # 标记样式
MARKER_SIZE = 3                     # 标记大小

# 颜色循环
COLOR_CYCLE = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
]

'''13. 绘制 BrokerHub 综合指标变化'''
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

def plot_combined_metrics_with_mer_participation():
    # 创建2行4列的子图布局
    fig, axes = plt.subplots(2, 4, figsize=(32, 12))  # 调整总宽度以适应4列
    
    # 设置颜色
    COLOR_CYCLE1 = [
        '#1f77b4',    # MER
        '#d62728',    # Total Revenue Ratio
        '#00CED1',    # Net Revenue Ratio
        '#2ca02c',    # Rank
        '#ff7f0e',    # Participation Rate 
        '#BA55D3',    # User Revenue Ratio
        '#bcbd22'     # User Funds
    ]
    color_mer = COLOR_CYCLE1[0]
    color_total_ratio = COLOR_CYCLE1[1]
    color_net_ratio = COLOR_CYCLE1[2]
    color_rank = COLOR_CYCLE1[3]
    color_participation = COLOR_CYCLE1[4]
    color_user_ratio = COLOR_CYCLE1[5]
    color_user_funds = COLOR_CYCLE1[6]
    # 初始化空列表，用于存储各列数据
    epochs = np.linspace(1, 300, 300)
    epoch_limit = 300

    hub1_total_ratio = []
    hub1_mer = []
    ranks_hub1 = []
    hub1_part = []
    hub1_user_funds = []
    hub1_net_ratio = []
    hub1_user_ratio = []


    # 遍历DataFrame的每一行，并将数据追加到相应的列表中
    df_hub1 = pd.read_csv("./hubres/hub0.csv")
    for _, row in df_hub1.iterrows():
        hub1_part.append(float(row['broker_num'])/float(20))
        hub1_mer.append(float(row['mer']))
        ranks_hub1.append(int(row['Rank'])) 
        hub1_total_ratio.append(1000 * float(row['revenue']) / float(row['fund']))
        hub1_user_funds.append(int(row['fund']) - 3000000)
        hub1_net_ratio.append(1000 * float(row['revenue']) / float(row['fund']) * float(row['mer']))
        hub1_user_ratio.append(1000 * float(row['revenue']) / float(row['fund']) * (1 - float(row['mer'])))

    hub2_mer = []
    ranks_hub2 = []
    hub2_part = []
    hub2_total_ratio = []
    hub2_user_funds = []
    hub2_net_ratio = []
    hub2_user_ratio = []

    df_hub2 = pd.read_csv("./hubres/hub1.csv")
    for _, row in df_hub2.iterrows():
        hub2_part.append(float(row['broker_num'])/float(20))
        hub2_mer.append(float(row['mer']))
        ranks_hub2.append(int(row['Rank'])) 
        hub2_total_ratio.append(1000 * float(row['revenue']) / float(row['fund']))
        hub2_user_funds.append(int(row['fund']) - 3000000)
        hub2_net_ratio.append(1000 * float(row['revenue']) / float(row['fund']) * float(row['mer']))
        hub2_user_ratio.append(1000 * float(row['revenue']) / float(row['fund']) * (1 - float(row['mer'])))

    # 图a (hub1: MER, 排名)
    ax = axes[0, 0]
    line1, = ax.plot(epochs[:epoch_limit], hub1_mer, label='BrokerHub1 MER', color=color_mer, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.set_ylim(-0.05, 1.05)
    ax.set_yticks(np.arange(0, 1.05, 0.25))
    
    
    ax2 = ax.twinx()
    line2, = ax2.plot(epochs[:epoch_limit], ranks_hub1, label='BrokerHub1 Rank', color=color_rank, linestyle=LINE_STYLE1, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax2.set_ylabel('Rank', fontsize=AXIS_FONTSIZE)
    ax2.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax2.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax2.set_ylim(-5,  105)
    ax2.set_yticks(range(0, 101, 20))
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, handletextpad=0.3, bbox_to_anchor=(1.01, 1))
    
    # 图e (hub2: MER, 排名)
    ax = axes[1, 0]
    line1, = ax.plot(epochs[:epoch_limit], hub2_mer, label='BrokerHub2 MER', color=color_mer, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.set_ylim(-0.05, 1.05)
    ax.set_yticks(np.arange(0, 1.05, 0.25))
    
    ax2 = ax.twinx()
    line2, = ax2.plot(epochs[:epoch_limit], ranks_hub2, label='BrokerHub2 Rank', color=color_rank, linestyle=LINE_STYLE1, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax2.set_ylabel('Rank', fontsize=AXIS_FONTSIZE)
    ax2.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax2.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax2.set_ylim(-5, 105)
    ax2.set_yticks(range(0, 101, 20))
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE,handletextpad=0.3, bbox_to_anchor=(1.01, 1))
    
    # 图b (hub1: MER, 用户参与率)
    ax = axes[0, 1]
    line1, = ax.plot(epochs[:epoch_limit], hub1_mer, label='BrokerHub1 MER', color=color_mer, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    line2, = ax.plot(epochs[:epoch_limit], hub1_part, label='Investor \nParticipation Rate', linestyle=LINE_STYLE1, color=color_participation, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.set_xlabel('Epoch', fontsize=AXIS_FONTSIZE)
    ax.set_xticks(np.arange(0, epoch_limit + 1, STEP))
    ax.set_xticklabels(np.arange(0, epoch_limit + 1, STEP), fontsize=TICK_FONTSIZE)
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, handlelength=1.5, handletextpad=0.3, bbox_to_anchor=(1.01, 1))
    
    # 图f (hub2: MER, 用户参与率)
    ax = axes[1, 1]
    line1, = ax.plot(epochs[:epoch_limit], hub2_mer, label='BrokerHub2 MER', color=color_mer, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    line2, = ax.plot(epochs[:epoch_limit], hub2_part, label='Investor \nParticipation Rate', linestyle=LINE_STYLE1, color=color_participation, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.set_xlabel('Epoch', fontsize=AXIS_FONTSIZE)
    ax.set_xticks(np.arange(0, epoch_limit + 1, STEP))
    ax.set_xticklabels(np.arange(0, epoch_limit + 1, STEP), fontsize=TICK_FONTSIZE)
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, handlelength=1.5, handletextpad=0.3, bbox_to_anchor=(1.01, 0.9))
    
    # 图c (hub1: 总收益率, 用户投资资金)
    ax = axes[0, 2]
    line1, = ax.plot(epochs[:epoch_limit], hub1_total_ratio, label='BrokerHub1 \nRevenue Ratio', color=color_total_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    
    ax2 = ax.twinx()
    line2, = ax2.plot(epochs[:epoch_limit], hub1_user_funds, label='Investor Funds', color=color_user_funds, linestyle=LINE_STYLE1, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax2.set_ylabel('Funds', fontsize=AXIS_FONTSIZE)
    ax2.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax2.yaxis.get_offset_text().set_fontsize(TICK_FONTSIZE)
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, handlelength=1.5)
    
    # 图g (hub2: 总收益率, 用户投资资金)
    ax = axes[1, 2]
    line1, = ax.plot(epochs[:epoch_limit], hub2_total_ratio, label='BrokerHub2 \nRevenue Ratio', color=color_total_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    
    ax2 = ax.twinx()
    line2, = ax2.plot(epochs[:epoch_limit], hub2_user_funds, label='Investor Funds', color=color_user_funds, linestyle=LINE_STYLE1, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax2.set_ylabel('Funds', fontsize=AXIS_FONTSIZE)
    ax2.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax2.yaxis.get_offset_text().set_fontsize(TICK_FONTSIZE)
    
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax.legend(lines, labels, loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, handlelength=1.5, bbox_to_anchor=(0.96, 0.97))
    
    # 图d (hub1: 净收益率, 用户实际收益率)
    ax = axes[0, 3]
    ax.plot(epochs[:epoch_limit], hub1_net_ratio, label='BrokerHub1 \nNet Revenue Ratio', color=color_net_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.plot(epochs[:epoch_limit], hub1_user_ratio, label='Investor \nRevenue Ratio', color=color_user_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.legend(loc=LEGEND_POSITION, fontsize=LEGEND_SIZE)
    
    # 图h (hub2: 净收益率, 用户实际收益率)
    ax = axes[1, 3]
    ax.plot(epochs[:epoch_limit], hub2_net_ratio, label='BrokerHub2 \nNet Revenue Ratio', color=color_net_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.plot(epochs[:epoch_limit], hub2_user_ratio, label='Investor \nRevenue Ratio', color=color_user_ratio, linewidth=LINE_WIDTH, marker=MARKER, markersize=MARKER_SIZE)
    ax.set_ylabel('Ratio (%)', fontsize=AXIS_FONTSIZE)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.tick_params(axis='y', labelsize=TICK_FONTSIZE)
    ax.legend(loc=LEGEND_POSITION, fontsize=LEGEND_SIZE, bbox_to_anchor=(0.945, 1))
       
    # 设置所有子图的x轴标签
    for ax in axes.flat:
        ax.set_xlabel('Epoch', fontsize=AXIS_FONTSIZE)
        ax.set_xticks(np.arange(0, epoch_limit + 1, STEP))
        ax.set_xticklabels(np.arange(0, epoch_limit + 1, STEP), fontsize=TICK_FONTSIZE)
    
    # 设置子图标题
    subtitles = [
        '(a) BrokerHub1 MER & Rank Performance',
        '(b) BrokerHub1 MER & Participation',
        '(c) BrokerHub1 Revenue Ratio & Investor Funds',
        '(d) BrokerHub1 Net Revenue & Investor Revenue',
        '(e) BrokerHub2 MER & Rank Performance',
        '(f) BrokerHub2 MER & Participation',
        '(g) BrokerHub2 Revenue Ratio & Investor Funds',
        '(h) BrokerHub2 Net Revenue & Investor Revenue',
    ]
    positions = [
        (0.127, 0.465),  # a
        (0.39, 0.465),  # b
        (0.64, 0.465),  # c
        (0.91, 0.465),  # d
        (0.127, -0.01),  # e
        (0.39, -0.01),  # f
        (0.64, -0.01),  # g
        (0.91, -0.01)   # h
    ]
    for subtitle, pos in zip(subtitles, positions):
        fig.text(pos[0], pos[1], subtitle, ha='center', fontsize=SUBTITLE_FONTSIZE)
    
    # 调整布局并保存
    plt.tight_layout(rect=[0, 0, 1, 0.92], h_pad=4.5, w_pad=3)
    plt.savefig(os.path.join('./hubres', 'zxy_brokerhub_combined_metrics.pdf'), dpi=300, bbox_inches='tight')
    print("plot done.")
    plt.close()

if __name__ == "__main__":
    plot_combined_metrics_with_mer_participation()