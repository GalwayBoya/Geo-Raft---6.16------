import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

# 读取CSV文件并计算平均延迟
def process_csv_data(csv_file):
    # 读取CSV数据
    df = pd.read_csv(csv_file)
    
    # 转换延迟时间从毫秒到秒
    df['ConsensusLatency_s'] = df['ConsensusLatency_ms'] / 1000
    
    # 按节点数量分组并计算平均延迟
    avg_latency = df.groupby('NodeCount')['ConsensusLatency_s'].mean().reset_index()
    
    # 获取每组的测试次数
    test_counts = df.groupby('NodeCount').size().reset_index(name='TestCount')
    
    # 合并平均延迟和测试次数
    result = pd.merge(avg_latency, test_counts, on='NodeCount')
    
    return avg_latency, result

# 在同一图上绘制两组数据
def plot_consensus_latency_comparison(csv_file1, csv_file2, labels=None):
    # 处理两组数据
    avg_latency1, result1 = process_csv_data(csv_file1)
    avg_latency2, result2 = process_csv_data(csv_file2)
    
    # 设置标签
    if labels is None:
        label1 = "Raft"
        label2 = "GRaft"
    else:
        label1, label2 = labels
    
    # 打印统计信息
    print(f"数据集1 ({label1}) 节点数量和平均共识延迟时间:")
    for _, row in result1.iterrows():
        print(f"节点数: {row['NodeCount']}, 平均延迟: {row['ConsensusLatency_s']:.6f}秒, 测试次数: {row['TestCount']}")
    
    print(f"\n数据集2 ({label2}) 节点数量和平均共识延迟时间:")
    for _, row in result2.iterrows():
        print(f"节点数: {row['NodeCount']}, 平均延迟: {row['ConsensusLatency_s']:.6f}秒, 测试次数: {row['TestCount']}")
    
    # 绘图
    plt.figure(figsize=(12, 7))
    
    # 绘制第一组数据
    plt.plot(avg_latency1['NodeCount'], avg_latency1['ConsensusLatency_s'], 'o-', 
             linewidth=2, color='blue', label=label1)
    
    # 添加第一组数据标签
    for i, row in avg_latency1.iterrows():
        plt.annotate(f"{row['ConsensusLatency_s']:.3f}s", 
                     (row['NodeCount'], row['ConsensusLatency_s']), 
                     textcoords="offset points", 
                     xytext=(0,10), 
                     ha='center')
    
    # 绘制第二组数据
    plt.plot(avg_latency2['NodeCount'], avg_latency2['ConsensusLatency_s'], 's--', 
             linewidth=2, color='red', label=label2)
    
    # 添加第二组数据标签
    for i, row in avg_latency2.iterrows():
        plt.annotate(f"{row['ConsensusLatency_s']:.3f}s", 
                     (row['NodeCount'], row['ConsensusLatency_s']), 
                     textcoords="offset points", 
                     xytext=(0,-15), 
                     ha='center')
    
    # 设置图表属性
    plt.xlabel('Node Count')
    plt.ylabel('Consensus Latency (s)')
    plt.title('Raft vs GRaft Consensus Latency')
    plt.grid(True)
    plt.legend()
    
    # 保存图片
    output_file = "../raft/data/consensus_latency/consensus_latency_comparison_plot.png"
    plt.savefig(output_file)
    print(f"比较图表已保存至: {output_file}")
    
    # 显示图表
    plt.show()

if __name__ == "__main__":
    # 默认使用预设文件
    csv_file1 = "../raft/data/consensus_latency/consensus_latency_results1 4-24 100.csv"
    csv_file2 = "../raft/data/consensus_latency/consensus_latency_results2 4-24 100.csv"
    
    # 处理命令行参数
    if len(sys.argv) >= 3:
        # 如果提供了两个文件
        csv_file1 = sys.argv[1]
        csv_file2 = sys.argv[2]
        labels = [sys.argv[3], sys.argv[4]] if len(sys.argv) > 4 else None
        plot_consensus_latency_comparison(csv_file1, csv_file2, labels)
    else:
        # 使用默认文件和标签
        plot_consensus_latency_comparison(csv_file1, csv_file2, ["Raft", "GRaft"])
