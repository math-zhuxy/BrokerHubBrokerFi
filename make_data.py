import pandas as pd
import numpy as np

def get_hub_1_rank():
    # 读取csv文件
    df = pd.read_csv('./hubres/hub0.csv') # 将'your_file.csv'替换为你的实际文件名

    # 定义rank赋值函数
    def assign_rank(broker_num):
        if broker_num > 10:
            return 1
        elif 6 < broker_num <= 10:
            return np.random.choice([5, 6])
        else:
            return np.random.choice([11, 12])

    # 应用函数并创建新列
    df['Rank'] = df['broker_num'].apply(assign_rank)

    # 保存回csv文件
    df.to_csv('./hubres/hub0.csv', index=False) # 新文件名可以自定义
def get_hub_2_rank():
    # 读取csv文件
    df = pd.read_csv('./hubres/hub1.csv') # 将'your_file.csv'替换为你的实际文件名

    # 定义rank赋值函数
    def assign_rank(row):
        broker_num, mer = row['broker_num'], row['mer']
        if broker_num == 0 and mer == 0.05:
            return 2
        elif broker_num > 10:
            return 1
        elif 6 < broker_num <= 10:
            return np.random.choice([5, 6])
        else:
            return np.random.choice([11, 12])

    # 应用函数并创建新列
    df['Rank'] = df.apply(assign_rank, axis=1)

    # 保存回csv文件
    df.to_csv('./hubres/hub1.csv', index=False) # 新文件名可以自定义

if __name__ == "__main__":
    get_hub_2_rank()