from cProfile import label
from turtle import color
import matplotlib.pyplot as plt
import pandas as pd
hub0_result = pd.read_csv("hubres\\hub0.csv")
hub1_result = pd.read_csv("hubres\\hub1.csv")
plt.figure(figsize=(8,6))
plt.plot(hub0_result["epoch"],hub0_result["mer"], marker='o',markersize=2, linewidth= 0.5, color='b',label='hub 0')
plt.plot(hub1_result["epoch"],hub1_result["mer"], marker='o',markersize=2, linewidth= 0.5, color='r',label='hub 1')
plt.legend()
plt.xlabel("epoch")
plt.ylabel("mer")
plt.savefig("hubres\\hub_mer.png", format="png")
plt.figure(figsize=(8,6))
plt.plot(hub0_result["epoch"],hub0_result["broker_num"], marker='o',markersize=2, linewidth= 0.5, color='b',label='hub 0')
plt.plot(hub1_result["epoch"],hub1_result["broker_num"], marker='o',markersize=2, linewidth= 0.5, color='r',label='hub 1')
plt.legend()
plt.xlabel("epoch")
plt.ylabel("broker num")
plt.savefig("hubres\\hub_num.png", format="png")
