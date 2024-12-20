import matplotlib.pyplot as plt

# 从txt文件中读取数据
data = {
    'cloudsort': {'x': [], 'y': []},
    'localsort': {'x': [], 'y': []}
}

with open('result-test.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        parts = line.split()
        if len(parts) == 3:
            algorithm = parts[0]
            x = int(parts[1])
            y = int(parts[2])
            data[algorithm]['x'].append(x)
            data[algorithm]['y'].append(y)

# 绘制折线图
plt.plot(data['cloudsort']['x'], data['cloudsort']['y'], label='cloudsort')
plt.plot(data['localsort']['x'], data['localsort']['y'], label='localsort')

# 添加标题和标签
plt.title('Cloudsort vs Localsort')
plt.xlabel('data size')
plt.ylabel('time')
plt.legend()  # 添加图例

# 显示图表
plt.show()