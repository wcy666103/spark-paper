import matplotlib.pyplot as plt

# 从txt文件中读取数据
data = {
    'cloudsort-4-24-8': {'x': [], 'y': []},
    'cloudsort-1-6-2-': {'x': [], 'y': []},
    'cloudsort-2-12-4': {'x': [], 'y': []},
    'localsort-4-24-8': {'x': [], 'y': []}
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
# plt.plot(data['cloudsort-4-24-8']['x'], data['cloudsort-4-24-8']['y'], label='cloudsort-4-24-8')
plt.plot(data['cloudsort-1-6-2-']['x'], data['cloudsort-1-6-2-']['y'], label='cloudsort-1-6-2-')
# plt.plot(data['cloudsort-2-12-4']['x'], data['cloudsort-2-12-4']['y'], label='cloudsort-2-12-4')
plt.plot(data['localsort-4-24-8']['x'], data['localsort-4-24-8']['y'], label='localsort')

# 添加标题和标签
plt.title('Cloudsort vs Localsort')
plt.xlabel('data size')
plt.ylabel('time')
plt.legend()  # 添加图例

# 显示图表
plt.show()