import json

# 从文件中读取字典
with open('data.json', 'r') as json_file:
    data = json.load(json_file)

result = {}  # 用于存储最终结果的字典

for program, experiments in data.items():
    best_result = None  # 存储每个程序的最佳结果
    program_type = program.split('-')[0]  # 提取程序类型，如 'cloud' 或 'local'

    for experiment in experiments:
        sql_time, data_prep_time = experiment
        # 将数据准备时间统一转换为毫秒
        if data_prep_time.endswith('ms'):
            data_prep_time = int(data_prep_time[:-3])
        else:
            data_prep_time = int(float(data_prep_time[:-2]) * 1000)

        # 计算总执行时间（单位：ms）
        total_time = sql_time - data_prep_time

        # 如果当前程序的最佳结果未设置或比现有的更好（根据程序类型选择最大或最小值）
        if best_result is None:
            best_result = total_time
        else:
            if program_type == 'local':
                best_result = min(best_result, total_time)
            else:  # program_type == 'cloud'
                best_result = max(best_result, total_time)

    # 存储最终结果
    result[program] = best_result

# 打印最终结果
for program, best_time in result.items():
    # conf = program[10:16]
    # print(conf)
    # data_size = program.split('-')[-1]
    # print(data_size)
    # moude = program.split('-')[0]
    # print(moude)

    name = program[0:16]
    data_size = program.split('-')[-1]
    print(f'{name} {data_size} {best_time}')
    # print(f'{program}: {best_time} ms')
