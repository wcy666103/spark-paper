import requests

import json

applications = requests.get('http://node1:18080/api/v1/applications?status=completed')

list = json.loads(applications.text)
total_time = {}

# list = ["local-1705681816828"]
# # ,"app-20240120002234-0141"

for application in list:
    request_map = json.loads(requests.get('http://node1:18080/api/v1/applications/' + application['id'] + '/sql').text)
    # request_map = json.loads(requests.get('http://node1:18080/api/v1/applications/' + application + '/sql').text)
    # print(request_map)
    name = application['name']

    if int(name.split('-')[-1]) > 2500000:
        continue
    # 后来一次实验设置的名字不一样
    if name.startswith('local') and not name.startswith('localsort-4-24-8'):
        print(name)
        name = 'localsort-4-24-8-' + name.split('-')[-1]
        print(name)
    print(name)
    if name not in total_time:
        total_time[name] = []
    duration = request_map[0]['duration']

    # 提取节点信息
    nodes_info = request_map[0]['nodes']
    # 打印节点信息
    if name.startswith("local"):
        WholeStageCodegen_time = nodes_info[2]['metrics'][0]['value']
    else:
        # print(nodes_info[2]['metrics'][0]['value'])
        # 使用正则表达式提取 'max' 值
        import re
        # 使用正则表达式提取第三个ms值
        pattern = r'\((\d+ ms, \d+ ms, (\d+ ms))'
        match = re.search(pattern,  nodes_info[2]['metrics'][0]['value'])
        WholeStageCodegen_time = match.group(2)

    total_time[name].append((duration,WholeStageCodegen_time))

# 打开文件以写入
# 将字典写入文件
with open('data.json', 'w') as json_file:
    json.dump(total_time, json_file)
# print(total_time)

# 总的完成时间：'duration'
#