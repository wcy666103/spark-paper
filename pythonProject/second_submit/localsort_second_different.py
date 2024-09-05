import os


# bin/spark-submit --files /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/conf/metrics.properties
# --class org.apache.spark.examples.my.DataFrameSortExperiment
# --master spark://node1:7077
# --executor-cores 4
# --total-executor-cores 24
# --executor-memory 8g
# myjar/localsort.jar
# 10000000 10000000 10000000
def dist_spark_submit():
    base_command = '/home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/bin/spark-submit ' \
                   '--files /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/conf/metrics.properties ' \
                   '--class org.apache.spark.examples.my.DataFrameSortExperiment4 ' \
                   '--master '

    resource = ' --executor-cores {} --total-executor-cores {} --executor-memory {}g '

    cloud = "spark://node1:7077 " + resource
    # local = 'local[2] ' +'--driver-memory 4g '
    local = 'local[1] '

    name = '--name {} '
    conf = "--conf spark.app.name="

    jar = ' /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/myjar/localsort.jar '

    # peizhi = [['4', '24', '8'], ['2', '12', '4'], ['1', '6', '2']]  第一次运行到
    peizhi = [['2', '12', '4'], ['1', '6', '2']]
    # sort_conf = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # scale = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    scale = list(range(3000000, 4830001, 10000))

    for p in peizhi :
        for scal in scale:
            for sortConf in [0, 1]:
                for cishu in [0, 1, 2]:
                    sortConf1 = scal - sortConf

                    cloud2 = cloud if sortConf1 < scal else local
                    name2 = "cloud" if sortConf1 < scal else "local"

                    if (int(p[0]) < 4) and (name2.startswith("local")):
                        continue

                    app_name = "\"" + name2 + "sort-"+ p[0] + '-' +p[1] + '-' + p[2] + '-'+ scal.__str__() + "\" "

                    commit_line = base_command + cloud2 + name.format(app_name) \
                                  + conf + app_name \
                                  + jar + scal.__str__() + ' ' + sortConf1.__str__()
                    if name2.startswith("cloud"):
                        commit_line = commit_line.format(p[0],p[1],p[2])
                    os.system(commit_line)
                    # print(commit_line)

if __name__ == '__main__':
    dist_spark_submit()
