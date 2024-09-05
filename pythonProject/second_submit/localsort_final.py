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

    # local = 'local[2] ' +'--driver-memory 4g '
    local = 'local[1] '

    name = '--name {} '
    conf = "--conf spark.app.name="

    jar = ' /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/myjar/localsort.jar '

    # peizhi = [['4', '24', '8'], ['2', '12', '4'], ['1', '6', '2']]
    peizhi = [['2', '12', '4'], ['1', '6', '2']]
    # sort_conf = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # scale = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    scale = list(range(1500000, 30000000, 10000))

    for scal in scale:
        for cishu in [0, 1, 2]:

            app_name = "\""  + "localsort-" +  scal.__str__() + "\" "

            commit_line = base_command + local + name.format(app_name) \
                          + conf + app_name \
                          + jar + scal.__str__() + ' ' + scal.__str__()
            os.system(commit_line)
            # print(commit_line)

if __name__ == '__main__':
    dist_spark_submit()
