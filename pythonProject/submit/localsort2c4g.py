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
                   '--files /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/conf/metrics.properties --class org.apache.spark.examples.my.DataFrameSortExperiment4 ' \
                   '--master '

    resource = ' --executor-cores 4 --total-executor-cores 24 --executor-memory 8g '

    name = '--name {} '
    conf = "--conf spark.app.name="
    end_command = ' /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/myjar/localsort.jar '

    cloud = "spark://node1:7077 " + resource
    local = 'local[2] ' +'--driver-memory 4g '

    # sort_conf = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # scale = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # scale = list(range(100000,100000001,100000))
    scale = list(range(1000,100001,1000))
    # print(scale)
    master = [1,2]


    for scal in scale:
        for sortConf in [0,1]:
            for cishu in [0,1,2]:
                sortConf1 = scal - sortConf
                cloud2 = cloud if sortConf1 < scal else local
                name2 = "cloud" if sortConf1 < scal else "local"
                # app_name = "\"" + name2 + "sort-" \
                #                           "" \
                #                           "" + "{:.0f}".format(scal/100000) + "w\" "
                app_name = "\"" + name2 + "sort-" + scal.__str__() + "\" "
                commit_line = base_command + cloud2 + name.format(app_name) \
                              + conf + app_name \
                              + end_command + scal.__str__() + ' ' + sortConf1.__str__()
                              # + ' ' + range_size.__str__()
                os.system(commit_line)
                # print(commit_line)

if __name__ == '__main__':
        dist_spark_submit()