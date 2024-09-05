import os

# bin/spark-submit --files /opt/module/spark-3.0.0-standalone/conf/metrics.properties
# --class org.apache.spark.examples.my.DataFrameSortExperiment
# --master spark://node1:7077
# --executor-cores 4
# --total-executor-cores 24
# --executor-memory 8g
# myjar/localsort.jar
# 10000000 10000000 10000000
def dist_spark_submit():
    base_command = '/opt/module/spark-3.0.0-standalone/bin/spark-submit ' \
                   '--files /opt/module/spark-3.0.0-standalone/conf/metrics.properties --class org.apache.spark.examples.my.DataFrameSortExperiment3 ' \
                   '--master '

    resource = ' --executor-cores 4 --total-executor-cores 24 --executor-memory 8g '

    name = '--name {} '
    conf = "--conf spark.app.name="
    end_command = ' /opt/module/spark-3.0.0-standalone/myjar/myexample.jar '

    cloud = "spark://node1:7077 " + resource
    local = 'local[1] '
    masters = [1 ,2]
    print(masters)

    start_value = 2940000
    stop_value = 600000000
    step_size = 1000000

    sort_conf = list(range(start_value, stop_value + 1, step_size))

    # sort_conf = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # scale = [1000000,2000000,4000000,6000000,8000000,10000000,20000000,40000000,80000000,100000000]
    # range_size = 10000

    for sortConf in sort_conf:
        # for scal in scale:
            for master in masters:
                if master == 1:
                    base_command1 = base_command + cloud
                    app_name = "\"" + "cloudsort-" + sortConf.__str__() + "\" "
                else:
                    base_command1 = base_command + local
                    app_name = "\"" + "localsort-" + sortConf.__str__() + "\" "

                commit_line = base_command1  + name.format(app_name) \
                              + conf + app_name \
                              + end_command + sortConf.__str__()
                              # + ' ' + range_size.__str__()
                os.system(commit_line)
                # print(commit_line)


if __name__ == '__main__':
        dist_spark_submit()