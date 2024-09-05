import os

def dist_spark_submit():
    base_command = '/home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/bin/spark-submit ' \
                   '--files /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/conf/metrics.properties --class org.apache.spark.examples.my.DataFrameSortExperiment4 ' \
                   '--master '

    resource = ' --executor-cores {} --total-executor-cores {} --executor-memory {} '

    name = '--name {} '
    conf = "--conf spark.app.name="
    end_command = ' /home/wcy_spark/spark-3.5.0-SNAPSHOT-bin-wcy2/myjar/localsort.jar '


    local = 'local[2] ' +'--driver-memory 4g '

    scale = list(range(1000, 100001, 1000))
    execoutors=[(1,2),()]
    executor_memory = ['4g', '8g']  # 可以根据需要调整Executor内存

    for scal in scale:
        for sortConf in [0, 1]:
            for cishu in [0, 1, 2]:
                cloud = "spark://node1:7077 " + resource
                sortConf1 = scal - sortConf

                cloud2 = cloud if sortConf1 < scal else local
                name2 = "cloud" if sortConf1 < scal else "local"
                app_name = "\"" + name2 + "sort-" + scal.__str__() + "\" "

                for exec_cores in executor_cores:
                    for total_exec_cores in total_executor_cores:
                        for exec_mem in executor_memory:
                            app_conf = app_name + resource.format(exec_cores, total_exec_cores, exec_mem)
                            commit_line = base_command + cloud2 + name.format(app_name) \
                                          + conf + app_name \
                                          + end_command + scal.__str__() + ' ' + sortConf1.__str__()
                            # os.system(commit_line)
                            print(commit_line)

if __name__ == '__main__':
        dist_spark_submit()