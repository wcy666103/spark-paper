# spark-paper
This repository supports the research described in the paper "Dynamic-awareness-based Spark Execution Plan Selection and Shuffle Optimization Strategies", 
submitted to the **IAENG International Journal of Computer Science** on November 11, 2024. It contains the source code, experimental data, and auxiliary scripts used in the study.


## Repository Structure
1. spark.tar

   + Modified Spark 3.0.0 source code with key optimizations:
     + Custom physical execution plans for the D2CS strategy, enabling dynamic selection between Driver-side and cluster-side execution.
     + Enhanced ShuffleRead pipeline implementing the PASA strategy, including parallel aggregation-sorting mechanisms and memory optimization logic.
     + Intermediate files from experimental evaluations (e.g., spill statistics, task execution logs).
2. hibench.tar
   + Customized HiBench benchmark suite for generating multi-scale datasets:
     + Modified to produce Parquet-formatted datasets (60 GB–200 GB) used in performance tests.
     + Added resource monitoring hooks to capture CPU/memory/disk I/O metrics during experiment runs.
3. pythonProject.tar
   + Scripts for automated experimental data collection:
     + Python-based HTTP request handlers to fetch real-time metrics from Prometheus-Grafana monitoring systems.
     + Data processing scripts for generating performance tables and visualization data.


## Data Availability
   Due to GitHub's single-file size limit, the full training datasets and detailed result sets are not hosted here. For reproducibility, please contact the corresponding author via email to request these materials as attachments.
   This work corresponds to the research article submitted to IAENG International Journal of Computer Science (submission date: November 11, 2024). For citation and usage, please reference the published version once available.