# Operator concurrency — 64_nospill

| run | op | n | exec_sum_s | span_s | eff_parallel | conc_max | conc_p90 | conc_mean | total_cpus | note |
|---|---|---|---|---|---|---|---|---|---|---|
| 64_nospill | CLUSTER | 1238 |  |  |  | 203 | 144 | 67 | 256 | saturation 203/256 |
| 64_nospill | ReadFilesParquetV2 | 100 | 488 | 8.7 | 56 | 100 | 100 | 56 | 256 |  |
| 64_nospill | _shuffle_map_task | 78 | 619 | 13.3 | 47 | 78 | 74 | 46 | 256 |  |
| 64_nospill | _shuffle_reduce_task | 492 | 573 | 13.7 | 42 | 79 | 71 | 42 | 256 |  |
| 64_nospill | Write | 500 | 941 | 11.7 | 80 | 157 | 126 | 80 | 256 |  |
