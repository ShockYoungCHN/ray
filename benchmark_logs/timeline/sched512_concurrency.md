# Operator concurrency — 512_spill

| run | op | n | exec_sum_s | span_s | eff_parallel | conc_max | conc_p90 | conc_mean | total_cpus | note |
|---|---|---|---|---|---|---|---|---|---|---|
| 512_spill | CLUSTER | 2583 |  |  |  | 195 | 187 | 114 | 256 | saturation 195/256 |
| 512_spill | ReadFilesParquetV2 | 769 | 14671 | 109.5 | 134 | 157 | 143 | 134 | 256 |  |
| 512_spill | _shuffle_map_task | 502 | 4844 | 110.1 | 44 | 62 | 52 | 44 | 256 |  |
| 512_spill | _shuffle_reduce_task | 500 | 4136 | 112.7 | 37 | 171 | 129 | 37 | 256 |  |
| 512_spill | Write | 500 | 4295 | 113.2 | 38 | 72 | 61 | 38 | 256 |  |
