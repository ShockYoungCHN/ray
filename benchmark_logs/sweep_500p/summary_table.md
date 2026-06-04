# OOC shuffle sweep — 500 partitions (64→512 GB)

| size_GB | spill_GB | elapsed_s | GB/s | read_s | read_s/GB | map_s | map_s/GB | reduce_s | write_s | reduce.ray_get_s | reduce.reduce_fn_s | map.ipc_encode_s | map.partition_fn_s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 64 | 0.0 | 39.3 | 1.63 | 9 | 0.137 | 15 | 0.23 | 17 | 14 | 103.9 | 0.82 | 284.9 | 264.5 |
| 128 | 0.0 | 82.3 | 1.56 | 49 | 0.386 | 49 | 0.382 | 20 | 25 | 85.6 | 0.99 | 549.9 | 484.4 |
| 256 | 14.0 | 145.3 | 1.76 | 80 | 0.313 | 82 | 0.32 | 47 | 53 | 220.8 | 1.91 | 1124.8 | 976.8 |
| 384 | 162.0 | 180.7 | 2.13 | 86 | 0.225 | 86 | 0.225 | 77 | 82 | 3369.9 | 2.86 | 1740.9 | 1448.7 |
| 512 | 294.6 | 242.3 | 2.11 | 124 | 0.242 | 112 | 0.219 | 108 | 113 | 1736.3 | 3.99 | 2355.6 | 1952.6 |

_Wall times overlap (streaming executor). sub-timers are cluster-wide cumulative seconds. spill from per-node events log._
