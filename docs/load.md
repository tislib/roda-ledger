# Load Test Report

Two benchmarks on a **CCX33 server at Hetzner** (8 dedicated vCPU), 1,000,000 accounts, deposit operations (random account, fixed amount), release build with WAL persistence enabled:

- **[Throughput](#throughput)** — sustained transactions per second, with submit-side latency.
- **[Latency under load](#latency-under-load)** — end-to-end latency (submit → durable & readable) across five load levels.

## Throughput

**Date:** 2026-04-10 · **Duration:** 50s · **Mode:** async (fire-and-forget)

```text

  +-----+--------+------------+------------+----------+----------+------------+--------------------------+
  |   # |   time |        TPS |        TPC |      P50 |      P99 |  in-flight |  seq>cmp/cmp>cmt/cmt>snp |
  +-----+--------+------------+------------+----------+----------+------------+--------------------------+
  |   1 |     1s |    5114633 |    5117098 |     60ns |    170ns |      32903 |             1.3k/31.6k/0 |
  |   2 |     2s |    5570782 |   10689030 |     60ns |     90ns |      20971 |         1.5k/19.5k/17.4k |
  |   3 |     3s |    5361797 |   16055906 |     60ns |     90ns |      24095 |          1.1k/23.0k/1.9k |
  |   4 |     4s |    5046406 |   21110017 |     60ns |     90ns |      19984 |          1.7k/18.3k/6.5k |
  |   5 |     5s |    4807209 |   25926201 |     60ns |     90ns |      13800 |          2.0k/11.8k/7.8k |
  |   6 |     6s |    4961211 |   30896311 |     60ns |     90ns |      13690 |          1.5k/12.2k/8.1k |
  |   7 |     7s |    5106755 |   36004030 |     60ns |     90ns |      25971 |          1.5k/24.5k/8.9k |
  |   8 |     8s |    5248100 |   41260781 |     60ns |     90ns |      19220 |          1.4k/17.8k/3.2k |
  |   9 |     9s |    5095105 |   46361028 |     60ns |     90ns |      18973 |             1.9k/17.0k/0 |
  |  10 |    10s |    5038156 |   51407620 |     60ns |    100ns |      22381 |          1.6k/20.8k/3.3k |
  |  11 |    11s |    5411236 |   56819530 |     60ns |    100ns |      20471 |          1.8k/18.7k/1.7k |
  |  12 |    12s |    5257536 |   62082878 |     60ns |     80ns |      17123 |          1.5k/15.6k/8.1k |
  |  13 |    13s |    5126148 |   67211352 |     60ns |     80ns |      18649 |          1.2k/17.4k/8.6k |
  |  14 |    14s |    5042382 |   72254680 |     60ns |     90ns |      25321 |          1.9k/23.4k/2.0k |
  |  15 |    15s |    5066671 |   77327588 |     60ns |     90ns |      22413 |          1.0k/21.4k/7.6k |
  |  16 |    16s |    5105334 |   82435206 |     60ns |     90ns |      14795 |         1.5k/13.3k/13.4k |
  |  17 |    17s |    5116705 |   87559576 |     60ns |     90ns |      20425 |         1.3k/19.1k/11.6k |
  |  18 |    18s |    4970700 |   92537944 |     60ns |    100ns |      22057 |             1.6k/20.5k/0 |
  |  19 |    19s |    5040220 |   97585388 |     60ns |     90ns |      14613 |          1.4k/13.2k/3.8k |
  |  20 |    20s |    5082557 |  102669968 |     60ns |     80ns |      10033 |           1.4k/8.6k/7.6k |
  |  21 |    21s |    5105639 |  107776428 |     60ns |     90ns |      13573 |          1.6k/11.9k/2.4k |
  |  22 |    22s |    5242667 |  113023394 |     50ns |     80ns |      26607 |          1.3k/25.3k/2.0k |
  |  23 |    23s |    5365046 |  118398309 |     50ns |     80ns |      11692 |         1.7k/10.0k/11.8k |
  |  24 |    24s |    5259334 |  123659516 |     50ns |     70ns |      20485 |         1.2k/19.3k/14.0k |
  |  25 |    25s |    4949362 |  128608908 |     50ns |     80ns |      11093 |           1.4k/9.7k/6.2k |
  |  26 |    26s |    5345618 |  133955141 |     60ns |     90ns |      34860 |             1.6k/33.3k/0 |
  |  27 |    27s |    5097986 |  139060742 |     60ns |     90ns |      19259 |             1.3k/18.0k/0 |
  |  28 |    28s |    5251062 |  144316754 |     60ns |     90ns |      23247 |         2.0k/21.3k/10.5k |
  |  29 |    29s |    5490695 |  149807595 |     60ns |     80ns |      22406 |          1.3k/21.1k/4.9k |
  |  30 |    30s |    5439768 |  155256740 |     60ns |     80ns |      23261 |         1.6k/21.7k/11.1k |
  |  31 |    31s |    5487901 |  160755145 |     60ns |     90ns |      34856 |             1.4k/33.4k/0 |
  |  32 |    32s |    5301035 |  166056721 |     50ns |     80ns |      13280 |          1.7k/11.6k/8.5k |
  |  33 |    33s |    5344608 |  171404376 |     50ns |     80ns |      25625 |         1.0k/24.6k/14.8k |
  |  34 |    34s |    5229637 |  176642420 |     50ns |     80ns |      27581 |         1.2k/26.3k/10.1k |
  |  35 |    35s |    5551168 |  182198360 |     50ns |    100ns |      21641 |         1.2k/20.5k/11.7k |
  |  36 |    36s |    5363468 |  187563104 |     50ns |     90ns |      16897 |          1.5k/15.4k/1.7k |
  |  37 |    37s |    5246498 |  192815692 |     50ns |     80ns |      14309 |          1.5k/12.8k/7.9k |
  |  38 |    38s |    5196030 |  198021093 |     50ns |     90ns |      18908 |          1.4k/17.5k/4.3k |
  |  39 |    39s |    5311393 |  203337827 |     60ns |    100ns |      32174 |          1.5k/30.7k/8.2k |
  |  40 |    40s |    5517843 |  208863309 |     60ns |    110ns |      16692 |          1.3k/15.4k/9.5k |
  |  41 |    41s |    5347233 |  214214341 |     60ns |    110ns |      15660 |         1.9k/13.7k/10.5k |
  |  42 |    42s |    5072908 |  219288707 |     50ns |     80ns |      21294 |         1.9k/19.4k/12.2k |
  |  43 |    43s |    5165765 |  224454498 |     50ns |     90ns |      15503 |          1.9k/13.6k/4.6k |
  |  44 |    44s |    5193680 |  229651005 |     60ns |     90ns |      18996 |             1.0k/17.9k/0 |
  |  45 |    45s |    5209979 |  234867113 |     60ns |    100ns |      12888 |          2.0k/10.9k/7.2k |
  |  46 |    46s |    4891265 |  239766041 |     60ns |    100ns |      23960 |         1.9k/22.1k/10.9k |
  |  47 |    47s |    5368438 |  245143056 |     60ns |     90ns |      26945 |         1.8k/25.2k/10.1k |
  |  48 |    48s |    5312403 |  250460248 |     50ns |     90ns |       9753 |           1.6k/8.2k/8.6k |
  |  49 |    49s |    5235608 |  255698494 |     60ns |    110ns |      21507 |          1.5k/20.0k/2.5k |
  |  50 |    50s |    5067572 |  260774860 |     60ns |     90ns |      15141 |             1.7k/13.5k/0 |
  |  51 |    51s |    5175854 |  265951867 |     50ns |    100ns |      18134 |          1.3k/16.9k/5.1k |
  |  52 |    52s |    5249061 |  271202147 |     60ns |     90ns |      37854 |          1.8k/36.1k/2.6k |
  |  53 |    53s |    5311198 |  276515386 |     60ns |     90ns |      24615 |         1.5k/23.1k/10.1k |
  |  54 |    54s |    5443167 |  281970753 |     50ns |     90ns |      19248 |          1.8k/17.4k/3.7k |
  |  55 |    55s |    5265014 |  287240328 |     60ns |     80ns |      19673 |            1.3k/18.4k/41 |
  |  56 |    56s |    5247141 |  292491352 |     60ns |     90ns |      18649 |          1.2k/17.4k/8.2k |
  |  57 |    57s |    5242670 |  297745956 |     60ns |    100ns |      14045 |             1.2k/12.9k/0 |
  |  58 |    58s |    5337059 |  303090776 |     50ns |     90ns |      19225 |          1.8k/17.4k/4.6k |
  |  59 |    59s |    5208835 |  308300888 |     50ns |     90ns |      19113 |         1.7k/17.4k/13.9k |
  +-----+--------+------------+------------+----------+----------+------------+--------------------------+

  ╔══════════════════════════════════════════════╗
  ║              LOAD TEST SUMMARY               ║
  ╠══════════════════════════════════════════════╣
  ║  Duration      :      60.00s                 ║
  ║  Submitted     :  313220001                  ║
  ║  Avg TPS       :    5220227                  ║
  ║  Mode          :      async                  ║
  ╠══════════════════════════════════════════════╣
  ║  P50  Latency  :       60ns                  ║
  ║  P99  Latency  :       90ns                  ║
  ║  P999 Latency  :      210ns                  ║
  ║  Min  Latency  :       30ns                  ║
  ║  Max  Latency  :     11.0µs                  ║
  ║  Samples       :      31323                  ║
  ╚══════════════════════════════════════════════╝

```

## Latency Under Load

**Date:** 2026-06-07 · **Samples:** 1000 probes per scenario

Each probe submits one deposit, records its `tx_id`, then spins on the snapshot index until that id is reached — the elapsed time is the **end-to-end latency** (submit → committed to the WAL → visible in the snapshot). A background thread sustains the target throughput while the probe runs; this repeats 1000 times per scenario to build the percentiles.

```text
  +---------------+--------+--------------+----------+----------+----------+----------+
  | Scenario      | Target | Achieved TPS |   P50    |   P99    |   P999   |   Max    |
  +---------------+--------+--------------+----------+----------+----------+----------+                                   
  | No Load       |      - |         1.7k |  547.3µs |    1.2ms |    1.4ms |    1.6ms |                                                
  | Sustain 10k   |  10.0k |        10.9k |    1.1ms |    1.5ms |    2.5ms |    3.0ms |                                                     
  | Sustain 100k  | 100.0k |       100.7k |    1.3ms |    1.7ms |    2.1ms |    2.1ms |                                                 
  | Sustain 1M    |  1.00M |        1.00M |    2.2ms |    2.9ms |    3.3ms |    5.0ms |                                                    
  | Full (Spike)  |    max |        4.41M |    4.2ms |    5.8ms |    8.0ms |   10.6ms |
  +---------------+--------+--------------+----------+----------+----------+----------+
```

Latencies are millisecond-scale because every probe waits for **durability** — its transaction must be committed to the WAL and applied to the snapshot before the timer stops. (Submit-only latency, fire-and-forget, is ~60ns — see [Throughput](#throughput) above.) The achieved-TPS column confirms the background load held its target; under *Full* the pipeline saturates at 4.41M tx/s and per-request latency climbs as transactions queue behind the backlog.

Run it:

```bash
cargo run --package ledger --release --bin load_latency
```

Flags: `--account-count` (default 1,000,000), `--samples` (default 1000), `--warmup-ms` (default 500).

### Run Link
[Run Link](https://github.com/tislib/roda-ledger/actions/runs/27103521421/job/79988424880)