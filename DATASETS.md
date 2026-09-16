# Datasets — one spine, three companions

## The verdict

**One spine dataset for all nine TPs. Not one per session.**

A new dataset costs 5–10 minutes of schema orientation at the top of a session. You
have ~8 hours of lecture in the entire semester. Nine datasets burns roughly a full
session on "what does this column mean" — and buys nothing, because none of your TPs
are about the domain. They are about partitioning, shuffle, storage layout and skew.

The stronger argument is your own redundancy-control table. The whole course is built
on callbacks:

> *"In TODO 2, your job took as long as your slowest partition. **Remember that number.**
> In session 7 you'll see it again with a Spark UI screenshot attached."*

That line only works if session 7 is the same data as session 2. Change the dataset and
"remember that number" becomes a figure of speech. The spiral repetition you designed on
purpose collapses into nine unrelated exercises.

**But one file is not enough.** Three TPs need properties a single raw file does not
have, and pretending otherwise produces sessions where the lesson does not land:

| TP | Needs | Raw taxi data gives you |
|---|---|---|
| S6 — "read one column out of 30" | ≥30 columns | 19 |
| S7 — broadcast join | a small dimension table | nothing to join to |
| S8 — quality gates | *known* defect counts to grade against | real but unmeasured dirt |

So: **one spine, three derived companions, all generated from the spine by
`scripts/prepare_tp_data.py`.** A student downloads ~60 MB and builds ~4.5 GB locally.
Nobody downloads gigabytes on a Tunisian home connection, and everyone's inputs are
byte-identical (the manifest fingerprint proves it).

---

## The spine

**NYC TLC Yellow Taxi Trip Records**, January 2024.

| Item | Link | Size |
|---|---|---|
| Landing page (data dictionary, all months) | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page | — |
| Seed file — `yellow_tripdata_2024-01.parquet` | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet | ~48 MB |
| Taxi Zone Lookup (**the broadcast-join table**) | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv | ~12 KB |
| Yellow data dictionary (give this to students) | https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf | — |

Swap the month in the filename for any month from 2009-01 to 2026-05. Data is published
with about a two-month lag.

**Why this one, concretely — not "it's the standard dataset":**

- **The skew is real and free.** ~90% of yellow pickups are Manhattan. Your S2 outline
  assumes a synthetic key where one value is 60%. You do not need to fabricate that;
  the real distribution is *worse*, which is a better lesson: nobody injected it, it is
  just what the world looks like.
- **It is natively Parquet**, so S6's "CSV vs JSON vs Parquet" comparison starts from the
  good format and degrades to the bad ones — you are measuring a real decision, not a
  contrived one.
- **It partitions on date** without you inventing a partition key.
- **The zone lookup is 265 rows.** That is a textbook broadcast side, under the 10 MB
  auto-broadcast threshold, so S7 can demonstrate both "Spark got it right automatically"
  and "here is what happens when you stop it".
- **It is genuinely dirty**: negative fares, zero-passenger trips, timestamps in 2002 and
  2098. You are not inventing data-quality problems for S8, you are cleaning real ones.

**One free gift:** TLC added a `cbd_congestion_fee` column from 2025 onward. That is a
real, documented, in-the-wild schema change. `prepare_tp_data.py` reproduces it in the
S8 data, so your "schema drift" slide is not hypothetical — it is a column that actually
appeared in this dataset while the course was being written.

---

## Companion 1 — the curated wide table (S6 and beyond)

Not a download. `prepare_tp_data.py` widens the raw 19 columns to **38** by joining the
zone lookup and deriving what any real pipeline derives: `pickup_borough`, `pickup_zone`,
`pickup_hour`, `is_weekend`, `trip_duration_min`, `avg_speed_mph`, `tip_pct`,
`fare_per_mile`, `is_airport_trip`, `payment_label`, and the date parts.

Every added column is a derivation a data engineer would actually write. None of it is
padding. And building it in the prep script means S6's column-pruning measurement is on a
table shaped like a real curated layer, which is where column pruning matters.

## Companion 2 — the zone dimension (S7)

265 rows, straight from TLC. Small side of the broadcast join, and the source of
`pickup_borough` — which is where the skew comes from.

## Companion 3 — the dirty table (S8)

Generated with a fixed seed, with **a defect manifest written next to it**
(`s8/defect_manifest.json`). Eight defect classes with exact counts:

| Defect | Rate | Teaches |
|---|---|---|
| Null `PULocationID` | 1.2% | null gate on a key column |
| Exact duplicate rows | 2.0% | dedup, and why append-mode is a trap |
| Negative fares | 0.4% | range gate |
| Absurd fares (99,999.99) | 0.1% | outlier gate |
| Zero passengers | 3.0% | domain validity vs null |
| Dates in 2002 / 2098 | 0.2% | date-range gate |
| Dropoff before pickup | 0.15% | cross-field validity |
| Malformed date strings (`31/02/2024`) | 0.1% | parse failure, not just a filter |
| `cbd_congestion_fee` null for 70% of rows | — | schema drift |

You cannot grade "did your quality gate catch it" without knowing what "it" is. This is
the file that makes S8 gradeable rather than vibes-based.

---

## Session map

| Session | Dataset | Path | Note |
|---|---|---|---|
| S1 | oversized CSV | `s1/trips_big.csv` (~2.2 GB) | generated locally, never downloaded |
| S2 | shared slice | `shared/trips_slice.csv` (2M rows) | skew key = `pickup_borough` |
| S3 | **same slice** | `shared/trips_slice.csv` | "the TODO 3 you failed in S1" |
| S4 | **same slice**, as Parquet | `shared/trips_slice.parquet` | "the same problem, twice" |
| S5 | — | — | catch-up; no new data |
| S6 | three formats + partitioned + small files | `s6/` | 38 columns, 10,000 tiny files pre-generated |
| S7 | skewed trips + zones | `s7/` | day-partitioned so the UI shows ~31 tasks |
| S8 | dirty table + day 2 | `s8/` | day 2 exists so re-running is testable |
| S9 | student's own S8 output | `s9/` | DuckDB over their Parquet |

Four of the nine TPs run on literally the same file. That is the point.

---

## The thing your architecture gets wrong

Your spec says the default project dataset is NYC TLC. If the TPs also run on NYC TLC
yellow, **you have handed every group nine sessions of working, debugged code against
their project dataset.** Grading weights 30% to "design justification (partitioning,
format, architecture)" — and the honest answer for a group using yellow taxi becomes
"we partitioned by date because that's what the S6 notebook did."

Fix, and it costs you nothing:

- **TPs: yellow taxi.** Small, fast, already built.
- **Projects: NOT yellow taxi.** Default to **High Volume FHV** (`fhvhv_tripdata_*`) —
  same family, so zero new schema-orientation cost, but ~20M rows/month, different
  columns, different join keys, and no notebook they can copy.
  `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet`
- **Make the floor explicit**, because "a size or file-count threshold you set" is a TODO
  in your own spec and groups will exploit the gap: **≥ 40M rows AND ≥ 3 GB raw AND ≥ 6
  monthly files.** Six months of FHVHV clears all three. Three months of yellow does not.
  That threshold is what actually stops you receiving pandas in a trench coat.

---

## Alternatives, if a group wants their own

All free, no cloud account, no login. Each clears the floor above.

| Dataset | Link | Why it works | Watch out |
|---|---|---|---|
| **NYC TLC High Volume FHV** | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page | ~20M rows/month, Parquet, date-partitionable | the default; steer indecisive groups here |
| **Citi Bike trips** | https://citibikenyc.com/system-data | monthly CSV, station dimension to join, real skew | CSV only — they must convert, which is a lesson |
| **Backblaze Hard Drive Stats** | https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data | daily drive failures; thematically perfect for S1's "failure is normal" | very wide (~150 cols), heavy nulls |
| **GH Archive** | https://www.gharchive.org/ | hourly JSON.gz, ~3M events/day, nested schema | JSON — expensive, which is the S6 lesson learned the hard way |
| **OpenSky flight data** | https://opensky-network.org/datasets/ | large, time-series, natural partitions | registration needed for some sets |
| **Wikipedia pageviews** | https://dumps.wikimedia.org/other/pageviews/ | hourly, enormous, brutal skew | no join dimension; weak for the join requirement |

Reject anything that does not clear the floor. A group arriving with a 200 MB Kaggle CSV
is a group that will not encounter a single thing this course taught.

---

## Hosting

Do not make 30 students hit nyc.gov at 08:55 on the day of S1.

1. Download `yellow_tripdata_2024-01.parquet` and `taxi_zone_lookup.csv` once.
2. Attach both to a **GitHub Release** on your course repo (2 GB per asset, no LFS quota,
   permanent URLs, no account needed to download).
3. Set `SEED_URL` and `ZONES_URL` in `docker/docker-compose.yml`.
4. If that download fails for any reason, the prep script falls back to a
   **schema-identical synthetic seed** — same 38 columns, same ~90% Manhattan skew, same
   defect profile. Every TP still runs. The class does not stop because a CDN did.
