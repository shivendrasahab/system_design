# System Design Interview Cheat Sheet

Condensed from `system-design-notes_ap_bg.md`. One card per topic: what to say, the numbers, the trap, the trade-off. Read the full notes for the why; use this the morning of.

---

## 1. The 45-minute script

| Minutes | Do | Say out loud |
|---|---|---|
| 0-5 | Functional requirements (3 to 5, name what is out of scope). Non-functional: availability, p99 latency, consistency (where strong, where eventual), durability, read/write ratio, scale. | "I'll assume 100:1 read/write; if it's 1:1 the cache goes and we shard writes." |
| 5-10 | Back-of-envelope: QPS, storage, bandwidth, per-node capacity. Let the numbers pick the design. | "2k QPS on 400 GB fits one primary plus replicas. 200k QPS on 100 TB means sharding from day one." |
| 10-20 | High-level boxes. Only components you know you need. For each: DB and caching, scaling and fault tolerance, async, communication, consistency/correctness, observability. | "I'm drawing the standard parts fast so we can spend time on the hard part." |
| 20-40 | The hard part. Alternatives with explicit trade-offs. Failure modes: slow, down, wrong data. Concurrency: two requests race, is data still right? | "Option A gives X, costs Y. I pick A because Z. I give up Y." |
| 40-45 | Rollout, migration, metrics and alarms, cost, ownership. What changes at 10x. | "At 10x the single primary runs out of writes, so we shard by user_id." |

**SLI / SLO / error budget:** SLI is what you measure (fraction of requests under 200 ms). SLO is the target (99.9% over 30 days). Error budget is the allowed miss (0.1%). Budget left: ship. Budget spent: fix reliability.

**Availability math:** 99.9% = ~43 min/month down. 99.99% = ~4 min/month. Each extra nine costs a lot more.

**What a Staff/Principal round checks:** you drive the ambiguity, you pin NFRs, you estimate with reasons, you find the hard part, you present alternatives with trade-offs, you reason about failure and concurrency, you think past day one (rollout, operations, cost, evolution).

---

## 2. Numbers to carry in your head

| Thing | Rough number |
|---|---|
| Seconds in a day | ~100k (86,400) |
| One relational primary | few thousand writes/s, few TB comfortably |
| One web server | few thousand simple req/s |
| One Redis node | ~100k ops/s |
| Redis / KV read | ~1 ms (sub-ms in-DC) |
| In-process cache read | sub-µs |
| S3 first byte | tens of ms per request; throughput huge if parallel |
| Local SSD read | ~100 µs |
| Cross-region round trip | 100+ ms |
| TCP setup | 1 RTT (2 with TLS) |
| Disk sequential read | 200 MB/s to 2 GB/s |
| Bloom filter | ~10 bits/key for 1% false positives, k ≈ 7 |
| Redis Cluster | 16,384 hash slots |
| SHA-256 | 256 bits = 32 B. There is no SHA-128; MD5 is 128-bit. |

**Estimation template:** `daily users x actions/day / 100k s = avg QPS; peak = 3-5x`. `events/day x bytes = storage/day; x365 x replicas = yearly`.

---

## 3. Building blocks

### Relational databases and ACID
- Pick relational for relations and ACID. C in ACID = only the rules the DB can check (constraints); app rules are yours.
- Durability = write-ahead log flushed before commit ack.
- **Two anomalies ACID does not save you from by default:** lost update (both read 5, both write 6) and write skew (two doctors both see 2 on call, both leave). Fixes: `SELECT ... FOR UPDATE`, optimistic `version` column with compare-and-set, serializable + retry, or let the DB do the math (`SET count = count + 1`).
- At scale you drop first: cross-shard foreign keys, cross-shard joins (denormalize), cross-shard transactions (shard key keeps them local; sagas otherwise).

### Isolation levels
| Level | Dirty read | Non-repeatable | Phantom | Lost update / write skew |
|---|---|---|---|---|
| Read Uncommitted | yes | yes | yes | yes |
| Read Committed | no | yes | yes | yes |
| Repeatable Read | no | no | allowed (engine-dependent) | write skew yes |
| Serializable | no | no | no | no |
- MVCC: writers create new row versions; readers see a snapshot; nobody blocks anybody. Repeatable Read = one snapshot per txn; Read Committed = snapshot per statement.
- **Engine traps:** MySQL InnoDB default RR (gap locks stop most phantoms); PostgreSQL default Read Committed. Serializable: InnoDB locks reads; PostgreSQL SSI detects and aborts, so you must retry.
- Keep transactions short; never hold one across a network call.

### Replication and scaling
- Vertical first (has a ceiling, needs reboot). Read replicas fix read load only; sharding fixes writes.
- Sync: zero lag, writes stall if replica down. Async: fast, loses un-replicated writes on failover. **Semi-sync** (wait for one replica) is the production middle.
- **Lag bugs:** read-your-own-writes (route writer's reads to primary for a few seconds, or carry a version) and monotonic reads (pin user to one replica).
- Failover: promote most-caught-up replica; **fence** the old primary (split-brain otherwise).
- Quorum (leaderless, Dynamo/Cassandra): N copies, W + R > N, e.g. 3/2/2.
- Consensus (Raft/Paxos): majority picks the leader so two cannot exist in one term.

### Sharding and partitioning
- Horizontal = split rows; vertical = split columns/tables.
- **Shard key:** high cardinality, even spread, present in the hot queries. Bad: date, status.
- Hash (even, no range scans), range (range scans, hot ranges), directory (flexible, lookup dependency).
- **Hot key:** salt it (`key#0..9`, fan-in on read), split the shard, cache it.
- **Resharding:** pre-split into many logical shards (1024) on few nodes; consistent hashing; live migration = dual-write, backfill, verify, cut over.
- Scatter-gather latency = slowest shard. Design the key so hot queries never need it.
- Cross-shard txn: 2PC (blocking, slow), sagas (compensate), or avoid by co-locating.
- Index: local (cheap write, scatter read) vs global (one-partition read, extra eventually-consistent write; DynamoDB GSI).

### Non-relational stores
- Document (MongoDB): JSON, partial updates, closest to relational. **Elasticsearch is a search engine, not a primary store** (no transactions, can lose writes on relocation); feed it from the source of truth.
- KV (Redis, DynamoDB): GET/PUT/DEL, shards for free, no aggregations.
- Wide-column (Cassandra, Bigtable, HBase; DynamoDB with sort key): partition key + sort key, one table per query, write-heavy, time-series shape.
- Graph (Neo4j): relationships and traversals.
- **LSM** (fast sequential writes, compaction, read/space amplification) vs **B-tree** (fast point reads, in-place updates).
- **CAP:** during a partition pick C or A. **PACELC:** the rest of the time you still trade latency vs consistency. Tunable: Cassandra QUORUM/ONE, DynamoDB `ConsistentRead`.

### Picking a database
1. Write down the top queries first; model for them (DynamoDB single table: `PK=USER#42, SK=ORDER#<ts>#id` and `PK=ORDER#id` for both access patterns; duplicate writes buy single-lookup reads).
2. Fits one node? Strong consistency needed where? Range/search/graph queries or pure KV? Durability and regions? Who runs it, managed, cost? How hard to migrate off?
- Safer default when nothing is specific: managed relational. Move a piece of data only on a measured need. "Future-proof" is not a requirement.

### Caching
- Cache-aside (read-through by app), write-through (write both, no misses on fresh data), write-behind (cache first, flush later, can lose data), write-around (skip cache on write).
- Hit ratio is the first metric; the DB must survive miss traffic and 100% if the cache dies. Eviction: LRU default, LFU for long-lived hot keys, TTL bounds staleness. Negative caching with short TTL.
- **Stampede:** per-key lock / request coalescing, probabilistic early refresh, TTL jitter, warm on deploy.
- **Hot key:** in-process cache with 1 s TTL in front of Redis; replicate under several names.
- **Consistency:** delete-on-write + short TTL is the usual compromise; CDC-driven invalidation when many writers. There is always a stale window; bound it, don't deny it.
- **Cache down:** degraded page, cap DB concurrency with fast 503s, warm standby, or size DB for 50% hit ratio. Decide before.
- Levels: browser (Cache-Control, ETag), CDN (`s-maxage`, purge is slow, versioned URLs are the real invalidation, origin shield, signed URLs), in-process, Redis, denormalized DB column (atomic `UPDATE ... +1`, reconciliation job). Each level adds a stale window.
- Don't cache: fast queries, rarely-read data, strongly consistent data (balances), huge values.

### Message queues
- Use for long-running work and cross-service triggers. Buffer, retry, decouple.
- **Delivery:** at-most-once, at-least-once (SQS/RabbitMQ default), exactly-once = at-least-once + idempotent consumer.
- **Idempotency key** per message; dedup table or `SET NX`; prefer "set to value" over "increment"; write the key in the same txn as the work.
- Visibility timeout (SQS) hides in-flight messages; too short = duplicates. DLQ after N failures; alarm on depth. Poison messages.
- Retries: exponential backoff + **jitter** (retry storms), capped attempts, one layer retries.
- Backpressure: alarm on **age of oldest message**, not depth. Scale consumers, shed, slow producers.
- Ordering: standard queues don't; FIFO per message group at lower throughput. Carry full state, not deltas.
- **Dual write (DB then queue) is not atomic.** Transactional outbox: event row in the same DB txn, relay publishes. Or CDC.

### Kafka and streams
- Streams = one message, many consumer types, retained and replayable. Kafka does **not** fix the DB-to-Kafka dual write; outbox or CDC (Debezium) does.
- Topic → partitions; partition key gives per-key order; skewed key = hot partition.
- Consumer group: every group sees everything; within a group one partition per member. Max active consumers = partitions (over-provision partitions; adding later breaks key mapping). Offsets: commit after processing, handle duplicates. Rebalance pauses.
- Retention by time/size; compaction keeps last per key.
- Durability: replication factor 3, `acks=all`, `min.insync.replicas=2`.
- Exactly-once stops at Kafka's edge; your DB write still needs idempotency.
- **Consumer lag** is the metric.

### Realtime pub/sub
- Redis Pub/Sub: push, fire-and-forget, no persistence, offline subscribers miss, slow subscribers get disconnected. Redis Streams/Kafka for replay.
- Config push = version number on the channel + fetch on start + slow poll as safety net.
- WebSocket fan-out: every connection server subscribes; publish once; reconnect asks "what did I miss since id X".

### Load balancers
- L4 (TCP, fast, no HTTP routing, NLB) vs L7 (path/header routing, TLS termination, ALB).
- Round robin, weighted, least connections / least outstanding requests (handles slow servers), power-of-two-choices, hash (affinity, hot spots).
- Health checks: active (`/health`) and passive (watch 5xx); slow start; fail-open when all unhealthy. Connection draining on deploy.
- Sticky sessions: avoid (state on server) except WebSockets.
- The LB is a SPOF unless managed/HA pair/anycast. Global: GeoDNS (TTL-bound failover) or anycast. Client-side LB (gRPC) at very large fan-out.

### Circuit breakers and resilience
- Cascades come from calls with no (or too long) timeouts filling thread pools. **Timeouts first**, set from p99 + headroom.
- Real breaker = client-side state machine per dependency: **closed** (count failures in a sliding window) → **open** (fail fast for a cool-down) → **half-open** (trial calls). Knobs: failure-rate threshold with minimum volume, cool-down with jitter.
- A central "breaker DB" everyone polls is a kill switch, not a breaker: shared hot-path dependency, polling lag, one switch for the fleet. Keep it only as a manual override.
- Fallbacks (cached/default/degraded feature). Retries: only retryable errors, backoff + jitter, **retry budget** (~10%), one layer. Bulkheads (pool per dependency). **Load shedding** protects the callee: reject early and cheap, prioritize. Library (Hystrix/resilience4j) vs sidecar (Envoy).

### Redundancy and recovery
- **RPO** (data you can lose, in time) and **RTO** (time you can be down). Say the numbers.
- Backups: daily incremental + weekly full → RPO up to a day. **Point-in-time recovery** from WAL/binlog → RPO minutes; the only undo for a bad `DELETE` (replicas delete too).
- A backup never restored is a hope: restore drills, alarm on "no verified backup in X hours". Different account/region, immutable (Object Lock).
- Multi-region: active-passive (promote + DNS, start here) vs active-active (needs a conflict rule per table: last-writer-wins, per-key home region, or Spanner-style TrueTime + Paxos).

### Leader election
- Majority vote (Raft/Paxos); fewer than a majority alive = no leader, by design. Odd node counts.
- Use ZooKeeper/etcd/Consul or a DB lease with TTL (DynamoDB conditional write). Lease longer than GC pauses; heartbeat 1-3 s, lease 10-15 s.
- **Split-brain** fix = **fencing token** (monotonic epoch; storage rejects older). Election storms (randomized timeouts); alive-but-stuck (work-based health, not just heartbeat).
- In practice: Kubernetes controllers, autoscaling groups, Patroni.

### Protocols
- TCP: 3-way handshake (SYN, SYN-ACK, ACK); **4-way teardown** (FIN/ACK each way), TIME_WAIT.
- HTTP/1.1 is **persistent by default**; `Connection: Keep-Alive` is the 1.0 idiom; head-of-line blocking (browsers open ~6). HTTP/2 multiplexes streams on one TCP (one lost packet stalls all). HTTP/3 = QUIC over UDP.
- gRPC inside (HTTP/2 + protobuf, typed, streaming); REST/JSON at the edge.
- Realtime: long polling (fallback), SSE (one-way push, simple), WebSocket (two-way). Scaling WebSockets: sticky routing, connection servers + pub/sub, reconnect with backoff + resume, ping every 20-30 s past proxy idle timeouts.

### Blob storage (S3)
- Immutable objects, key prefixes not folders, not a file system.
- **High latency per request, huge throughput in parallel.** Bad for a million 1 KB reads, great for a 10 GB scan. **Strongly consistent** since late 2020.
- 11 nines durability protects from S3 losing data, not from you deleting it: versioning, Object Lock.
- **Presigned URLs**: client moves bytes directly; API only signs and records metadata. Multipart above ~100 MB. Event notifications (at-least-once) trigger pipelines. Storage classes + lifecycle rules. CDN in front for public content. Cost = storage + requests + egress.

### Bloom filters
- "Definitely not" with certainty, "maybe" otherwise. Insert-only, no deletes (counting/cuckoo filters if you must).
- k hashes, m bits: `m ≈ -n ln p / (ln 2)^2`, `k ≈ (m/n) ln 2`. 1M keys at 1% ≈ 9.6M bits ≈ 1.2 MB, k ≈ 7.
- False positive consequence: a profile never shown (fine), a valid payment rejected (not fine; hit the DB on "maybe"). Alarm on fill ratio > 50%; rebuild from the durable log.

### Consistent hashing
- Solves data ownership, not data movement. Modulo hashing moves almost everything on N→N+1; the ring moves ~1/N.
- Key owned by first node clockwise. **Virtual nodes** (100-256 per node) fix uneven arcs, spread a failed node's load across all, allow weights.
- Replication: next N distinct physical nodes (Dynamo preference list) + quorum; hinted handoff.
- Alternatives: rendezvous hashing (O(nodes), no ring), jump hash (numbered buckets), fixed slots (Redis Cluster 16384).
- Ring lives in proxy or smart clients; membership via config store or gossip. "The ring is a hint, the node is the truth."

### Big data processing
- MapReduce: map per record, **shuffle** (group by key across network, the expensive step), reduce per key.
- **Skew:** combiners (pre-aggregate on mapper), salting hot keys + second aggregation, broadcast small joins. Watch max vs median task time.
- Batch (finite) vs stream (infinite). Event time vs processing time. Windows: tumbling, sliding, session. **Watermark** = "I have seen everything up to T"; late data dropped or updates emitted.
- Exactly-once = checkpointed state + replayable source + idempotent/transactional sink.
- Lambda (batch + speed layers) vs Kappa (stream only, replay from Kafka). Parallelism = partitions. Task retries, speculative execution for stragglers.

---

## 4. Case study cards

Each card: the hard part (where to spend time), key decisions, failure modes, trade-offs, numbers.

### E-commerce product listing (100 items)
- **Hard part:** there isn't one; say so. Structure (frontend, backend, DB, admin) and knowing what changes at 10x/100x.
- **Design:** LB → API fleet → SQL. Read replica for read-heavy; cache-aside with delete-on-write invalidation; owner reads to primary (read-your-own-writes).
- **Failures:** primary down → read-only from replica; cache down → DB takes all reads (fine at 20 QPS, not at 2k); replication lag → owner's edits "vanish".
- **At scale:** pagination + indexes, CDN for images, search index via CDC at 100k products.
- **Numbers:** 10k shoppers x 10 pages = 1-2 QPS avg, 20 peak; 10,000:1 read/write.

### Rate limiter
- **Hard part:** correct enough while off the critical path; behaving well when the limiter itself is sick.
- **Algorithms:** fixed window (edge bursts), sliding log (exact, O(limit) memory), sliding counter (approx, 2 ints), **token bucket** (bursts up to B, smooth average; pick this), leaky bucket (smooth output, adds latency).
- **Atomicity:** `INCR` + `EXPIRE` in one Lua script; naive GET/SET races and over-admits. Use Redis time, not app clocks.
- **Placement:** library in proxy/service (2 fewer hops) vs service (central, more hops). **Local + global tiers:** per-server token bucket synced to Redis every ~100 ms; ~100x less Redis load, a few % over-admission.
- **Failures:** Redis down → **fail-open** by default with local fallback + alarm; fail-closed for login/payment. Redis slow → tight timeout or you become a latency amplifier. Hot key (one abuser) → reject locally once over limit. Bad config (limit 0) → validation, staged rollout, floor.
- **Contract:** 429, `Retry-After`, `X-RateLimit-Limit/Remaining/Reset`.
- **Multi-region:** regional limits (bounded over-admission) over a global counter (100 ms per request).
- **Numbers:** 20 B x 100M keys ≈ 2 GB; 1M req/s = 10+ Redis shards without the local tier.

### Notification service
- **Hard part:** fan-out throughput without starving P1; "exactly once" to the user on at-least-once plumbing.
- **Design:** control service + templates (versioned, render at enqueue) → SQS per priority (P1/P2/P3, own worker pools) → providers. Bulk: iterator service splits 100M users into id ranges with checkpoints.
- **Dedup:** `SET notif:{user}:{campaign}:{channel} NX EX` before sending; the key is the idempotency key. Bloom filter only as a pre-filter (false positive = silently skipped user).
- **Failures:** provider 429/5xx → backoff + jitter, DLQ, per-provider health and quotas, failover provider. Tracker down → P1 sends anyway, marketing pauses. Iterator crash → resume from checkpoint. Queue growth → alarm on age of oldest message. Bad template → canary cohort + kill switch. Stale tokens → mark channel invalid.
- **Unsaid requirements:** opt-out, quiet hours (local time = 24 campaigns), frequency caps, consent audit, delivery receipts (at-least-once too), scheduling.
- **Numbers:** 100M sends/hour ≈ 28k/s; tracker 100M x 8 B = 800 MB per campaign; SMS providers cap at hundreds/s.

### Realtime abuse masker
- **Hard part:** seeing it is a library, not a service; matching quality.
- **Design:** dictionary from S3 at startup, in-process. Trie-with-reset misses glued/overlapping words → **Aho-Corasick** (failure links, one pass, O(text + matches)). Normalize first (lowercase, Unicode fold, collapse repeats, leetspeak map). Whole-word by default + **allowlist** (Scunthorpe problem). Async ML classifier for context.
- **Hot reload:** versioned file + pointer in S3, pub/sub "reload v42", build on the side, atomic swap, poll as safety net, keep last good copy.
- **Failures:** S3 down at startup → baked-in list, never start unmasked. Blank line matches everything → validate on admin side. Common word added by mistake → preview hit count, flip pointer back.
- **Why not a service:** TCP setup + teardown + network I/O per message, plus a dependency that can take chat down.

### Tinder feed
- **Hard part:** geo queries with skew; match correctness under concurrency; never repeat, cheaply.
- **Geo:** geohash/S2/H3 cells; shard by cell; query cell + 8 neighbours (boundary problem); precision ≈ search radius; dense cities → adaptive split, cache hot cells. Redis GEO uses geohash in a sorted set.
- **Feed DB:** `user_id` hash key, `created_at` range key, denormalized candidate card (Approach 2) with TTL; never a list document.
- **Match race:** A and B swipe simultaneously, both see "not yet", no match. Fix: canonical pair key `min(a,b):max(a,b)` with a conditional atomic write; or route both to one partition; or a reconciliation job.
- **No repeats:** per-user Bloom filter in Redis; false positive = profile never shown (acceptable); ~12 KB for 10k swipes at 1%; rebuild from Feed DB.
- **Failures:** filter lost → fail closed (rebuild first). Generator backlog → trigger refill at 5 remaining. Location spoofing → reject implausible jumps.
- **Privacy:** store cell not raw coords, round distances, TTL locations.
- **Numbers:** 12 B x 50M = 600 MB; 50M users every 30 s ≈ 1.7M writes/s → send only on movement.

### Twitter trends
- **Hard part:** windowed counting over a firehose with skew and lateness; scoring acceleration not size; spam.
- **Pipeline:** Kafka (tweets) → filter (spam) → entity tagging → windowed aggregation keyed by (location, entity) → scorer → enricher (news clusters, top article, image) → Trends DB → cached API (60 s TTL).
- **Windows:** tumbling vs sliding; event time not processing time; watermarks for late tweets. **Approximate counting:** count-min sketch + top-k (Space-Saving); exact map won't fit.
- **Scoring:** `count_now / (baseline + c)` with exponential decay (half-life ~30 min); distinct users as anti-spam weight.
- **Spam:** velocity per account, reputation/age, simhash near-duplicates, coordinated-burst detection.
- **Failures:** Kafka lag → serve last good window with "as of"; job crash → checkpoint restart, idempotent per-window writes; enrichment down → trends without articles; spam breakthrough → hold-for-review state.
- **Numbers:** 350k tweets/min ≈ 6k/s, 5-10x spikes.

### URL shortener
- **Hard part:** unique, unguessable, short codes at high write rate with no single point of contention; redirect path fast and always up.
- **Codes:** hash (too long, collisions), int (predictable), base-62 of a random-ish id. **Ticket servers:** ranges table, pick a range at random, atomic `UPDATE ... current+1 ... RETURNING` (naive SELECT-then-UPDATE has a lost update); prefetch blocks of ids per API server. Alternative: **KGS** pre-generated random keys (truly unguessable, costs a pool).
- **301 vs 302:** 301 is cached by browsers/CDNs → analytics undercount, can't disable the link. 302/307 hits you every time. Pick 302 if analytics is a feature.
- **Read path:** cache-aside, set on create (new links are hot), 80-90%+ hit ratio; async Kafka publish for analytics (never block the redirect).
- **Failures:** cache down → DB sized for a fraction of peak + tight Redis timeout; ticket DB down → creates fail only after prefetched blocks run out; Kafka down → analytics lost, redirects fine; malicious destinations → scan on create, re-scan popular, report/takedown, interstitial.
- **Expiry/abuse:** 410 for expired, 404 for never-existed, never reuse codes, rate-limit creation. Regional ranges avoid cross-region coordination.
- **Numbers:** 100M/month ≈ 2k/min avg; 128 B x 100M = 12.8 GB/month; 2^48 ≈ 5.6e14 codes at 8 chars; 1.2e9/year per range → ~500k ranges.

### PasteBin
- **Hard part:** getting bytes off the API servers; lifecycle correctness (expiry, delete, edit, CDN); secrecy without auth.
- **Design:** metadata in relational/KV (sharded by uid), content in S3. **Presigned PUT** (PENDING → client uploads → complete/S3 event → READY; size cap in the policy) and **presigned GET or 302** (API checks visibility/expiry, never streams bytes). CDN for public pastes; versioned keys for edits.
- **Secret pastes:** 128-bit random ids, no listing, short-lived signed URLs, owner-only edit, SSE at rest + TLS.
- **Expiry:** 410 with tombstone vs 404; stop signing URLs within `url_ttl` of expiry so the cleanup job doesn't race readers; S3 versioning makes a buggy cleanup reversible.
- **Don't cache 10 MB blobs in Redis**; the CDN is the cache for the head of the long tail; cache the meta row.
- **Analytics:** Elasticsearch as secondary index only, fed via Kafka; CDN/S3 access logs once reads bypass the API.
- **Numbers:** 10M x 10 MB = 100 TB/month worst case; 1:50 → 5 PB read; metadata 10M x 168 B ≈ 1.6 GB/month.

### Fraud detection
- **Hard part:** 200 ms with rich features; training/serving skew; behaviour when the fraud service fails.
- **Design:** Txn API → sync call to Fraud Detection (score 0-1 + reasons → allow / step-up / hold). Model (random forest / GBT) from S3; **rules engine** around it (denylists, caps, instant to change). Offline: Spark moves txn DB + CS DB to S3 → Spark MLlib trains → model to S3.
- **Features:** velocity counters in Redis (`INCR` with TTL, fed by a stream job), profile baselines, graph features (shared device/IP/beneficiary as precomputed tables). **Feature store** so offline and online definitions match; log features used per decision.
- **Latency budget:** ~20 network, ~50 features (parallel), ~30 model, ~10 rules, ~90 headroom.
- **Labels:** delayed (30-90 days), biased (only what we allowed or CS confirmed; let a sample of "would block" through), imbalanced (measure precision/recall, not accuracy).
- **Rollout:** versioned artifacts, shadow mode, canary 5%, config rollback; alarm on score distribution drift (usually a broken feature, not fraud).
- **Fail-open vs fail-closed:** low value allow + async re-score; high value step-up/hold; tight timeout + breaker.

### Recommendation engine
- **Hard part:** scale and speed (two-stage split); knowing whether it works (offline metrics lie a little; A/B with guard rails); cold start and freshness.
- **Two stages:** candidate generation (cheap, recall: item-to-item table, ANN neighbours, popular, recent; ~1000 of 100M) → ranking (expensive model on the shortlist) → re-rank (seen/bought filter via Bloom, diversity, stock, rules).
- **Methods:** content filtering (exploit; cosine over hand features → **embeddings + ANN**, two-tower, FAISS/HNSW), collaborative filtering (explore; graph DB is the teaching version; production = precomputed **item-to-item** similarity table or ANN index).
- **Cold start:** new user → popularity + onboarding + context; new item → content features + exploration quota.
- **Freshness:** nightly rebuilds + realtime short-term profile (last 20 items in Redis, TTL) blended in.
- **Evaluation:** precision@k/recall offline; CTR/conversion/dwell online; guard rails (latency, returns, diversity, clickbait).
- **Failures:** service down → popular items or hide widget, never block the page; stale index → alarm on age; bad model → canary + rollback; feedback-loop degeneracy → track coverage, keep exploration.

### Web crawler
- **Hard part:** the frontier (priority + politeness + hundreds of workers + no host starves the rest); deciding what not to fetch; a 320 TB index that serves in ms.
- **Frontier (Mercator):** front queues by priority → back queues one per host → heap on `next allowed fetch time`; one connection per host, ≥1 s or `Crawl-delay`. robots.txt cached 24 h. Local DNS cache (DNS was a top bottleneck).
- **Storage:** crawlers write local disk → daemon zips → S3 partitioned by time; mark crawled only after upload. URL DB (DynamoDB, hash key = domain) with `content_hash`, `simhash`, `change_count`, `status`, `next_crawl_at`.
- **Dedup:** exact hash catches byte-identical only; **simhash/minhash** over shingles for near-duplicates (~28% of the web). Bloom filter for "not recently crawled" (false positive = skipped until next cycle).
- **Freshness:** per-page change rate (multiply interval on unchanged, divide on changed), `If-Modified-Since`/ETag, sitemap `lastmod`.
- **Traps:** canonicalize URLs (strip `utm_*`, sessionid, fragments), cap depth and per-host pages, flag hosts with endless new URLs and repeating hashes.
- **Ownership:** consistent hashing of hosts to crawlers (politeness + DNS locality for free) with vnodes; dead crawler → hosts reassigned, IN_PROGRESS with old timestamps re-queued. 429/5xx from a host → back off that host only.
- **Index:** `1M words x (8 + 10M x 32 B) ≈ 320 TB`; doc-id compression and gap encoding cut it to tens of TB; document-partitioned for serving (scatter-gather), term-partitioned for building. Atomic index swap after validation.
- **Numbers:** 1B pages / 30 days ≈ 400 pages/s, ~100 TB/month raw.

---

## 5. Misconceptions that get probed

| Common claim | Correct version |
|---|---|
| "Return a 301 and log analytics" | 301 is cached by browsers and CDNs; repeat visits never reach you. Use 302/307 if you need counts or control. |
| "Kafka fixes the half-failed write" | Kafka fixes "one message, many consumers". DB + Kafka is still a dual write; fix with transactional outbox or CDC. |
| "Circuit breaker = central DB of flags that services poll" | That's a kill switch. Real breakers are client-side state machines (closed/open/half-open) on local failure rates. |
| "Serializable means every read locks" | Only in InnoDB. PostgreSQL uses SSI: no read locks, aborts on conflict, you retry. |
| "Repeatable Read protects me from write skew / lost update" | Snapshot isolation does not. Use `FOR UPDATE`, a version column, or atomic in-DB arithmetic. |
| "HTTP/1.1 closes after each response; send Keep-Alive" | 1.1 is persistent by default; Keep-Alive is the 1.0 idiom. The real 1.1 limit is head-of-line blocking. |
| "TCP teardown is 2-way" | 4-way FIN/ACK (3 when piggybacked). |
| "S3 reads are slow" | High per-request latency, huge parallel throughput. Also strongly consistent since 2020. |
| "Elasticsearch is a document database" | It's a search engine. Secondary index only; keep a durable source of truth. |
| "SHA-128" | Doesn't exist. MD5 is 128-bit; SHA-256 is 32 B; for routing use MurmurHash/xxHash. |
| "Bloom filter = one hash into a bit array" | k hashes; size by `m ≈ -n ln p / (ln 2)^2`; can't delete. |
| "Consistent hashing with one slot per node" | Needs virtual nodes or arcs are uneven and a failure doubles one neighbour's load. |
| "Nothing specific → MongoDB to future-proof" | Managed relational is the safer default; move on a measured need. |
| "Reset the trie on each separator" | Misses glued/overlapping words; that's Aho-Corasick's job. Normalize first; whole-word + allowlist. |
| "Exact content hash dedups the web" | Only byte-identical pages. Use simhash/minhash. |
| "Check tracker, send, then mark" | Race under at-least-once delivery. Atomic `SET NX` first, then send. |
| "Mark my swipe, then check theirs" | Two simultaneous swipes never match. Conditional write on a canonical pair key. |
| "Read replicas scale writes" | They don't. Sharding does; resharding is the hard part. |
| "Backups are configured" | Backups never restored are a hope. Drill restores; PITR for bad deletes; immutable copies in another account. |

---

## 6. Closing checklist (say before you stop)

1. Components with exclusive responsibilities and clear owners.
2. For each: DB/caching, scaling/fault tolerance, async, communication, consistency, observability.
3. Failure modes written down: slow, down, wrong data; what the user sees; what we do.
4. SLOs named with an error budget; metrics and alarms tied to them.
5. Rollout and migration plan with rollback (flag, canary, dual-write, backfill, cut over).
6. Cost: the two or three biggest line items.
7. Regional vs global components; what happens when a region is lost.
8. What breaks at 10x and what replaces it.
