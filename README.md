# System Design Playbook — spoken-style answers to the 25 designs in Alex Xu's "System Design Interview" (Vol. 1 and Vol. 2), plus two travel-marketplace designs

The Volume 1 source is the PDF of the book (269 pages, 16 chapters). Chapters 1 to 3 are foundations and chapters 4 to
15 are the twelve design questions. Each book chapter runs twenty to forty pages. The documents here condense
each one to what a candidate can actually say in a 45 to 60 minute interview or a team design review, and they
add the things the book leaves out: API contracts, the data model with its query patterns, a database choice
per piece of data with the reasoning, tool choices with the reasoning, observability, and how the system is
rolled out and run.

Every document is written as narration, the way a senior or staff engineer would talk through the problem.
Each decision names the alternatives, says why one was picked for this system's requirements, and says what
was given up. The standard they are held to is the pair of reference documents in this folder, the full notes
and the cheat sheet condensed from them.

Both volumes are covered: documents 01 to 12 are Volume 1 and documents 13 to 25 are Volume 2, "System Design
Interview: An Insider's Guide, Volume 2". The Volume 2 PDF on hand was scanned images with no text layer, so
its designs were grounded in the chapter-by-chapter notes published at pagefy.io for the same book, which carry
the book's requirements, numbers, API shapes and deep-dive content, together with the book's well-known
designs.

## The designs, Volume 1

| # | System | Book chapter | Document | Where the real difficulty is |
|---|---|---|---|---|
| 01 | Rate limiter | 4 | [01-rate-limiter.md](01-rate-limiter.md) | Being correct enough without sitting on the critical path, and behaving well when the limiter itself is unhealthy |
| 02 | Consistent hashing | 5 | [02-consistent-hashing.md](02-consistent-hashing.md) | Virtual nodes, propagating membership so clients agree, and hot keys that hashing cannot fix |
| 03 | Key-value store | 6 | [03-key-value-store.md](03-key-value-store.md) | Staying available and correct at once: quorums, vector clocks, gossip, Merkle repair |
| 04 | Unique ID generator | 7 | [04-unique-id-generator.md](04-unique-id-generator.md) | Clocks that move backwards and worker IDs that must never be reused too soon |
| 05 | URL shortener | 8 | [05-url-shortener.md](05-url-shortener.md) | Unguessable codes with no contention point, a redirect path that survives the cache dying, 301 versus 302 |
| 06 | Web crawler | 9 | [06-web-crawler.md](06-web-crawler.md) | The frontier: priority and politeness for hundreds of workers; deciding what not to fetch |
| 07 | Notification system | 10 | [07-notification-system.md](07-notification-system.md) | Fan-out that never starves priority one, and effectively-once delivery on at-least-once plumbing |
| 08 | News feed | 11 | [08-news-feed.md](08-news-feed.md) | Fan-out on write versus read, the celebrity problem, bounded stale windows in every cache |
| 09 | Chat system | 12 | [09-chat-system.md](09-chat-system.md) | A stateful connection tier that survives server loss, per-conversation ordering, presence at scale |
| 10 | Search autocomplete | 13 | [10-search-autocomplete.md](10-search-autocomplete.md) | A constant-time read per keystroke, rebuilding without mutating under load, sharding a skewed prefix space |
| 11 | YouTube | 14 | [11-youtube.md](11-youtube.md) | The transcoding graph with its resource manager, and delivery cost |
| 12 | Google Drive | 15 | [12-google-drive.md](12-google-drive.md) | Strong metadata consistency with conflict handling, deduplication versus encryption, safe garbage collection |

## The designs, Volume 2

| # | System | Book chapter | Document | Where the real difficulty is |
|---|---|---|---|---|
| 13 | Proximity service | 1 | [13-proximity-service.md](13-proximity-service.md) | Turning two dimensions into one indexable key: geohash versus quadtree versus S2, and the cell-boundary problem |
| 14 | Nearby friends | 2 | [14-nearby-friends.md](14-nearby-friends.md) | Fourteen million pub/sub deliveries a second from moving points, and resizing that cluster without losing too much |
| 15 | Google Maps | 3 | [15-google-maps.md](15-google-maps.md) | Routing on a planet-sized graph with multi-level routing tiles, and live traffic re-routing |
| 16 | Distributed message queue | 4 | [16-distributed-message-queue.md](16-distributed-message-queue.md) | Durability and ordering from commodity disks, consumer rebalancing, and where exactly-once stops |
| 17 | Metrics monitoring and alerting | 5 | [17-metrics-monitoring-alerting.md](17-metrics-monitoring-alerting.md) | A million writes a second into a time-series store, pull versus push, and reliable yet quiet alerting |
| 18 | Ad click event aggregation | 6 | [18-ad-click-event-aggregation.md](18-ad-click-event-aggregation.md) | Exactly-once counts for billing under late events, duplicates, hot ads and node crashes; nightly reconciliation |
| 19 | Hotel reservation | 7 | [19-hotel-reservation.md](19-hotel-reservation.md) | Double booking: idempotency keys and optimistic locking versus pessimistic locks versus constraints |
| 20 | Distributed email service | 8 | [20-distributed-email-service.md](20-distributed-email-service.md) | A metadata store no off-the-shelf database fits perfectly, and deliverability as reputation engineering |
| 21 | S3-like object storage | 9 | [21-s3-like-object-storage.md](21-s3-like-object-storage.md) | Six nines at a sane cost: failure domains, replication versus erasure coding, packing small objects, safe GC |
| 22 | Real-time gaming leaderboard | 10 | [22-real-time-gaming-leaderboard.md](22-real-time-gaming-leaderboard.md) | Rank for twenty-five million players in real time, and what breaks when sharding a sorted structure |
| 23 | Payment system | 11 | [23-payment-system.md](23-payment-system.md) | Exactly-once money movement across four stateful systems, and reconciliation as the proof of correctness |
| 24 | Digital wallet | 12 | [24-digital-wallet.md](24-digital-wallet.md) | Atomic cross-partition transfers at a million a second with replayable history: TC/C, sagas, event sourcing, Raft |
| 25 | Stock exchange | 13 | [25-stock-exchange.md](25-stock-exchange.md) | Tens of microseconds with determinism: one server, shared-memory event store, sequencer, hot replicas |

## Beyond the books: designs from the Agoda staff question bank

These two are not in either volume. They come from the prompts collected in
[agoda-staff-platform-and-system-design-question-bank.md](../agoda-interview/agoda-staff-platform-and-system-design-question-bank.md)
and follow the same seventeen-section format and voice.

| # | System | Bank prompts | Document | Where the real difficulty is |
|---|---|---|---|---|
| 26 | Flight search and booking with multi-supplier aggregation | B1, B3, B5, R1, R8, S9 | [26-flight-search-and-booking.md](26-flight-search-and-booking.md) | Metered, pull-only aggregation under freshness and cost pressure (per-supplier budgets, cache with stampede control, cheapest-wins dedupe, cursor paging), then quote versus commitment: re-price, hold, pay, confirm, reconcile |
| 27 | Ride-sharing (Uber, Lyft) | S6 | [27-ride-sharing.md](27-ride-sharing.md) | Tens of thousands of location updates a second into a per-city geo index, and dispatch that never double-assigns a driver: one owner key with set-if-absent and expiry, sequential offers, conditional trip transitions, a sweeper for every stuck state |

Supporting documents:

- [00-approach-and-framework.md](00-approach-and-framework.md) explains the method behind every design: how to
  scope, how to turn requirements into numbers, how to estimate, how to find the hard part, the six questions to
  ask of every component, how to state a trade-off, how to walk failure modes, and how to close. Its second half
  is the writing guide: the voice every document uses, the four-beat rule for decisions, the failure-mode and
  concurrency rules, what each of the seventeen sections must contain, before-and-after samples, Mermaid
  pitfalls, and the audit to run before a document is called done.

## For whoever writes the next design, including a future session with no memory of this one

Read the writing guide in `00-approach-and-framework.md` first and treat it as binding. The short version: write
as a staff engineer talks, in complete sentences, with a reason attached to every choice and a sentence on what
that choice gives up. No label-plus-noun-list fragments, no semicolon or slash chains in place of sentences, no
arrow chains in place of narration, tables only for APIs, schemas and genuine side-by-side comparisons. Keep the
seventeen sections and the two Mermaid diagrams. Target two and a half to four and a half thousand words. Then
run the audit in the guide against the cheat sheet before finishing. A good self-test is to pick any paragraph
and ask whether a person could say it aloud to an interviewer and have the interviewer learn both what was
decided and why.

Two documents to use as the reference for voice and depth: `08-news-feed.md` for a read-heavy product system
with caching and fan-out, and `01-rate-limiter.md` for an infrastructure component with an algorithm choice and
fail-open-versus-closed decisions.
- [system-design-interview-cheat-sheet.md](system-design-interview-cheat-sheet.md) and
  [system-design-notes_ap_bg.md](system-design-notes_ap_bg.md) are the reference standard.

## What every design carries

Each document has the same seventeen sections: problem statement, scope, functional requirements,
non-functional requirements, design tenets, back-of-the-envelope estimation, components, architecture and flow
diagrams, communication between services, deep dives, API design, data model, database choices, tools and
technologies, metrics and monitoring, notification and logging, and finally CI/CD, cost, operations and what
comes next.

Within those sections, every design does the following. It states the non-functional requirements as numbers
and explains why each number is what it is. It names the hard part right after the estimates and spends the
deep dives there. It gives every component one responsibility and an owner. For every piece of data it says
which store was considered, which was chosen, why, and what was given up; it does the same for every tool. It
walks the failure modes as what breaks, what the user sees, and what we do, including whether each component
fails open or closed. It addresses what happens when two requests race. And it closes with rollout, migration,
the biggest cost line items, ownership, which components are regional and which are global, and what changes at
ten times the scale.

## Using a document in a 45 to 60 minute slot

Spend the first five to eight minutes on sections one through five, agreeing on scope and pinning the
requirements as numbers. Spend three to five on the estimates in section six and say out loud what the numbers
decide. Spend ten to fifteen on the high-level design, sections seven through nine and the API, and get the
interviewer's agreement before going deeper. Spend fifteen to twenty on the deep dives, the data model and the
database and tool choices, sections ten through fourteen. Keep the last three to five minutes for metrics,
operations and evolution, sections fifteen through seventeen. If the interviewer pulls you into a deep dive
early, follow them; the sections are a checklist, not a script, and if time runs short, cut breadth rather than
the hard part.
