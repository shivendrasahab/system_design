# System Design Playbook — spoken-style answers to the 12 designs in Alex Xu's "System Design Interview" (Vol. 1)

The source is the PDF of the book (269 pages, 16 chapters). Chapters 1 to 3 are foundations and chapters 4 to
15 are the twelve design questions. Each book chapter runs twenty to forty pages. The documents here condense
each one to what a candidate can actually say in a 45 to 60 minute interview or a team design review, and they
add the things the book leaves out: API contracts, the data model with its query patterns, a database choice
per piece of data with the reasoning, tool choices with the reasoning, observability, and how the system is
rolled out and run.

Every document is written as narration, the way a senior or staff engineer would talk through the problem.
Each decision names the alternatives, says why one was picked for this system's requirements, and says what
was given up. The standard they are held to is the pair of reference documents in this folder, the full notes
and the cheat sheet condensed from them.

Only one PDF was found at the path given. When the second volume is shared, its designs will be added in the
same format.

## The designs

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
