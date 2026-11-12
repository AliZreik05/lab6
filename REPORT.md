# Operating Systems Lab 6 — Babble

**Participants** — Ali Zreik and Amine Wehbe

## Stage 1 — Concurrent client connections

The main thread accepts connections and starts one communication thread per client. The communication thread handles LOGIN and sends its reply before reading subsequent requests. It parses those requests and puts them in a shared, bounded FIFO queue. One executor takes commands from the queue, runs them and sends replies. Parse errors and end-of-connection cleanup are queued as PARSE_ERROR and UNREGISTER, so an error reply or socket close stays behind earlier requests from that client.

Multiple communication threads can produce commands simultaneously. A mutex protects the queue indices and count. Producers wait for space and the executor waits for work on two condition variables. Both waits recheck the relevant condition after waking. A separate mutex inside the registration functions protects lookup, insertion and removal because the communication threads register clients while the executor looks them up or unregisters them.

**Tests passed.** With random delays enabled (`-r`), `follow_test.run` and `stress_test.run -n 4 -k 10` passed in normal mode. Manual tests covered simultaneous clients, follow and publish, timeline retrieval followed by an empty timeline, disconnect and reconnect, and an invalid FOLLOW followed by a valid PUBLISH.

**Known issues.** The Stage 1 receive loop can spin on clean EOF before it has read a complete frame. The manual disconnect test observed a connection reset. A bare streaming prefix `S` can also index past the parser's token array. Stage 2 fixes both issues.

## Stage 2 — Multiple executors

### Step 2.a — Shared state

| Command | Concurrency issue |
| --- | --- |
| FOLLOW | Looks up the requester and target, checks the target's follower list for the requester, then appends if absent. Concurrent FOLLOW requests can race on the duplicate check, insertion and count. PUBLISH can also traverse or compact the list during the update. |
| PUBLISH | Looks up the publisher, traverses its follower list, appends a publication to each connected follower's timeline and removes disconnected followers. Different publishers can write to the same timeline and race on the buffer slot, next-write index and count. UNREGISTER can change a follower's connection state during traversal. |
| TIMELINE | Reads the total number of new publications, copies up to the four most recent and resets the count. A concurrent PUBLISH can make those reads inconsistent or have its increment lost during the reset. |
| FOLLOW_COUNT | Reads the same count that FOLLOW and disconnected-follower cleanup update. |

### Step 2.b — RDV and command order

A FIFO dequeue alone allows an executor to start RDV while another executor is still running an earlier PUBLISH from the same client. The client could receive an acknowledgment before its publications reach followers. Concurrent replies on one socket can also interleave, and early UNREGISTER can close the socket during a reply.

### Steps 2.c and 2.d — Implementation

Four executor threads each seed their random generator once before entering the work loop. Each client has a follower-list mutex covering FOLLOW's duplicate check and insertion, PUBLISH's traversal and cleanup, and FOLLOW_COUNT's count read. Each timeline has its own mutex. Insertion holds it from slot selection through the post write and count update. TIMELINE holds it through copying the answer and resetting the count. PUBLISH takes the follower-list lock before any timeline lock and releases each timeline lock before visiting the next follower. UNREGISTER atomically marks the record disconnected as part of removing it from the registration table.

The queue mutex also protects one active command slot per executor. An executor selects the oldest queued command whose client key has no active command and records it as active before releasing the mutex. It clears the slot after processing and sending any reply, then signals waiting executors. This preserves each client's execution and reply order while allowing work for different clients to overlap. A streaming FOLLOW for a missing user produces no reply, keeping the next acknowledgment aligned with the command that requested it. The communication thread queues UNREGISTER after EOF or a failed LOGIN reply. The socket I/O loops stop on EOF or a permanent error and retry interrupted calls.

### Verification

The clean Stage 2 build succeeded without warnings. Each test below exited with status 0 against a server running with random delays, with a 45-second timeout per invocation.

| Test | Normal mode | Streaming mode |
| --- | --- | --- |
| `follow_test.run` | Passed | Passed with `-s` |
| `stress_test.run -n 4 -k 10` | Passed | Passed with `-s` |

An additional socket test sent a streaming FOLLOW for a missing user, then 16 streaming publications and RDV before half-closing the connection. RDV was the next reply. The follower's timeline reported 16 new publications and returned the last four in order. A second timeline was empty, and after EOF the publisher's name could register again.

A second socket test checked that a bare `S`, a streaming RDV and a streaming FOLLOW for a missing user leave the next regular RDV reply aligned. It also checked that a 64-byte publication survives intact and that the server closes the connection after clean EOF.

**Known limitation.** Disconnected client records and timelines remain allocated because follower lists can still reference them. Repeated connections therefore grow memory use.

## Stage 3 — PUBLISH priority (bonus)

When several clients have work pending, strict FIFO dispatch can make publications wait behind other commands. Giving every queued PUBLISH priority would allow a client's PUBLISH to overtake its own FOLLOW or RDV and change the order that client observes.

Under the queue mutex, the executor looks for the oldest PUBLISH that is the first pending command for its client and whose client has no active command. If it finds none, it takes the oldest runnable command. This gives publications priority across clients while preserving each client's execution and reply order. The implementation uses one bounded queue and keeps the Stage 2 active slots. With sustained publication traffic, a runnable command of another type can wait as long as publications keep taking priority.

**Priority test.** Run `make -C stage_3 priority_test.run` and `timeout 5s stage_3/priority_test.run`. The test exercises queue operations in a fixed sequence. It checks that a PUBLISH overtakes an older FOLLOW from another client, that a client with an active command is skipped, and that FOLLOW, PUBLISH and RDV for the same client are dispatched in order.

**Tests passed.** The clean build and priority test passed. The Stage 2 follow and stress commands in the table also passed against Stage 3 in both modes with `-r` and the same timeouts. The socket test covering malformed streaming requests, the 64-byte publication, RDV and clean EOF passed against Stage 3.

## Feedback

The random-delay option makes it easy to exercise different thread schedules with the same test inputs. A small test dedicated to RDV ordering and clean EOF would make failures easier to isolate. A queue example with two clients would also help illustrate how FIFO dequeue and FIFO completion can differ once multiple executors are running.

## Sources and assistance

- *Lab 6: Babble — A multi-threaded server (for social networking)*, Université Grenoble Alpes, 2026, §§4–6 and §8.
- OpenAI Codex assisted with the Stage 2 and Stage 3 implementations, test execution and report drafting.
