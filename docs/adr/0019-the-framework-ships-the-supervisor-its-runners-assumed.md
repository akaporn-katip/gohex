# The framework ships the supervisor its runners already assumed

`relay.Relay.Run`, `projection.CatchUp.Run`, `projection.InboxReader.Run`
and `projection.InboxWriter.Run` have always documented the same
contract — "tails ... until ctx is cancelled (returns nil) or an error
occurs (returns it; the supervisor restarts from the checkpoint, so the
in-flight batch is re-applied — handlers must be idempotent)" — and that
supervisor existed nowhere: not in gohex, and least of all in the
canonical example, where all four services put every runner in an
`errgroup.WithContext` alongside the HTTP server. An errgroup cancels
every member the moment one returns an error, so the wiring the
framework taught did the exact opposite of what its doc comments
promised: one message a projection could not apply cancelled the saga
runner, the relay and the API, the process exited, and the orchestrator
restarted it straight into the same message — a deterministic failure in
a background queue became CrashLoopBackOff for a whole service whose
read models were perfectly readable. A downstream product hit it in
staging with a single hand-written Kafka record and lost four services
to it. That asymmetry between a documented contract and the wiring every
user copies is the defect, by the same test as ADR-0018: the example is
the framework's real API surface, so a bug there is every user's bug,
and the fix belongs here rather than in each service's `main.go`. The
new `supervise` module is therefore the contract's missing half — a loop
that runs one NAMED runner until its context ends, restarting it after
every failure with bounded exponential backoff (`Backoff{First, Most,
Healthy}`), counting every restart and reporting a failure only when it
is news. It never cancels, never knows its siblings, and never decides
what is fatal: `Supervisor.Wait` only waits, and the composition root
keeps the judgement that the HTTP server's failure ends the process
while a projection's does not. Two things the loop must do that are easy
to get wrong are settled here rather than left to callers. A runner that
returns nil while its context is still live has broken the Run contract
(nil means "my context ended"), and returning from the loop there — the
obvious reading — leaves a dead projection inside a healthy-looking
process with no log, no metric and no failing probe; it is reported and
restarted as `ErrStopped` instead, because silence is the failure mode
this whole line of work exists to remove. And a panic inside a runner is
recovered (`ErrPanic`), since otherwise the promise "one runner's
failure is not the service's" is false for the most common application
bug — but recovering is only defensible if it is never quieter than an
error: a panic is counted as a restart and reported with its stack,
exactly as the crashing process would have printed it. The module
imports the standard library and nothing else, by the same
mechanism-versus-policy line as `projection.Config.Observe` and
`InboxWriterConfig.OnRejected` (ADR-0017, ADR-0018): `Config.Restarted`
and `Config.Report` are callbacks, so no service is forced into
OpenTelemetry or `slog` by wanting a restart loop, while the decision
that *depends on the backoff state machine* — is this failure news, or
the same one for the thousandth time — stays inside, where it can only
be implemented once. `o11y` supplies the framework's counter,
`RunnerRestartHook()` feeding `gohex.runner.restarts` with a
`gohex.runner.name` attribute, sitting beside `gohex.relay.lag`,
`gohex.projection.lag` and `gohex.inbox.depth` (ADR-0017) because it
answers what those gauges cannot: a backlog that is not moving is either
a slow worker or a dying one. The point carries the runner's name and
nothing else — a runner's error can hold whatever a failing handler put
in it, which in production means customer data, and a metric is
aggregated and retained far longer than a log, so the error goes to the
report callback. Rejected: fixing it only in `gohex-example` (the four
`main.go` files would drift, and every service outside the example still
copies the errgroup); a supervisor that owns the HTTP server or decides
what is fatal (that is the one judgement a framework cannot make for a
service, and it is how the errgroup became load-bearing in the first
place); letting the runners retry internally (the checkpoint-and-return
contract is what makes them testable, and an infinite internal retry
hides the failure from the composition root entirely); importing
`golang.org/x/sync` for an errgroup inside the module (a supervisor is a
loop; which runners a service groups is its own business); and putting
the error text on the restart metric (unbounded cardinality and
long-lived customer data, for something the log already says better).
