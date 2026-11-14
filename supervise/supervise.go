// Package supervise keeps one background runner's failure from becoming the
// whole service's (ADR-0019).
//
// Every long-running component in this framework — the relay, a projection's
// catch-up runner, its inbox reader and its inbox writers — makes the same
// promise: a cancelled context returns nil, anything else returns the error,
// and progress lives in a durable checkpoint so running it again resumes from
// there. Their doc
// comments say out loud who acts on that error — "the supervisor restarts
// from the checkpoint, so the in-flight batch is re-applied" — and until this
// package there was no supervisor anywhere, including in the example every
// service copies its composition root from. Those services put the runners in
// an errgroup beside the HTTP server, and an errgroup cancels every member the
// moment one returns an error, so a single message a projection could not
// apply took the service's API down with it and the orchestrator restarted the
// process straight back into the same message: a deterministic failure in a
// background queue became CrashLoopBackOff for a service whose read models
// were perfectly readable.
//
// A supervisor is a loop around that contract and nothing more. It knows a
// runner's name, its own backoff, and two callbacks; it does not know what an
// HTTP server is, which runners are siblings, or what failure is fatal to the
// process. That judgement belongs in the composition root, which is the only
// place that can make it.
//
// Wiring, in the composition root, beside the runners it supervises:
//
//	sup := supervise.New(supervise.Config{
//	    Restarted: o11y.RunnerRestartHook(),
//	    Report:    reportRunnerFailure, // the service's logger
//	})
//	sup.Go(ctx, "ordering.relay", rly.Run)
//	sup.Go(ctx, "ordering.order_summary.catchup", catchUp.Run)
//
//	err := serveHTTP(ctx, port, handler) // the one runner that IS fatal
//	stop()                               // cancel; runners wind down
//	sup.Wait()
//
// This package imports the standard library only. A framework module that
// reached for OpenTelemetry or called slog directly would force both on every
// service that wants a restart loop, which is the same line projection.Config's
// Observe hook and InboxWriterConfig's OnRejected draw (ADR-0017, ADR-0018):
// gohex owns the mechanism, the service owns the policy. What stays inside is
// the part that is mechanism — whether a failure is NEWS is a question about
// the backoff state machine, and a caller reimplementing it would get it
// wrong.
//
// The runners themselves are in the relay and projection modules; this package
// imports neither, because a supervisor needs nothing of a runner but its
// Run method and its name.
package supervise

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// ErrStopped is the error a runner is restarted with when it returned nil
// while its context was still live.
//
// The runner contract says nil means "my context ended". A runner that says it
// with the context still open has broken that contract — it returned early for
// a reason it decided not to name — and the honest reading is a failure, not a
// completion. The prototype this package grew from returned from the loop in
// that case, which meant a projection could stop for ever and emit nothing at
// all: no error, no log, no metric, no failing probe, with the process happily
// serving a read model that had quietly stopped advancing. That is exactly the
// invisible-failure class this line of work exists to kill, so the stop is
// turned into a reported, counted restart instead: visible on the counter,
// named in the log, and the runner is started again, which for every runner in
// this framework is the correct response anyway (it resumes from its
// checkpoint).
var ErrStopped = errors.New("supervise: runner returned nil while its context was still live")

// ErrPanic marks a restart caused by a recovered panic; the panic value is
// formatted into the wrapping error and the stack rides along in
// [Failure].Stack.
//
// Recovering is a deliberate choice, and the dangerous half of it is the
// silence a recover usually brings: a swallowed bug is worse than a crash,
// because a crash at least tells somebody. So a panic costs a runner exactly
// what an error costs it and never less — it is counted as a restart, it is
// handed to Report like any other failure, and it carries the stack the
// crashing process would have printed. What recovering buys is the promise
// this package exists for: an index-out-of-range in one projection handler is
// that projection's problem, not the API's. The alternative — letting it
// unwind the process — hands the panicking runner the power over its siblings
// that the whole package is here to take away, and makes the most common
// application bug the one case where the supervisor does nothing.
var ErrPanic = errors.New("supervise: runner panicked")

// Backoff paces a failing runner. Without a ceiling a runner that can never
// succeed retries at the pace of its first failure for ever; without a floor
// it spins.
type Backoff struct {
	// First is the wait after a failure that is news, and the wait every
	// subsequent failure doubles from.
	First time.Duration
	// Most is the longest a runner is ever left alone between attempts: a
	// wedged runner costs a slow pulse, not a hot loop.
	Most time.Duration
	// Healthy is how long a run must last before its failure counts as a NEW
	// problem rather than the same one again. A runner that fell over after an
	// hour of work was working; one that falls over after 200ms never was.
	Healthy time.Duration
}

// DefaultBackoff retries within a second, settles at one attempt every five
// minutes, and trusts a run that lasted a minute. It is what [Config] fills
// unset fields from; a service with a different sense of urgency replaces it.
var DefaultBackoff = Backoff{First: time.Second, Most: 5 * time.Minute, Healthy: time.Minute}

// Failure is one restart worth reporting: the supervisor hands it to
// [Config.Report] when the failure is news, never on every repeat.
type Failure struct {
	// Runner is the supervised name, e.g. "ordering.order_summary.inbox".
	Runner string
	// Err is what the runner returned, or a wrapped [ErrPanic].
	Err error
	// Stack is the goroutine stack at the moment of recovery, non-nil only
	// when Err wraps [ErrPanic].
	Stack []byte
	// RetryIn is how long the supervisor waits before starting the runner
	// again.
	RetryIn time.Duration
}

// Config configures a [Supervisor]. Both callbacks are optional and may be
// called from any supervised goroutine, so an implementation must be safe for
// concurrent use.
type Config struct {
	// Backoff paces every supervised runner. Unset fields take their value
	// from [DefaultBackoff].
	Backoff Backoff
	// Restarted is called once per restart, before the wait, with the runner's
	// name — it is where a counter is incremented, and it is a callback so
	// this package does not decide what a metric is called (o11y supplies the
	// framework's one: gohex.runner.restarts, through o11y.RunnerRestartHook).
	//
	// It is given the runner's name and NOTHING else, deliberately. A runner's
	// error can carry whatever a failing handler put in it, which on a real
	// deployment means customer data, and a metric is aggregated and kept far
	// longer than a log. The error goes to Report.
	Restarted func(ctx context.Context, runner string)
	// Report is called when a failure is news: the first one, and the first
	// one that reads differently from the one before it. A runner failing
	// every second for a day is one fact, not eighty thousand log lines; the
	// counter Restarted feeds is what carries "it is STILL failing", and this
	// is what says why. A run that lasted [Backoff.Healthy] makes the next
	// failure news again, so a runner that recovered and later broke is
	// reported again rather than hidden by what it said last week.
	Report func(ctx context.Context, f Failure)
}

// Supervisor runs named runners, each in its own loop. It never cancels
// anything: the only thing that ends a runner's loop is the context the caller
// gave it.
type Supervisor struct {
	backoff   Backoff
	restarted func(ctx context.Context, runner string)
	report    func(ctx context.Context, f Failure)
	running   sync.WaitGroup
}

// New builds a Supervisor from cfg, filling unset backoff fields from
// [DefaultBackoff] and nil callbacks with no-ops.
func New(cfg Config) *Supervisor {
	s := &Supervisor{
		backoff:   cfg.Backoff,
		restarted: cfg.Restarted,
		report:    cfg.Report,
	}
	if s.backoff.First <= 0 {
		s.backoff.First = DefaultBackoff.First
	}
	if s.backoff.Most <= 0 {
		s.backoff.Most = DefaultBackoff.Most
	}
	if s.backoff.Healthy <= 0 {
		s.backoff.Healthy = DefaultBackoff.Healthy
	}
	if s.restarted == nil {
		s.restarted = func(context.Context, string) {}
	}
	if s.report == nil {
		s.report = func(context.Context, Failure) {}
	}
	return s
}

// Run calls run until ctx is done, starting it again after every failure with
// the configured backoff. It returns only when ctx is done, and it returns
// nothing: there is no error for a caller to act on, because acting on the
// error is what this loop does.
//
// A failure is anything other than a cancelled context: an error from run, a
// panic inside it ([ErrPanic]), or run returning nil early ([ErrStopped]).
// Every failure is counted through [Config.Restarted]; it is reported through
// [Config.Report] only when it is news. Once ctx is done nothing is counted,
// reported or retried — a runner returning its context's error as it winds
// down has not failed, it has shut down.
func (s *Supervisor) Run(ctx context.Context, name string, run func(context.Context) error) {
	wait, last := s.backoff.First, ""
	for {
		began := time.Now()
		err, stack := attempt(ctx, run)
		if ctx.Err() != nil {
			return
		}
		if time.Since(began) >= s.backoff.Healthy {
			// It was working, so this failure is a new problem: pace it from
			// the start and say it out loud even if it reads the same as the
			// trouble it had last week.
			wait, last = s.backoff.First, ""
		}
		s.restarted(ctx, name)
		if said := err.Error(); said != last {
			last = said
			s.report(ctx, Failure{Runner: name, Err: err, Stack: stack, RetryIn: wait})
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if wait *= 2; wait > s.backoff.Most {
			wait = s.backoff.Most
		}
	}
}

// Go runs name in a new goroutine under [Supervisor.Run] and returns
// immediately. It is the shape a composition root wants: start the background
// runners, then block on whatever the process considers fatal.
func (s *Supervisor) Go(ctx context.Context, name string, run func(context.Context) error) {
	s.running.Add(1)
	go func() {
		defer s.running.Done()
		s.Run(ctx, name, run)
	}()
}

// Wait blocks until every runner started with [Supervisor.Go] has returned.
// It only waits: cancelling them is the caller's job, through the context it
// handed them, because a supervisor that could cancel runners would be one
// runner's failure away from being the errgroup this package replaces.
func (s *Supervisor) Wait() { s.running.Wait() }

// attempt runs one attempt, converting the two ways a runner can fail without
// returning an error into errors: a panic ([ErrPanic], with the stack as the
// process would have printed it) and an early nil ([ErrStopped]). The caller
// decides whether the result matters — a cancelled context makes it shutdown,
// not failure.
func attempt(ctx context.Context, run func(context.Context) error) (err error, stack []byte) {
	defer func() {
		if p := recover(); p != nil {
			stack = debug.Stack()
			err = fmt.Errorf("%w: %v", ErrPanic, p)
		}
	}()
	if err = run(ctx); err == nil {
		err = ErrStopped
	}
	return err, nil
}
