package supervise_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/supervise"
)

// fast keeps these tests in milliseconds. Healthy is an hour here, so no run
// in a test counts as having worked unless the test says so; the wall-clock
// defaults are never waited on.
var fast = supervise.Backoff{First: time.Millisecond, Most: 5 * time.Millisecond, Healthy: time.Hour}

// recorder is the in-memory stand-in for the two callbacks a service wires:
// the restart counter (o11y's gohex.runner.restarts) and the log.
type recorder struct {
	mu        sync.Mutex
	restarts  map[string]int
	failures  []supervise.Failure
	announced []string
}

func (r *recorder) restarted(_ context.Context, runner string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.restarts == nil {
		r.restarts = map[string]int{}
	}
	r.restarts[runner]++
}

func (r *recorder) report(_ context.Context, f supervise.Failure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, f)
	r.announced = append(r.announced, f.Runner+": "+f.Err.Error())
}

func (r *recorder) countOf(runner string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.restarts[runner]
}

func (r *recorder) reported() []supervise.Failure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]supervise.Failure(nil), r.failures...)
}

func (r *recorder) said() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.announced, "\n")
}

func (r *recorder) config(b supervise.Backoff) supervise.Config {
	return supervise.Config{Backoff: b, Restarted: r.restarted, Report: r.report}
}

// eventually waits for cond, failing the test rather than hanging the suite.
func eventually(t *testing.T, cond func() bool, why string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", why)
}

// TestAFailingRunnerIsRestartedAndItsSiblingsAreUntouched is the regression
// test for the incident shape: one runner that fails on every attempt beside
// one that is healthy. Under errgroup.WithContext the first error cancelled
// the second (and the HTTP server with it); here the failing one is started
// again and the healthy one never notices.
func TestAFailingRunnerIsRestartedAndItsSiblingsAreUntouched(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	var attempts atomic.Int32
	var siblingEnded atomic.Bool
	siblingUp := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	sup.Go(ctx, "bad", func(context.Context) error {
		attempts.Add(1)
		return errors.New("cannot decode payload")
	})
	sup.Go(ctx, "good", func(ctx context.Context) error {
		close(siblingUp)
		<-ctx.Done()
		siblingEnded.Store(true)
		return nil
	})

	<-siblingUp
	eventually(t, func() bool { return attempts.Load() >= 3 }, "the failing runner was not started again")
	if siblingEnded.Load() {
		t.Fatal("a sibling's failure cancelled the healthy runner")
	}
	if got := seen.countOf("bad"); got < 2 {
		t.Fatalf("every restart is counted under the runner's name: got %d", got)
	}
	if got := seen.countOf("good"); got != 0 {
		t.Fatalf("a healthy runner has no restarts: got %d", got)
	}

	cancel()
	sup.Wait()
	if !siblingEnded.Load() {
		t.Fatal("the healthy runner did not wind down with its context")
	}
}

// TestCancellingTheContextEndsTheRunnerCleanly pins the other half of the Run
// contract: a cancelled context is shutdown, not a fault to count, report or
// retry.
func TestCancellingTheContextEndsTheRunnerCleanly(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	var runs atomic.Int32
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	sup.Go(ctx, "quiet", func(ctx context.Context) error {
		runs.Add(1)
		close(started)
		<-ctx.Done()
		return nil
	})

	<-started
	cancel()
	sup.Wait()

	if got := runs.Load(); got != 1 {
		t.Fatalf("the runner was started again after shutdown: %d runs", got)
	}
	if got := seen.countOf("quiet"); got != 0 {
		t.Fatalf("shutdown is not a restart: got %d", got)
	}
	if got := seen.reported(); len(got) != 0 {
		t.Fatalf("shutdown is not reported: got %v", got)
	}
}

// TestAnErrorReturnedDuringShutdownIsNotAFailure: a runner that hands back its
// context's error as it winds down has shut down, not failed.
func TestAnErrorReturnedDuringShutdownIsNotAFailure(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup.Run(ctx, "winding-down", func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	})

	if got := seen.countOf("winding-down"); got != 0 {
		t.Fatalf("shutdown counted as a restart: got %d", got)
	}
}

// TestTheSameFailureIsReportedOnceWhileEveryRestartIsCounted is the log's half
// of the bargain: a runner failing every millisecond writes one record, not
// one per failure, and a failure that reads differently is news again. The
// counter is what says it is still happening.
func TestTheSameFailureIsReportedOnceWhileEveryRestartIsCounted(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	sup.Go(ctx, "noisy", func(context.Context) error {
		if n := attempts.Add(1); n > 10 {
			return errors.New("a different failure")
		}
		return errors.New("the same failure")
	})

	eventually(t, func() bool { return attempts.Load() >= 12 }, "the runner was not retried enough times")
	cancel()
	sup.Wait()

	said := seen.said()
	if got := strings.Count(said, "the same failure"); got != 1 {
		t.Fatalf("repeats are counted, not reported: reported %d times\n%s", got, said)
	}
	if got := strings.Count(said, "a different failure"); got != 1 {
		t.Fatalf("a failure that reads differently is news: reported %d times\n%s", got, said)
	}
	if got := strings.Count(said, "noisy: "); got != 2 {
		t.Fatalf("every report carries the runner's name: %s", said)
	}
	if got := seen.countOf("noisy"); got < 10 {
		t.Fatalf("every restart is on the counter: got %d", got)
	}
}

// TestARunThatLastedLongEnoughResetsTheBackoffAndTheReporting pins Healthy:
// a runner that worked for a while and then failed the way it failed last week
// is a new problem — paced from First again, and said out loud again.
func TestARunThatLastedLongEnoughResetsTheBackoffAndTheReporting(t *testing.T) {
	seen := &recorder{}
	// Healthy is tiny and First is large: the second run clears Healthy, so its
	// failure must be reported again AND must wait First rather than 2*First.
	sup := supervise.New(seen.config(supervise.Backoff{
		First:   20 * time.Millisecond,
		Most:    time.Second,
		Healthy: time.Millisecond,
	}))

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.Run(ctx, "flapping", func(context.Context) error {
		if attempts.Add(1) > 1 {
			time.Sleep(5 * time.Millisecond) // long enough to count as working
		}
		return errors.New("the usual failure")
	})

	eventually(t, func() bool { return len(seen.reported()) >= 2 },
		"a failure after a healthy run was swallowed as a repeat")
	cancel()
	sup.Wait()

	failures := seen.reported()
	for _, f := range failures[:2] {
		if f.RetryIn != 20*time.Millisecond {
			t.Fatalf("a healthy run resets the backoff: retried in %s", f.RetryIn)
		}
		if f.Runner != "flapping" || f.Stack != nil {
			t.Fatalf("unexpected failure reported: %+v", f)
		}
	}
}

// TestBackoffDoublesUpToTheCeiling: a runner that can never succeed costs a
// slow pulse, not a hot loop.
func TestBackoffDoublesUpToTheCeiling(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(supervise.Backoff{
		First:   time.Millisecond,
		Most:    4 * time.Millisecond,
		Healthy: time.Hour,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	// Each failure reads differently, so every one of them is reported and its
	// RetryIn is observable.
	sup.Go(ctx, "wedged", func(context.Context) error {
		n := attempts.Add(1)
		return errors.New("failure " + strings.Repeat("x", int(n)))
	})

	eventually(t, func() bool { return len(seen.reported()) >= 6 }, "not enough failures to climb the ceiling")
	cancel()
	sup.Wait()

	want := []time.Duration{1, 2, 4, 4, 4, 4}
	for i, f := range seen.reported()[:6] {
		if f.RetryIn != want[i]*time.Millisecond {
			t.Fatalf("attempt %d waited %s, want %s", i+1, f.RetryIn, want[i]*time.Millisecond)
		}
	}
}

// TestARunnerThatReturnsNilEarlyIsReportedAndRestarted is the fix for the
// quietest defect of the lot. nil means "my context ended"; nil with the
// context still live is a runner that stopped for a reason it did not name,
// and the prototype simply returned, leaving a dead projection inside a
// healthy-looking process. It is a failure like any other now.
func TestARunnerThatReturnsNilEarlyIsReportedAndRestarted(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	sup.Go(ctx, "deserter", func(context.Context) error {
		attempts.Add(1)
		return nil
	})

	eventually(t, func() bool { return attempts.Load() >= 3 }, "a runner that stopped early was never started again")
	cancel()
	sup.Wait()

	if got := seen.countOf("deserter"); got < 2 {
		t.Fatalf("stopping early is a counted restart: got %d", got)
	}
	failures := seen.reported()
	if len(failures) == 0 || !errors.Is(failures[0].Err, supervise.ErrStopped) {
		t.Fatalf("stopping early is reported as ErrStopped: got %v", failures)
	}
}

// TestAPanicIsRecoveredReportedWithItsStackAndCounted: the promise is that a
// runner's failure does not take the service down, and a panic is the most
// common way an application fails. It costs exactly what an error costs, never
// less: counted, reported, and carrying the stack the crashing process would
// have printed.
func TestAPanicIsRecoveredReportedWithItsStackAndCounted(t *testing.T) {
	seen := &recorder{}
	sup := supervise.New(seen.config(fast))

	var attempts atomic.Int32
	var siblingEnded atomic.Bool
	siblingUp := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	sup.Go(ctx, "panicky", func(context.Context) error {
		attempts.Add(1)
		panic("index out of range in a projection handler")
	})
	sup.Go(ctx, "good", func(ctx context.Context) error {
		close(siblingUp)
		<-ctx.Done()
		siblingEnded.Store(true)
		return nil
	})

	<-siblingUp
	eventually(t, func() bool { return attempts.Load() >= 2 }, "a panicking runner was not started again")
	cancel()
	sup.Wait()

	if got := seen.countOf("panicky"); got < 1 {
		t.Fatalf("a panic is a counted restart: got %d", got)
	}
	failures := seen.reported()
	if len(failures) == 0 {
		t.Fatal("a panic must never be swallowed silently")
	}
	f := failures[0]
	if !errors.Is(f.Err, supervise.ErrPanic) {
		t.Fatalf("a panic is reported as ErrPanic: got %v", f.Err)
	}
	if !strings.Contains(f.Err.Error(), "index out of range in a projection handler") {
		t.Fatalf("the panic value is in the error: %v", f.Err)
	}
	if len(f.Stack) == 0 || !strings.Contains(string(f.Stack), "supervise") {
		t.Fatalf("a recovered panic reports its stack: %q", f.Stack)
	}
	if !siblingEnded.Load() {
		t.Fatal("the sibling of a panicking runner must survive it")
	}
}

// TestUnsetConfigFieldsFallBackToTheDefaults: a Supervisor built from the zero
// Config is usable — defaults for the pacing, no-ops for the callbacks — so a
// service can adopt it before it has decided where failures are reported.
func TestUnsetConfigFieldsFallBackToTheDefaults(t *testing.T) {
	sup := supervise.New(supervise.Config{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx, "nameless", func(ctx context.Context) error {
			cancel()
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}
