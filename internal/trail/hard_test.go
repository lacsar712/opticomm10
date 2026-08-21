package trail

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandleDispatchHonorsCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(2 * time.Second):
			w.WriteHeader(204)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- HandleDispatch(ctx, srv.URL, []byte("x"))
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancel error")
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("HandleDispatch did not return after cancel")
	}
}

func TestWrapRetryableClassifies(t *testing.T) {
	err := WrapRetryable("outbound", errors.New("boom"))
	if !IsRetryable(err) {
		t.Fatalf("expected retryable, got %v", err)
	}
	if ClassifyOutcome(err) != "retry" {
		t.Fatalf("classify=%s", ClassifyOutcome(err))
	}
}

func TestRouteRegistryConcurrentPutGet(t *testing.T) {
	r := NewRouteRegistry()
	done := make(chan struct{})
	go func() {
		r.Put("a", "1")
		r.Put("b", "2")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("RouteRegistry.Put deadlocks on successive puts (missing unlock)")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmtKey(i)
			r.Put(k, k)
			if _, ok := r.Get(k); !ok {
				t.Errorf("missing %s", k)
			}
		}(i)
	}
	wg.Wait()
	if r.Len() < 32 {
		t.Fatalf("len=%d", r.Len())
	}
}

func fmtKey(i int) string { return "k-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [16]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}

func TestBreakerSuccessClearsStreak(t *testing.T) {
	b := NewTripBreaker(2)
	b.Fail()
	RecordOutcome(b, nil)
	b.Fail()
	if b.Open() {
		t.Fatal("success should clear streak so one later fail does not open")
	}
}

func TestNonceRejectsReplay(t *testing.T) {
	book := NewNonceBook()
	if !book.CheckAndRemember("tok-1") {
		t.Fatal("first should accept")
	}
	if book.CheckAndRemember("tok-1") {
		t.Fatal("replay must be rejected")
	}
}

func TestApplyValidatedBlocksBadCommit(t *testing.T) {
	s := &MemStore{}
	err := ApplyValidated(BodyValidator{}, s, Item{ID: "a", Body: ""})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if s.Last() != "" {
		t.Fatalf("bad item was committed: %q", s.Last())
	}
}

func TestTaskPoolRunsAll(t *testing.T) {
	var n atomic.Int64
	jobs := make([]func(), 0, 8)
	for i := 0; i < 8; i++ {
		jobs = append(jobs, func() { time.Sleep(25 * time.Millisecond); n.Add(1) })
	}
	(TaskPool{}).Run(jobs)
	if n.Load() != 8 {
		t.Fatalf("got %d", n.Load())
	}
}

func TestWatchHoldHonorsCancelNoFire(t *testing.T) {
	var fired atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- WatchHold(ctx, 500*time.Millisecond, &fired)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("WatchHold hung after cancel")
	}
	time.Sleep(600 * time.Millisecond)
	if fired.Load() {
		t.Fatal("late timer must not fire shared state after cancel")
	}
}

func TestCloneSessionNoMutexCopy(t *testing.T) {
	s := NewLiveSession("a")
	s.Inc()
	c := CloneSession(s)
	if c.Value() != 1 {
		t.Fatalf("clone lost counter: got %d", c.Value())
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); c.Inc() }()
		go func() { defer wg.Done(); _ = c.Value() }()
	}
	wg.Wait()
	if c.Value() < 1 {
		t.Fatal("clone should remain usable under concurrency")
	}
}

func TestFanoutMarkNotDoneOnError(t *testing.T) {
	done, err := FanoutMark([]string{"a", "b", "c"}, func(target string) error {
		if target == "b" {
			return errors.New("boom")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if done {
		t.Fatal("batch must not be marked done when a target fails")
	}
}
