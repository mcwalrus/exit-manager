package exitmanager

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newTestExitManager returns a hijacked test ExitManager.
// This avoids the case where the ExitManager will call os.Exit(code) during tests.
// You can also record the exit manager exit code with the registed exitHandlerRecorder set.
func testExitManager(t *testing.T) *ExitManager {
	t.Helper()

	em := newExitManager()
	go em.listenForSignals()
	em.hijackExitHandler()

	t.Cleanup(func() {
		em.Shutdown()
	})

	return em
}

func (em *ExitManager) hijackExitHandler() {
	em.exit = &exitRecorder{
		mu:          &sync.Mutex{},
		hasExitedCh: make(chan struct{}),
	}
}

// exitRecorder implements the exitHandler interface.
type exitRecorder struct {
	mu          *sync.Mutex
	code        int
	nExits      int
	hasExited   bool
	waited      bool
	hasExitedCh chan (struct{})
}

func (er *exitRecorder) Exit(code int) {
	er.mu.Lock()
	er.nExits++
	er.code = code
	er.hasExited = true
	close(er.hasExitedCh)
	er.mu.Unlock()
}

func (er *exitRecorder) Wait() {
	<-er.hasExitedCh
	er.mu.Lock()
	er.waited = true
	er.mu.Unlock()
}

// faultyListener implements net.Listener with forced failure
type faultyListener struct {
	net.Listener
	errOnClose error
}

func (fl *faultyListener) Close() error {
	_ = fl.Listener.Close()
	return fl.errOnClose
}

// checkManagerExitCode tests for the expected exit code from a hijacked exit manager.
func checkManagerExitCode(t *testing.T, em *ExitManager, code int) {
	t.Helper()

	er, ok := (em.exit).(*exitRecorder)
	if !ok {
		t.Fatalf("required em.hijackExitHandler() for test")
	}

	if !er.hasExited {
		t.Errorf("exit manager has not been recorded to exit yet...")
		t.FailNow()
	}

	if er.code != code {
		t.Errorf("exit manager returned different exit code: %d != %d (recorded, expected)", er.code, code)
		t.FailNow()
	}
}

func TestNotify(t *testing.T) {
	t.Parallel()

	t.Run("wait for Shutdown()", func(t *testing.T) {
		em := testExitManager(t)

		select {
		case <-em.Notify():
			t.Fatalf("needed to wait for Shutdown() by exit handler")
		case <-time.After(100 * time.Millisecond):
		}

		em.Shutdown()
	})

	t.Run("listen after Shutdown()", func(t *testing.T) {
		em := testExitManager(t)

		em.Shutdown()
		select {
		case <-em.Notify():
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Notify() channel was not closed after Shutdown()")
		}

		checkManagerExitCode(t, em, 0)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		em := testExitManager(t)

		wg := &sync.WaitGroup{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				select {
				case <-em.Notify():
				case <-ctx.Done():
					time.Sleep(10 * time.Millisecond)
				}
				wg.Done()
			}()
		}

		em.Shutdown()
		wg.Wait()

		select {
		case <-ctx.Done():
			t.Errorf("context cancelled before all routinues were notified")
		default:
		}

		checkManagerExitCode(t, em, 0)
	})
}

func TestWithCancel(t *testing.T) {
	t.Parallel()

	t.Run("context cancellation waits for notified shutdown", func(t *testing.T) {
		em := testExitManager(t)
		ctx, cancel := em.WithCancel(context.Background())
		t.Cleanup(cancel)

		select {
		case <-ctx.Done():
			t.Fatal("context should not be cancelled before shutdown")
		case <-time.After(10 * time.Millisecond):
		}

		em.Shutdown()

		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("context was not cancelled after shutdown")
		}
		checkManagerExitCode(t, em, 0)
	})

	t.Run("context is returned cancelled on notified exit manager", func(t *testing.T) {
		em := testExitManager(t)
		em.Shutdown()
		ctx, cancel := em.WithCancel(context.Background())
		t.Cleanup(cancel)

		select {
		case <-ctx.Done():
			// expected
		case <-time.After(10 * time.Millisecond):
			t.Fatal("context should be immediately cancelled if shutdown already occurred")
		}
		checkManagerExitCode(t, em, 0)
	})

	t.Run("mutliple contexts cancelled", func(t *testing.T) {
		em := testExitManager(t)
		ctx1, cancel1 := em.WithCancel(context.Background())
		ctx2, cancel2 := em.WithCancel(context.Background())
		t.Cleanup(cancel1)
		t.Cleanup(cancel2)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			select {
			case <-ctx1.Done():
			case <-time.After(100 * time.Millisecond):
				t.Error("ctx1 was not cancelled after shutdown")
			}
		}()
		go func() {
			defer wg.Done()
			select {
			case <-ctx2.Done():
			case <-time.After(100 * time.Millisecond):
				t.Error("ctx2 was not cancelled after shutdown")
			}
		}()

		em.Shutdown()
		wg.Wait()
		checkManagerExitCode(t, em, 0)
	})
}

func isServerAlive(server *httptest.Server) bool {
	resp, err := http.Get(server.URL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func makeRequest(t *testing.T, url string, expectStatus int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("failed to make request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expectStatus {
		t.Errorf("unexpected status: got %d, want %d", resp.StatusCode, expectStatus)
	}
	_, _ = io.ReadAll(resp.Body)
}
