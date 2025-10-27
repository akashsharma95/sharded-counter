package s3counter

import (
	"context"
	"log"
	"time"
)

// Compactor periodically runs Compact for a counter using optional heuristics.
type Compactor struct {
	Counter        *Counter
	Name           string
	Interval       time.Duration // run at least this often (>=0). 0 disables ticker.
	DeltaThreshold int64         // run when estimated current-epoch delta >= threshold; 0 disables.
	SampleK        int           // shards to sample for delta estimation

	logger    *log.Logger
	triggerCh chan struct{}
	stopCh    chan struct{}
}

// NewCompactor builds a background compactor using the provided configuration.
func NewCompactor(counter *Counter, name string, interval time.Duration, deltaThreshold int64, sampleK int, logger *log.Logger) *Compactor {
	if sampleK <= 0 {
		sampleK = 4
	}
	return &Compactor{
		Counter:        counter,
		Name:           name,
		Interval:       interval,
		DeltaThreshold: deltaThreshold,
		SampleK:        sampleK,
		logger:         logger,
		triggerCh:      make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}
}

// Start launches the compactor in a goroutine. Call Stop to terminate.
func (r *Compactor) Start(ctx context.Context) { go r.loop(ctx) }

// Trigger asks the compactor to attempt a run soon.
func (r *Compactor) Trigger() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

// Stop stops the background goroutine.
func (r *Compactor) Stop() { close(r.stopCh) }

func (r *Compactor) loop(ctx context.Context) {
	var ticker *time.Ticker
	if r.Interval > 0 {
		ticker = time.NewTicker(r.Interval)
		defer ticker.Stop()
	}
	for {
		select {
		case <-r.stopCh:
			return
		case <-ctx.Done():
			return
		case <-r.triggerCh:
			r.maybeCompact(ctx)
		case <-r.nextTick(ticker):
			r.maybeCompact(ctx)
		}
	}
}

func (r *Compactor) maybeCompact(ctx context.Context) {
	if r.DeltaThreshold <= 0 {
		r.runOnce(ctx)
		return
	}

	meta, _, err := r.Counter.getEpoch(ctx, r.Name)
	if err != nil {
		r.logf("getEpoch: %v", err)
		return
	}

	N := max(1, meta.ShardCount)
	k := min(r.SampleK, N)
	ids := r.Counter.sampleDistinct(N, k)

	partial, err := r.Counter.sumShardIDs(ctx, r.Name, meta.Epoch, ids)
	if err != nil {
		r.logf("sumShardIDs: %v", err)
		return
	}

	est := int64(float64(partial) * float64(N) / float64(k))
	if est >= r.DeltaThreshold {
		r.runOnce(ctx)
	}
}

func (r *Compactor) runOnce(ctx context.Context) {
	folded, newEpoch, err := r.Counter.Compact(ctx, r.Name)
	if err != nil {
		r.logf("compact: %v", err)
		return
	}
	r.logf("compact: folded=%d newEpoch=%d", folded, newEpoch)
}

func (r *Compactor) logf(format string, args ...any) {
	if r.logger != nil {
		r.logger.Printf(format, args...)
	}
}

func (r *Compactor) nextTick(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return make(chan time.Time)
	}
	return t.C
}
