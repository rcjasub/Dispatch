package gqueue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	name := fmt.Sprintf("rel-%s-%d", t.Name(), time.Now().UnixNano())
	q := NewQueue(testClient, []string{name + ":high", name + ":mid", name + ":low"}, name)
	t.Cleanup(func() {
		testClient.Del(context.Background(), append(q.Names,
			"active-jobs:"+name, "poison:"+name, "queue:"+name+":paused", "delayed:"+name)...)
	})
	return q
}

// The core guarantee: there is no moment where a taken job is untracked.
// As soon as Dequeue returns, the job is already in active-jobs and marked active.
func TestClaimRecordsJobAtomically(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	job := NewJob(q.Name+"-1", "task", "payload")
	defer testClient.Del(ctx, "job:"+job.Id)

	q.Enqueue(ctx, job)
	got, ok := q.Dequeue(ctx)
	if !ok || got.Id != job.Id {
		t.Fatalf("expected to claim %q, got %q (ok=%v)", job.Id, got.Id, ok)
	}
	if _, err := testClient.ZScore(ctx, "active-jobs:"+q.Name, job.Id).Result(); err != nil {
		t.Errorf("claimed job missing from active-jobs: %v", err)
	}
	if s := testClient.HGet(ctx, "job:"+job.Id, "status").Val(); s != "active" {
		t.Errorf("expected status active, got %q", s)
	}
}

// ack: completed + gone from active-jobs, so the stalled detector won't re-run it.
func TestAckClearsActive(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	job := NewJob(q.Name+"-1", "task", "payload")
	defer testClient.Del(ctx, "job:"+job.Id)

	w := NewWorker(q, 1, nil)
	w.Register("task", func(Job, func(int)) (string, error) { return "ok", nil })

	q.Enqueue(ctx, job)
	got, _ := q.Dequeue(ctx)
	w.processJob(ctx, got)

	if n := testClient.ZCard(ctx, "active-jobs:"+q.Name).Val(); n != 0 {
		t.Errorf("expected active-jobs empty after success, got %d", n)
	}
	if s := testClient.HGet(ctx, "job:"+job.Id, "status").Val(); s != "completed" {
		t.Errorf("expected status completed, got %q", s)
	}
}

// Pause is checked inside the claim script: nothing is taken while paused.
func TestClaimRespectsPause(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	job := NewJob(q.Name+"-1", "task", "payload")
	defer testClient.Del(ctx, "job:"+job.Id)

	q.Enqueue(ctx, job)
	q.Pause(ctx)

	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if got, ok := q.Dequeue(short); ok {
		t.Fatalf("claimed %q from a paused queue", got.Id)
	}
	if n := testClient.LLen(ctx, q.Names[PriorityMid]).Val(); n != 1 {
		t.Errorf("job should still be waiting in its lane, lane has %d", n)
	}
}

// A job that is not valid JSON must not vanish: in Lua, writes made before an
// error are not rolled back, so the script parks it instead of erroring after RPOP.
func TestClaimParksUnreadableJob(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	testClient.LPush(ctx, q.Names[PriorityHigh], "not json")

	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, ok := q.Dequeue(short); ok {
		t.Fatal("an unreadable job should not be returned")
	}
	if got := testClient.LRange(ctx, "poison:"+q.Name, 0, -1).Val(); len(got) != 1 || got[0] != "not json" {
		t.Errorf("expected the raw job in poison list, got %v", got)
	}
}
