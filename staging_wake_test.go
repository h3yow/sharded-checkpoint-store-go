package checkpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitForQueued 轮询等待预算内部排队者数量达到 n，避免依赖固定 sleep 造成竞态。
func waitForQueued(t *testing.T, b *stagingBudget, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		got := len(b.waiters)
		b.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.mu.Lock()
	got := len(b.waiters)
	b.mu.Unlock()
	t.Fatalf("排队者数量=%d, 期望 %d", got, n)
}

// 一次释放出的空间足够多个排队者时，所有放得下的等待者都应被同一次释放放行，
// 不应只唤醒一个、其余挂死等下一次释放。
func TestStagingBudgetOneReleaseWakesMultipleWaiters(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	ctx := context.Background()
	if err := b.acquire(ctx, 10); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	const n = 3
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = b.acquire(ctx, 3)
		}(i)
	}
	close(start)
	waitForQueued(t, b, n)

	// 单次释放 9 字节空闲：三个 3 字节等待者都应拿到。
	b.release(9)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("一次释放后仍有等待者被挂住（唤醒丢失/只放行一个）")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("waiter %d err=%v, 期望成功", i, err)
		}
	}
	// 持额者保留 1，三个等待者共 9 -> used=10。
	b.mu.Lock()
	used, queued := b.used, len(b.waiters)
	b.mu.Unlock()
	if used != 10 || queued != 0 {
		t.Fatalf("used=%d queued=%d, 期望 used=10 queued=0", used, queued)
	}
	t.Logf("判定依据: 单次 release(9) 同时放行三个各需 3 字节的排队等待者, used=10")
	b.release(9)
}

// 单个等待者场景：释放出足够空间后被正常唤醒，不回归原语义。
func TestStagingBudgetSingleWaiterWoken(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	ctx := context.Background()
	if err := b.acquire(ctx, 10); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- b.acquire(ctx, 4) }()
	waitForQueued(t, b, 1)
	b.release(4)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("single waiter err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("单个等待者未被唤醒")
	}
	b.mu.Lock()
	used := b.used
	b.mu.Unlock()
	if used != 10 {
		t.Fatalf("used=%d, 期望 10", used)
	}
	b.release(4)
}

// 等待中取消：等待者尽快退出返回 ctx.Err()，从队列摘除且不占配额；
// 与其他等待者并存时，取消一个不影响其余等待者被后续释放唤醒。
func TestStagingBudgetWaitCancel(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	ctx := context.Background()
	if err := b.acquire(ctx, 10); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	canceledErr := make(chan error, 1)
	go func() { canceledErr <- b.acquire(cctx, 5) }()
	waitForQueued(t, b, 1)

	otherErr := make(chan error, 1)
	go func() { otherErr <- b.acquire(ctx, 6) }()
	waitForQueued(t, b, 2)

	cancel()
	select {
	case err := <-canceledErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("取消等待 err=%v, 期望 context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("取消后等待者未尽快退出")
	}
	waitForQueued(t, b, 1)

	// 释放 6 字节：仍在排队的 6 字节等待者应被唤醒，被取消者没有占位。
	b.release(6)
	select {
	case err := <-otherErr:
		if err != nil {
			t.Fatalf("other waiter err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("未取消的等待者未被唤醒（取消路径可能污染了队列）")
	}
	b.mu.Lock()
	used, queued := b.used, len(b.waiters)
	b.mu.Unlock()
	if used != 10 || queued != 0 {
		t.Fatalf("used=%d queued=%d, 期望 10/0（取消不得泄漏配额）", used, queued)
	}
	b.release(6)
}

// 放不下的等待者必须继续等待，且不能挡住它后面放得下的等待者（不超发、不饿死）。
func TestStagingBudgetWaiterThatDoesNotFitKeepsWaiting(t *testing.T) {
	b := newStagingBudget(10, StagingWait)
	ctx := context.Background()
	if err := b.acquire(ctx, 10); err != nil {
		t.Fatal(err)
	}

	aErr := make(chan error, 1) // 需 6 字节
	go func() { aErr <- b.acquire(ctx, 6) }()
	waitForQueued(t, b, 1)

	bErr := make(chan error, 1) // 需 4 字节
	go func() { bErr <- b.acquire(ctx, 4) }()
	waitForQueued(t, b, 2)

	// 只释放 4 字节：A(6) 放不下必须继续等待；B(4) 放得下应越过 A 被放行。
	b.release(4)
	select {
	case err := <-bErr:
		if err != nil {
			t.Fatalf("B err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("放得下的 B 未被放行（被队首 A 挡住）")
	}
	select {
	case err := <-aErr:
		t.Fatalf("放不下的 A 不应被放行, err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	b.mu.Lock()
	used, queued := b.used, len(b.waiters)
	b.mu.Unlock()
	if used != 10 || queued != 1 {
		t.Fatalf("used=%d queued=%d, 期望 10/1（A 继续等待、容量未超发）", used, queued)
	}

	// B 归还 4 仍放不下 A（剩 4 < 6），A 继续等；持额者再归还 6 后 A 才放行。
	b.release(4)
	select {
	case err := <-aErr:
		t.Fatalf("空位仍不足时 A 不应被放行, err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	b.release(6)
	select {
	case err := <-aErr:
		if err != nil {
			t.Fatalf("A err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("空位足够后 A 未被唤醒（饿死）")
	}
	b.release(6)
}

// Store 级：配额被一个事务占满时，多个排队事务应在占额事务一次提交释放后
// 全部被放行，随后都能提交并被完整恢复（复现批量并发写入卡死）。
func TestStoreMultipleWaitersReleasedTogether(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 10,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    8,
	})
	// v1 占满 10 字节。
	holder, err := s.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.WriteShard(context.Background(), 0, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}

	versions := []uint64{2, 3, 4}
	writers := make(map[uint64]*Writer, len(versions))
	var wg sync.WaitGroup
	writeErrs := make(chan error, len(versions))
	for _, v := range versions {
		w, err := s.Begin(context.Background(), v)
		if err != nil {
			t.Fatalf("begin v%d: %v", v, err)
		}
		writers[v] = w
		wg.Add(1)
		go func(w *Writer, v uint64) {
			defer wg.Done()
			// 每个事务暂存恰好 3 字节，3 * 3 = 9 <= 释放出的 10。
			writeErrs <- w.WriteShard(context.Background(), 0, []byte{byte('0' + v), 'x', 'x'})
		}(w, v)
	}
	waitForQueued(t, s.budget, len(versions))

	// 一次提交释放全部 10 字节：三个各需 3 字节的排队事务都应继续推进。
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("一次释放后仍有排队事务挂住（只有一个被放行）")
	}
	close(writeErrs)
	for err := range writeErrs {
		if err != nil {
			t.Fatalf("排队事务写入失败: %v", err)
		}
	}
	for _, v := range versions {
		if err := writers[v].Commit(); err != nil {
			t.Fatalf("commit v%d: %v", v, err)
		}
		data, rep, err := s.Recover(v)
		if err != nil || !rep.OK() || len(data[0]) != 3 {
			t.Fatalf("recover v%d: data=%v rep=%+v err=%v", v, data, rep, err)
		}
	}
	if latest, _ := s.Latest(); latest != 4 {
		t.Fatalf("latest=%d, 期望 4", latest)
	}
	t.Logf("判定依据: v1 一次提交释放 10 字节 -> v2/v3/v4 三个排队事务同时放行并提交")
}

// Store 级：多个等待者中部分在等待中被取消，必须尽快退出；Abort 清理目录，
// 未 Abort 的也只留下无清单目录（绝不可被恢复为有效版本）；其余等待者不受影响。
func TestStoreMultipleWaitersCancelNoResidue(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 10,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    8,
	})
	holder, err := s.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.WriteShard(context.Background(), 0, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}

	beginWaiter := func(v uint64) (*Writer, context.CancelFunc, chan error) {
		w, err := s.Begin(context.Background(), v)
		if err != nil {
			t.Fatalf("begin v%d: %v", v, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- w.WriteShard(ctx, 0, []byte("bbbbb")) }()
		return w, cancel, errCh
	}

	wb, cancelB, errB := beginWaiter(2)
	wc, _, errC := beginWaiter(3)
	wd, cancelD, errD := beginWaiter(4)
	_ = wd // 故意不 Abort：验证只剩无清单目录时也绝不会被当作有效版本
	waitForQueued(t, s.budget, 3)

	cancelB()
	cancelD()
	for name, ch := range map[string]chan error{"v2": errB, "v4": errD} {
		select {
		case err := <-ch:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s 取消后 err=%v, 期望 context.Canceled", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s 取消后未尽快退出", name)
		}
	}
	waitForQueued(t, s.budget, 1) // 只剩 v3

	// v2 走 Abort：目录被删除；v4 不 Abort：只剩无清单目录，仍不是有效版本。
	if err := wb.Abort(); err != nil {
		t.Fatalf("abort v2: %v", err)
	}

	// 持额者一次释放：v3 必须照常被唤醒；被取消的 v2/v4 没有占位。
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errC:
		if err != nil {
			t.Fatalf("v3 写入: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("未取消的 v3 未被唤醒")
	}
	if err := wc.Commit(); err != nil {
		t.Fatalf("commit v3: %v", err)
	}

	if _, _, err := s.Recover(2); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("recover v2 err=%v, 期望 ErrNoCheckpoint（Abort 后无残留）", err)
	}
	if _, _, err := s.Recover(4); !errors.Is(err, ErrIncompleteCommit) {
		t.Fatalf("recover v4 err=%v, 期望 ErrIncompleteCommit（无清单，绝不可当有效版本）", err)
	}
	if latest, _ := s.Latest(); latest != 3 {
		t.Fatalf("latest=%d, 期望 3（取消事务不得成为版本）", latest)
	}
	s.budget.mu.Lock()
	used := s.budget.used
	s.budget.mu.Unlock()
	if used != 0 {
		t.Fatalf("最终 used=%d, 期望 0（取消路径不得泄漏配额）", used)
	}
	t.Logf("判定依据: v2/v4 等待中取消尽快退出且无有效版本残留, v3 不受影响正常提交")
}

// Store 级：部分释放只放得下后面的小等待者时，小等待者越过队首放行，
// 放不下的队首继续等待，随后更大的释放到来再放行（不超发、不饿死）。
func TestStoreDoesNotFitKeepsWaitingStoreLevel(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 10,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    8,
	})
	// v1 持 4、v2 持 6，合计占满；按版本递增顺序提交，故先释放 4 后释放 6。
	w1, err := s.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.WriteShard(context.Background(), 0, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	w2, err := s.Begin(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.WriteShard(context.Background(), 0, []byte("222222")); err != nil {
		t.Fatal(err)
	}

	// v3 需 6（先排队，位于队首），v4 需 4（排在后面）。
	w3, err := s.Begin(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	err3 := make(chan error, 1)
	go func() { err3 <- w3.WriteShard(context.Background(), 0, []byte("333333")) }()
	waitForQueued(t, s.budget, 1)

	w4, err := s.Begin(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	err4 := make(chan error, 1)
	go func() { err4 <- w4.WriteShard(context.Background(), 0, []byte("4444")) }()
	waitForQueued(t, s.budget, 2)

	// v1 先提交只释放 4：v4(4) 越过 v3(6) 放行，v3 必须继续等待。
	if err := w1.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-err4:
		if err != nil {
			t.Fatalf("v4 写入: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("放得下的 v4 未越过队首被放行")
	}
	select {
	case err := <-err3:
		t.Fatalf("放不下的 v3 不应被放行, err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}
	s.budget.mu.Lock()
	used, queued := s.budget.used, len(s.budget.waiters)
	s.budget.mu.Unlock()
	if used != 10 || queued != 1 {
		t.Fatalf("used=%d queued=%d, 期望 10/1（容量未超发, v3 继续等待）", used, queued)
	}

	// v2 提交再释放 6：v3 此时放得下，必须被唤醒，不得饿死。
	if err := w2.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-err3:
		if err != nil {
			t.Fatalf("v3 写入: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("空位足够后 v3 仍未被唤醒（饿死）")
	}
	if err := w3.Commit(); err != nil {
		t.Fatalf("commit v3: %v", err)
	}
	if err := w4.Commit(); err != nil {
		t.Fatalf("commit v4: %v", err)
	}
	for _, v := range []uint64{3, 4} {
		if data, rep, err := s.Recover(v); err != nil || !rep.OK() || len(data[0]) == 0 {
			t.Fatalf("recover v%d: data=%v rep=%+v err=%v", v, data, rep, err)
		}
	}
	t.Logf("判定依据: 释放 4 只放行 v4, v3 继续等待; 再释放 6 后 v3 放行, 不超发不饿死")
}

// 竞态压力：占额者一次释放后大批等待者同批唤醒，其中夹杂被取消的等待者；
// 所有成功事务最终可提交恢复，配额最终归零，配额计数与队列在 -race 下保持一致。
func TestStagingWaitMassWakeupRace(t *testing.T) {
	s := mustOpen(t, &Config{
		MaxStagedBytes: 128,
		Staging:        StagingWait,
		MaxShardBytes:  1 << 20,
		MaxInflight:    32,
	})
	holder, err := s.Begin(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.WriteShard(context.Background(), 0, make([]byte, 128)); err != nil {
		t.Fatal(err)
	}

	const waiters = 12
	const canceledN = 4
	const total = waiters + canceledN
	var wg sync.WaitGroup
	var mu sync.Mutex
	okWriters := make([]*Writer, 0, waiters)
	cancels := make([]context.CancelFunc, 0, canceledN)
	cancelErrs := make(chan error, canceledN)
	for v := uint64(2); v <= total+1; v++ {
		w, err := s.Begin(context.Background(), v)
		if err != nil {
			t.Fatalf("begin v%d: %v", v, err)
		}
		if int(v)-2 < waiters {
			wg.Add(1)
			go func(w *Writer) {
				defer wg.Done()
				if err := w.WriteShard(context.Background(), 0, make([]byte, 8)); err == nil {
					mu.Lock()
					okWriters = append(okWriters, w)
					mu.Unlock()
				}
			}(w)
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)
			go func(w *Writer, ctx context.Context) {
				cancelErrs <- w.WriteShard(ctx, 0, make([]byte, 8))
			}(w, ctx)
		}
	}
	waitForQueued(t, s.budget, total) // 全部 16 个排队者就位

	// 确定性取消后 4 个等待者：在释放前全部摘除，其余 12 个继续等待。
	for _, cancel := range cancels {
		cancel()
	}
	for i := 0; i < canceledN; i++ {
		select {
		case err := <-cancelErrs:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("取消等待 err=%v, 期望 context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("取消后等待者未尽快退出")
		}
	}
	waitForQueued(t, s.budget, waiters)

	// 一次释放 128：12 个等待者全部放得下（96 字节），同批唤醒。
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	mu.Lock()
	got := len(okWriters)
	mu.Unlock()
	if got != waiters {
		t.Fatalf("同批唤醒成功者=%d, 期望 %d", got, waiters)
	}
	// 成功写入的事务乱序并发提交：与既有版本准入语义一致——
	// 高版本先提交时低版本会被 Commit 时复核拒绝，结果必须与存储状态吻合。
	var cwg sync.WaitGroup
	commitResults := make(map[uint64]error, waiters)
	var cmu sync.Mutex
	for _, w := range okWriters {
		cwg.Add(1)
		go func(w *Writer) {
			defer cwg.Done()
			err := w.Commit()
			cmu.Lock()
			commitResults[w.version] = err
			cmu.Unlock()
		}(w)
	}
	cwg.Wait()
	for v := uint64(2); v <= waiters+1; v++ {
		cerr := commitResults[v]
		_, _, rerr := s.Recover(v)
		if cerr == nil {
			if rerr != nil {
				t.Fatalf("v%d 提交成功但恢复失败: %v", v, rerr)
			}
		} else if !errors.Is(cerr, ErrVersionRegression) {
			t.Fatalf("v%d commit err=%v, 期望 nil 或 ErrVersionRegression", v, cerr)
		} else if rerr == nil {
			t.Fatalf("v%d 提交被拒但可恢复（提交结果与存储状态不一致）", v)
		}
	}
	s.budget.mu.Lock()
	used, queued := s.budget.used, len(s.budget.waiters)
	s.budget.mu.Unlock()
	if used != 0 || queued != 0 {
		t.Fatalf("最终 used=%d queued=%d, 期望 0/0", used, queued)
	}
}
