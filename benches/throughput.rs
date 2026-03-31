use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use s3dashmap::S3DashMap;
use std::sync::Arc;
use std::thread;

const N: usize = 100_000;

// ── Single-threaded ───────────────────────────────────────────────────────────

fn bench_insert(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert");
    g.throughput(Throughput::Elements(N as u64));

    g.bench_function("bounded", |b| {
        b.iter(|| {
            let map: S3DashMap<u64, u64> = S3DashMap::new(N);
            for i in 0..N as u64 {
                map.insert(i, i);
            }
        })
    });

    g.bench_function("unbounded", |b| {
        b.iter(|| {
            let map: S3DashMap<u64, u64> = S3DashMap::new_unbounded();
            for i in 0..N as u64 {
                map.insert(i, i);
            }
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let map: S3DashMap<u64, u64> = S3DashMap::new_unbounded();
    for i in 0..N as u64 {
        map.insert(i, i);
    }

    let mut g = c.benchmark_group("get");
    g.throughput(Throughput::Elements(N as u64));

    g.bench_function("hit", |b| {
        b.iter(|| {
            for i in 0..N as u64 {
                let _ = map.get(&i);
            }
        })
    });

    g.bench_function("miss", |b| {
        b.iter(|| {
            for i in N as u64..(2 * N) as u64 {
                let _ = map.get(&i);
            }
        })
    });
}

fn bench_remove(c: &mut Criterion) {
    let mut g = c.benchmark_group("remove");
    g.throughput(Throughput::Elements(N as u64));

    g.bench_function("sequential", |b| {
        b.iter(|| {
            let map: S3DashMap<u64, u64> = S3DashMap::new_unbounded();
            for i in 0..N as u64 {
                map.insert(i, i);
            }
            for i in 0..N as u64 {
                map.remove(&i);
            }
        })
    });
}

// ── Multi-threaded ────────────────────────────────────────────────────────────

fn bench_concurrent(c: &mut Criterion) {
    let thread_counts = [2usize, 4, 8];

    let mut g = c.benchmark_group("concurrent_insert");

    for &threads in &thread_counts {
        let per_thread = N / threads;
        g.throughput(Throughput::Elements((per_thread * threads) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, &t| {
            b.iter(|| {
                let map: Arc<S3DashMap<u64, u64>> = Arc::new(S3DashMap::new(N));
                let handles: Vec<_> = (0..t)
                    .map(|tid| {
                        let m = Arc::clone(&map);
                        let start = (tid * per_thread) as u64;
                        thread::spawn(move || {
                            for i in start..start + per_thread as u64 {
                                m.insert(i, i);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            })
        });
    }
}

fn bench_concurrent_read(c: &mut Criterion) {
    let map: Arc<S3DashMap<u64, u64>> = Arc::new(S3DashMap::new_unbounded());
    for i in 0..N as u64 {
        map.insert(i, i);
    }

    let thread_counts = [2usize, 4, 8];
    let mut g = c.benchmark_group("concurrent_get");

    for &threads in &thread_counts {
        let per_thread = N / threads;
        g.throughput(Throughput::Elements((per_thread * threads) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(threads), &threads, |b, &t| {
            b.iter(|| {
                let handles: Vec<_> = (0..t)
                    .map(|_| {
                        let m = Arc::clone(&map);
                        thread::spawn(move || {
                            for i in 0..per_thread as u64 {
                                let _ = m.get(&i);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            })
        });
    }
}

criterion_group!(
    benches,
    bench_insert,
    bench_get,
    bench_remove,
    bench_concurrent,
    bench_concurrent_read
);
criterion_main!(benches);
