use criterion::{criterion_group, criterion_main, Criterion};
use tempfile;
use rand::{self, RngCore, SeedableRng};

use rust_kvs_server::{storage, KVStorage};


pub fn criterion_benchmark(c: &mut Criterion) {
    let mut generator = rand::rngs::StdRng::seed_from_u64(123);
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut store = storage::KvLogStorage::open(temp_dir.path()).unwrap();

    c.bench_function(
        "kvs set 1",
        |b| b.iter(
            || {
                let key = "key".to_string();
                let value = generator.next_u64().to_string();
                store.set(key, value).unwrap();
            }
        )
    );

    c.bench_function(
        "kvs get 1",
        |b| b.iter(
            || {
                let key = "key".to_string();
                let val_opt = store.get(key).unwrap();
                assert!(val_opt.is_some());
            }
        )
    );

    c.bench_function(
        "kvs set 100",
        |b| b.iter(
            || {
                for idx in 1..100 {
                    let key = idx.to_string();
                    let value = generator.next_u64().to_string();
                    store.set(key, value).unwrap();
                }
            }
        )
    );

    c.bench_function(
        "kvs get 100",
        |b| b.iter(
            || {
                for idx in 1..100 {
                    let key = idx.to_string();
                    let val_opt = store.get(key).unwrap();
                    assert!(val_opt.is_some());
                }
            }
        )
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
