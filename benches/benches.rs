#[macro_use]
extern crate criterion;
use criterion::{BatchSize, Criterion, ParameterizedBenchmark};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::iter;
use tempfile::TempDir;

const NUM_SET_OP_PER_ITER: u16 = 100;
const NUM_GET_OP_PER_ITER: u16 = 1000;
const MAX_STR_LEN: usize = 100_000;

fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "sled",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().expect("unable to create temp dir");
                    let store = SledKvsEngine::open(temp_dir.path()).unwrap();
                    let mut rng: StdRng = SeedableRng::from_seed([1u8; 32]);
                    let mut dataset = Vec::new();
                    for _ in 0..NUM_SET_OP_PER_ITER {
                        dataset.push(gen_rand_string_pair(rng.gen()));
                    }
                    (store, dataset)
                },
                |(store, dataset)| {
                    for (key, value) in dataset {
                        store.set(key, value).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("kvs", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("unable to create temp dir");
                let store = KvStore::open(temp_dir.path()).unwrap();
                let mut rng: StdRng = SeedableRng::from_seed([1u8; 32]);
                let mut dataset = Vec::new();
                for _ in 0..NUM_SET_OP_PER_ITER {
                    dataset.push(gen_rand_string_pair(rng.gen()));
                }
                (store, dataset, temp_dir)
            },
            |(store, dataset, _temp_dir)| {
                for (key, value) in dataset {
                    store.set(key, value).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    })
    .measurement_time(std::time::Duration::from_millis(500));
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().expect("unable to create temp dir");
                    let store = KvStore::open(temp_dir.path()).unwrap();
                    let mut rng: StdRng = SeedableRng::from_seed([1u8; 32]);
                    let mut key_set = Vec::new();
                    for _ in 0..NUM_GET_OP_PER_ITER / 10 {
                        let (key, value) = gen_rand_string_pair(rng.gen());
                        store.set(key.to_owned(), value.to_owned()).unwrap();
                        key_set.push(key);
                    }
                    let choose_rng = rand::thread_rng();
                    (store, key_set, choose_rng, temp_dir)
                },
                |(store, key_set, mut rng, _temp_dir)| {
                    for _ in 0..NUM_GET_OP_PER_ITER {
                        store
                            .get(key_set.choose(&mut rng).unwrap().to_owned())
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled", |b, _| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("unable to create temp dir");
                let store = SledKvsEngine::open(temp_dir.path()).unwrap();
                let mut rng: StdRng = SeedableRng::from_seed([1u8; 32]);
                let mut key_set = Vec::new();
                for _ in 0..NUM_GET_OP_PER_ITER / 10 {
                    let (key, value) = gen_rand_string_pair(rng.gen());
                    store.set(key.to_owned(), value.to_owned()).unwrap();
                    key_set.push(key);
                }
                let choose_rng = rand::thread_rng();
                (store, key_set, choose_rng)
            },
            |(store, key_set, mut rng)| {
                for _ in 0..NUM_GET_OP_PER_ITER {
                    store
                        .get(key_set.choose(&mut rng).unwrap().to_owned())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    })
    .measurement_time(std::time::Duration::from_millis(500));
    c.bench("get_bench", bench);
}

/* generate a random string with random length between 1 and MAX_STR_LEN */
fn gen_rand_string_pair(seed: u8) -> (String, String) {
    /* gen key */
    let mut rng: StdRng = SeedableRng::from_seed([seed; 32]);
    let size = rng.gen_range(1, MAX_STR_LEN);
    let mut v1 = Vec::with_capacity(size);
    rng.fill(&mut v1[..]);

    /* gen value */
    drop(rng);
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(1, 100_000);
    let mut v2 = Vec::with_capacity(size);
    rng.fill(&mut v2[..]);

    (
        String::from_utf8_lossy(&v1).to_string(),
        String::from_utf8_lossy(&v2).to_string(),
    )
}

criterion_group!(benches, set_bench, get_bench);
// criterion_group!(benches, get_bench);
// criterion_group!(benches, set_bench);
criterion_main!(benches);
