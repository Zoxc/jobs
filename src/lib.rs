#![feature(use_extern_macros)]

extern crate jobs_proc_macro;
extern crate core;
#[macro_use]
extern crate lazy_static;

use std::sync::{Arc, Mutex, Condvar};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::any::{Any, TypeId};
use std::hash::Hash;
use std::panic;

pub use jobs_proc_macro::job;

pub struct PanickedJob;

pub enum JobState<T> {
    Active(JobHandle),
    Complete(T),
    Panicked,
}

type JobMap<K, V> = Arc<Mutex<HashMap<K, JobState<V>>>>;

struct NamedMap {
    name: &'static str,
    map: Box<Any + Send>,
}

struct Builder {
    job_types: Mutex<HashMap<TypeId, NamedMap>>,
}

lazy_static! {
    static ref BUILDER: Builder = {
        Builder {
            job_types: Mutex::new(HashMap::new()),
        }
    };
}

#[derive(Clone)]
pub struct JobHandle(Arc<(Mutex<bool>, Condvar)>);

impl JobHandle {
    fn new() -> Self {
        JobHandle(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn await(&self) {
        let mut done = (self.0).0.lock().unwrap();
        while !*done {
            done = (self.0).1.wait(done).unwrap();
        }
    }

    fn signal(&self) {
        (self.0).1.notify_all()
    }
}

pub fn execute_job<
    K: Eq + Hash + Send + Clone + 'static,
    V: Send + Clone + 'static,
    C: (FnOnce(K) -> V) + 'static, 
> (name: &'static str, key: K, compute: C) -> V {
    enum Action<T> {
        Create(JobHandle),
        Await(JobHandle),
        Complete(T),
        Panic,
    }

    let mut types_map = BUILDER.job_types.lock().unwrap();
    let job_map = types_map.entry(TypeId::of::<C>()).or_insert_with(|| {
        let empty_map: Box<JobMap<K, V>> = Box::new(Arc::new(Mutex::new(HashMap::new())));
        NamedMap {
            name,
            map: empty_map,
        }
    });
    let job_map = job_map.map.downcast_ref::<JobMap<K, V>>().unwrap().clone();

    let action = {
        match job_map.lock().unwrap().entry(key.clone()) {
            Entry::Occupied(entry) => {
                match *entry.get() {
                    JobState::Active(ref handle) => Action::Await(handle.clone()),
                    JobState::Complete(ref value) => Action::Complete(value.clone()),
                    JobState::Panicked => Action::Panic,
                }
            }
            Entry::Vacant(entry) => {
                let handle = JobHandle::new();
                let r = handle.clone();
                entry.insert(JobState::Active(handle));
                Action::Create(r)
            }
        }
    };

    match action {
        Action::Create(handle) => {
            let r = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                compute(key.clone())
            }));
            let new_state = r.as_ref().ok().map(|v| JobState::Complete(v.clone()))
                                           .unwrap_or(JobState::Panicked);
            job_map.lock().unwrap().insert(key.clone(), new_state);
            handle.signal();
            match r {
                Ok(v) => return v,
                Err(e) => panic::resume_unwind(e),
            }
        }
        Action::Await(handle) => {
            handle.await();
            match *job_map.lock().unwrap().get(&key).unwrap() {
                JobState::Complete(ref value) => return value.clone(),
                JobState::Panicked => (),
                _ => panic!(),
            }
            // Panic here so we don't poison the lock
            panic::resume_unwind(Box::new(PanickedJob))
        }
        Action::Panic => panic::resume_unwind(Box::new(PanickedJob)),
        Action::Complete(value) => return value,
    }
}
