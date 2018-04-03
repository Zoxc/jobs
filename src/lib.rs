#![feature(use_extern_macros)]

extern crate jobs_proc_macro;
extern crate core;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate bincode;
#[macro_use]
extern crate scoped_tls;

use std::sync::{Arc, Mutex, Condvar};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::any::{Any, TypeId};
use std::hash::Hash;
use std::panic;
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

pub use jobs_proc_macro::job;

pub struct PanickedJob;

/// Returns the last-modified time for `path`, or zero if it doesn't exist.
fn mtime(path: &Path) -> SystemTime {
    fs::metadata(path).and_then(|f| f.modified()).unwrap_or(UNIX_EPOCH)
}

pub fn use_mtime(path: &Path) {
    DEPS.with(|deps| deps.lock().unwrap().files.insert((path.to_path_buf(), mtime(path))));
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
struct DepNode {
    name: &'static str,
    key: Vec<u8>,
}

scoped_thread_local!(static DEPS: Mutex<Deps>);

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Deps {
    #[serde(skip)]
    from: HashSet<DepNode>,
    #[serde(skip)]
    files: HashSet<(PathBuf, SystemTime)>,
}

impl Deps {
    fn empty() -> Self {
        Deps {
            from: HashSet::new(),
            files: HashSet::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct DepNodeData {
    #[serde(skip)]
    up_to_date: Cell<Option<bool>>,
    deps: Deps,
    value: Vec<u8>,
}

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
    cached: Mutex<HashMap<DepNode, DepNodeData>>,
    job_types: Mutex<HashMap<TypeId, NamedMap>>,
}

lazy_static! {
    static ref BUILDER: Builder = {
        Builder {
            cached: Mutex::new(HashMap::new()),
            job_types: Mutex::new(HashMap::new()),
        }
    };
}

#[derive(Clone)]
pub struct JobHandle(Arc<(Mutex<bool>, Condvar, Mutex<Deps>)>);

impl JobHandle {
    fn new() -> Self {
        JobHandle(Arc::new((Mutex::new(false), Condvar::new(), Mutex::new(Deps::empty()))))
    }

    fn await(&self) {
        let mut done = (self.0).0.lock().unwrap();
        while !*done {
            done = (self.0).1.wait(done).unwrap();
        }
    }

    fn drain_deps(&self) -> Deps {
        std::mem::replace(&mut *(self.0).2.lock().unwrap(), Deps::empty())
    }

    fn signal(&self) {
        (self.0).1.notify_all()
    }
}

fn is_up_to_date(cached: &HashMap<DepNode, DepNodeData>, dep_node: &DepNode) -> bool {
    let node = cached.get(&dep_node).unwrap();
    let status = node.up_to_date.get();
    if let Some(status) = status {
        return status;
    }
    let mut up_to_date = true;
    for node in &node.deps.from {
        up_to_date = up_to_date && is_up_to_date(cached, node);
    }
    for file in &node.deps.files {
        up_to_date = up_to_date && (mtime(&file.0) == file.1);
    }
    node.up_to_date.set(Some(up_to_date));
    up_to_date
}

pub fn execute_job<
    K: Eq + Serialize + Hash + Send + Clone + 'static,
    V: Send + Clone + Serialize + for<'a> Deserialize<'a> + 'static,
    C: (FnOnce(K) -> V) + 'static, 
> (name: &'static str, key: K, compute: C) -> V {
    enum Action<T> {
        Create(JobHandle),
        Await(JobHandle),
        Complete(T),
        Panic,
    }

    let dep_node = DepNode {
        name,
        key: bincode::serialize(&key).unwrap(),
    };

    if DEPS.is_set() {
        DEPS.with(|deps| deps.lock().unwrap().from.insert(dep_node.clone()));
    }

    let builder = &*BUILDER;
    let job_map: JobMap<K, V> = {
        let mut types_map = builder.job_types.lock().unwrap();
        let job_map = types_map.entry(TypeId::of::<C>()).or_insert_with(|| {
            let empty_map: Box<JobMap<K, V>> = Box::new(Arc::new(Mutex::new(HashMap::new())));
            NamedMap {
                name,
                map: empty_map,
            }
        });
        job_map.map.downcast_ref::<JobMap<K, V>>().unwrap().clone()
    };

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
            let mut cached = builder.cached.lock().unwrap();
            let r = if cached.get(&dep_node).is_some() && is_up_to_date(&*cached, &dep_node) {
                eprintln!("Loaded from cache {:?}", dep_node);
                let r = Ok(bincode::deserialize::<V>(&cached.get(&dep_node).unwrap().value).unwrap());
                drop(cached);
                r
            } else {
                drop(cached);
                let r = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    DEPS.set(&(handle.0).2, || compute(key.clone()))
                }));
                if let Ok(ref v) = r {
                    let deps = handle.drain_deps();
                    eprintln!("Computed {:?} with deps {:#?}", dep_node, deps);
                    builder.cached.lock().unwrap().insert(dep_node, DepNodeData {
                        up_to_date: Cell::new(None),
                        deps,
                        value: bincode::serialize(v).unwrap(),
                    });
                }
                r
            };
            let new_state = r.as_ref().ok().map(|v| JobState::Complete(v.clone()))
                                           .unwrap_or(JobState::Panicked);
            job_map.lock().unwrap().insert(key.clone(), new_state);
            let deps = handle.signal();
            match r {
                Ok(v) => return v,
                Err(e) => panic::resume_unwind(e),
            }
        }
        Action::Await(handle) => {
            eprintln!("Await {:?}", dep_node);
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
        Action::Complete(value) => {
            
                eprintln!("Complete already {:?}", dep_node);
            return value },
    }
}
