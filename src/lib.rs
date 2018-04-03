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
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

pub use jobs_proc_macro::job;

pub struct PanickedJob;
/*
/// Returns the last-modified time for `path`, or zero if it doesn't exist.
fn mtime(path: &Path) -> SystemTime {
    fs::metadata(path).and_then(|f| f.modified()).unwrap_or(UNIX_EPOCH)
}

pub fn use_mtime(path: &Path) {
    DEPS.with(|deps| deps.lock().unwrap().files.insert((path.to_path_buf(), mtime(path))));
}
*/
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
    //#[serde(skip)]
    //files: HashSet<(PathBuf, SystemTime)>,
}

impl Deps {
    fn empty() -> Self {
        Deps {
            from: HashSet::new(),
            //files: HashSet::new(),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum DepNodeChanges {
    Unchanged,
    Changed,
}

enum DepNodeState {
    Cached(DepNodeData),
    Outdated,
    Active(JobHandle),
    Panicked,
    Fresh(DepNodeData, DepNodeChanges),
}

type JobValue = Vec<u8>;

#[derive(Serialize, Deserialize, Clone)]
struct DepNodeData {
    deps: Deps,
    value: JobValue,
}

struct NamedMap {
    name: &'static str,
    map: Box<Any + Send>,
}

struct Builder {
    forcers: Mutex<HashMap<&'static str, fn(Vec<u8>) -> JobValue>>,
    cached: Mutex<HashMap<DepNode, DepNodeState>>,
}

macro_rules! state_map {
    ($builder:expr) => (*$builder.cached.lock().unwrap())
}

macro_rules! state {
    ($builder:expr, $dep_node:expr) => (*state_map!($builder).get($dep_node).unwrap())
}

lazy_static! {
    static ref BUILDER: Builder = {
        Builder {
            forcers: Mutex::new(HashMap::new()),
            cached: Mutex::new(HashMap::new()),
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

fn try_mark_up_to_date(builder: &Builder, dep_node: &DepNode) -> bool {
    let deps = loop {
        let handle = match state!(builder, &dep_node) {
            DepNodeState::Active(ref handle) => {
                // Await the result and then retry
                handle.clone()
            }
            DepNodeState::Outdated |
            DepNodeState::Panicked => return false,
            DepNodeState::Cached(ref data) => break data.deps.from.clone(),
            DepNodeState::Fresh(_, changes) => return changes == DepNodeChanges::Unchanged,
        };
        handle.await();
    };
    let up_to_date = deps.into_iter().all(|node| try_mark_up_to_date(builder, &node));

    if up_to_date {
        // Mark the node as green
        let state_map = &mut state_map!(builder);
        let state = state_map.get_mut(&dep_node).unwrap();
        let new = match *state {
            DepNodeState::Outdated |
            DepNodeState::Panicked |
            DepNodeState::Active(..) => panic!(),
            DepNodeState::Cached(ref data) => Some(DepNodeState::Fresh(data.clone(),
                                                                          DepNodeChanges::Unchanged)),
            DepNodeState::Fresh(_, DepNodeChanges::Changed) => panic!(),
            // Someone else marked it as unchanged already
            DepNodeState::Fresh(_, DepNodeChanges::Unchanged) => None, 
        };
        if let Some(new) = new {
            *state = new;
        }
        true
    } else {
        let forcer = builder.forcers.lock().unwrap().get(&dep_node.name).map(|f| *f);

        if let Some(forcer) = forcer {
            force(builder, dep_node, forcer);
            // Check if its still outdated
            match state!(builder, &dep_node) {
                DepNodeState::Outdated |
                DepNodeState::Cached(..) |
                DepNodeState::Active(..) => panic!(),
                DepNodeState::Panicked => false,
                DepNodeState::Fresh(_, DepNodeChanges::Unchanged) => true,
                DepNodeState::Fresh(_, DepNodeChanges::Changed) => false,
            }
        } else {
            // Mark the node as outdated
            let state_map = &mut state_map!(builder);
            let state = state_map.get_mut(&dep_node).unwrap();
            let update = match *state {
                DepNodeState::Outdated => false, // Someone else marked it as outdated already
                DepNodeState::Panicked |
                DepNodeState::Active(..) => false, // Someone else executed it already, do nothing
                DepNodeState::Cached(..) => true,
                DepNodeState::Fresh(_, DepNodeChanges::Unchanged) => panic!(),

                // Someone else executed it already, do nothing
                DepNodeState::Fresh(_, DepNodeChanges::Changed) => false,
            };
            if update {
                *state =  DepNodeState::Outdated;
            }
            false
        }
    }
}

fn force<
    C: (FnOnce(Vec<u8>) -> JobValue) + 'static, 
> (builder: &Builder, dep_node: &DepNode, compute: C) {
    enum Action {
        Create(JobHandle, Option<JobValue>),
        Await(JobHandle),
    }

    let action = {
        match builder.cached.lock().unwrap().entry(dep_node.clone()) {
            Entry::Occupied(mut entry) => {
                let action = match *entry.get() {
                    DepNodeState::Active(ref handle) => Action::Await(handle.clone()),
                    DepNodeState::Fresh(..) |
                    DepNodeState::Panicked => return,
                    DepNodeState::Outdated => Action::Create(JobHandle::new(), None),
                    DepNodeState::Cached(ref data) => {
                        Action::Create(JobHandle::new(), Some(data.value.clone()))
                    }
                };
                if let Action::Create(ref handle, _) = action {
                    *entry.get_mut() = DepNodeState::Active(handle.clone())
                }
                action
            }
            Entry::Vacant(entry) => {
                let handle = JobHandle::new();
                entry.insert(DepNodeState::Active(handle.clone()));
                Action::Create(handle, None)
            }
        }
    };

    match action {
        Action::Create(handle, old_value) => {
            let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                DEPS.set(&(handle.0).2, || compute(dep_node.key.clone()))
            }));

            let (new_state, panic) = match result {
                Ok(value) => {
                    let deps = handle.drain_deps();
                    let changes = if old_value.map(|old_value| old_value == value).unwrap_or(false) {
                        DepNodeChanges::Unchanged
                    } else {
                        DepNodeChanges::Changed
                    };
                    (DepNodeState::Fresh(DepNodeData {
                        deps,
                        value,
                    }, changes), None)
                }
                Err(panic) => {
                    (DepNodeState::Panicked, Some(panic))
                }
            };

            {
                let state_map = &mut state_map!(builder);
                let state = state_map.get_mut(&dep_node).unwrap();

                match *state {
                    DepNodeState::Active(..) => (),
                    _ => panic!(),
                }

                *state = new_state;
            }

            handle.signal();

            if let Some(panic) = panic {
                panic::resume_unwind(panic)
            }
        }
        Action::Await(handle) => handle.await(),
    }
}

pub fn execute_job<
    K: Eq + Serialize + for<'a> Deserialize<'a> + Hash + Send + Clone + 'static,
    V: Send + Clone + Serialize + for<'a> Deserialize<'a> + 'static,
    C: (FnOnce(K) -> V) + 'static, 
> (name: &'static str, key: K, compute_orig: C) -> V {
    enum Action<T> {
        Create(JobHandle),
        Await(JobHandle),
        Complete(T),
        Panic,
    }

    let compute = move |key: Vec<u8>| -> JobValue {
        let key = bincode::deserialize::<K>(&key).unwrap();
        let value = compute_orig(key);
        bincode::serialize(&value).unwrap()
    };

    let dep_node = DepNode {
        name,
        key: bincode::serialize(&key).unwrap(),
    };

    if DEPS.is_set() {
        DEPS.with(|deps| deps.lock().unwrap().from.insert(dep_node.clone()));
    }

    let builder = &*BUILDER;

    let try_mark = match builder.cached.lock().unwrap().get(&dep_node) {
        Some(&DepNodeState::Cached(..)) => true,
        _ => false,
    };

    if try_mark {
        if !try_mark_up_to_date(builder, &dep_node) {
            let do_force = match state!(builder, &dep_node) {
                DepNodeState::Outdated |
                DepNodeState::Cached(..) |
                DepNodeState::Active(..) => true,
                DepNodeState::Panicked |
                DepNodeState::Fresh(..) => false,
            };
            if do_force {
                force(builder, &dep_node, compute)
            }
        }
    } else {
        force(builder, &dep_node, compute)
    }

    match *builder.cached.lock().unwrap().get(&dep_node).unwrap() {
        DepNodeState::Outdated |
        DepNodeState::Cached(..) |
        DepNodeState::Active(..) => panic!(),
        DepNodeState::Panicked => (),
        DepNodeState::Fresh(ref data, _) => {
            return bincode::deserialize::<V>(&data.value).unwrap()
        }
    };
    // Panic here so we don't poison the lock
    panic::resume_unwind(Box::new(PanickedJob))
}
