#![feature(use_extern_macros)]
#![feature(proc_macro)]

extern crate core;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate bincode;
#[macro_use]
extern crate scoped_tls;

// Allows macros to refer to this crate as `::jobs`
extern crate self as jobs;

use std::sync::{Arc, Mutex, Condvar};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::panic;
use std::fs::{File};
use std::io::{Write, Read};
use serde::{Deserialize, Serialize};

pub use bincode::{deserialize, serialize};
pub use util::Symbol;

pub struct PanickedJob;

pub mod util;

pub struct TaskGroup(fn (&mut Builder));

#[macro_export]
macro_rules! tasks {
    ($gvis:vis group $group:ident; $($tvis:vis task $name:ident $arg:tt -> $res:ty { $($body:tt)* })*) => {
        $gvis const $group: ::jobs::TaskGroup = {
            fn register() {

            }

            ::job::TaskGroup(register)
        };
    };
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
struct DepNode {
    name: Symbol,
    input: bool,
    key: Vec<u8>,
}

scoped_thread_local!(static DEPS: Mutex<Deps>);

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Deps {
    from: HashSet<DepNode>,
}

impl Deps {
    fn empty() -> Self {
        Deps {
            from: HashSet::new(),
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

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DepNodeData {
    deps: Deps,
    value: JobValue,
}

struct Builder {
    forcers: Mutex<HashMap<Symbol, fn(Vec<u8>) -> JobValue>>,
    cached: Mutex<HashMap<DepNode, DepNodeState>>,
}

#[derive(Serialize, Deserialize)]
struct Graph {
    data: Vec<(DepNode, DepNodeData)>
}

pub trait RecheckResultOfJob {
    fn forcer() -> (Symbol, fn(Vec<u8>) -> JobValue);
}

pub fn recheck_result_of<T: RecheckResultOfJob>() {
    let (name, forcer) = T::forcer();
    BUILDER.forcers.lock().unwrap().insert(name, forcer);
}

pub fn setup() {
    recheck_result_of::<util::use_mtime>();
    load();
}

const PATH: &str = "build-index";

pub fn load() {
    let mut file = if let Ok(file) = File::open(PATH) {
        file
    } else {
        return;
    };
    let mut data = Vec::new();
    file.read_to_end(&mut data).unwrap();
    let data = bincode::deserialize::<Graph>(&data).unwrap();
    let map = &mut *BUILDER.cached.lock().unwrap();
    for (node, data) in data.data {
        //println!("loading {:?} {:?}", node, data);
        map.insert(node, DepNodeState::Cached(data));
    }
}

pub fn save() {
    let mut graph = Vec::new();
    let map = &mut *BUILDER.cached.lock().unwrap();
    for (node, state) in map.iter() {
        match *state {
            DepNodeState::Cached(ref data) |
            DepNodeState::Fresh(ref data, _)  => {
                //println!("saving {:?} {:?}", node, data);
             graph.push((node.clone(), data.clone()))
        },
            DepNodeState::Panicked |
            DepNodeState::Outdated => (),
            DepNodeState::Active(..) => panic!(),
        }
    }
    let graph = Graph { data: graph };
    let data = bincode::serialize(&graph).unwrap();
    let mut file = File::create(PATH).unwrap();
    file.write_all(&data).unwrap();
    let data = serde_json::to_string_pretty(&graph).unwrap();
    let mut file = File::create("build-index.json").unwrap();
    file.write_all(data.as_bytes()).unwrap();
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
    if dep_node.input {
        let forcer = builder.forcers.lock().unwrap().get(&dep_node.name).map(|f| *f);
        if let Some(forcer) = forcer {
            force(builder, dep_node, forcer);
            // Check if its still outdated
            return match state!(builder, &dep_node) {
                DepNodeState::Outdated |
                DepNodeState::Cached(..) |
                DepNodeState::Active(..) => panic!(),
                DepNodeState::Panicked => false,
                DepNodeState::Fresh(_, DepNodeChanges::Unchanged) => true,
                DepNodeState::Fresh(_, DepNodeChanges::Changed) => false,
            }
        } else {
            panic!("input job `{}` must be registered as rechecked job", dep_node.name);
        }
    }

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
            assert!(!dep_node.input, "input jobs must be registered as checked jobs");
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
                    //println!("gots deps {:?} for node {:?}", deps, dep_node);
                    if deps.from.is_empty() && !dep_node.input {
                        eprintln!("warning: job `{}` has no dependencies \
                                   and isn't an input, it won't be executed again", dep_node.name);
                    }
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
> (name: Symbol, input: bool, key: K, compute_orig: C) -> V {
    let compute = move |key: Vec<u8>| -> JobValue {
        let key = bincode::deserialize::<K>(&key).unwrap();
        let value = compute_orig(key);
        bincode::serialize(&value).unwrap()
    };

    let dep_node = DepNode {
        name,
        input,
        key: bincode::serialize(&key).unwrap(),
    };

    if DEPS.is_set() {
        //println!("adding dep {:?}", dep_node);
        DEPS.with(|deps| deps.lock().unwrap().from.insert(dep_node.clone()));
    }

    let builder = &*BUILDER;

    if dep_node.input {
        if builder.forcers.lock().unwrap().get(&dep_node.name).is_none() {
            panic!("input job `{}` must be registered as rechecked job", dep_node.name);
        }
    }

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
