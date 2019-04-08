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
use std::hash::Hash;
use std::panic;
use std::fs::{File};
use std::io::{Write, Read};
use serde::{Deserialize, Serialize};

pub use bincode::{deserialize, serialize};
pub use util::Symbol;

pub struct PanickedJob;

macro_rules! state_map {
    ($builder:expr) => (*$builder.cached.lock().unwrap())
}

macro_rules! state {
    ($builder:expr, $dep_node:expr) => (*state_map!($builder).get($dep_node).unwrap())
}

pub mod util;
mod execute;

pub struct TaskGroup(fn (&mut Builder));

pub trait Task: Eq + Serialize + for<'a> Deserialize<'a> + Hash + Send + Clone + 'static  {
    const IDENTIFIER: &'static str;
    const IS_INPUT: bool;

    type Result: Send + Clone + Serialize + for<'a> Deserialize<'a> + 'static;

    fn run(builder: &Builder, key: Self) -> Self::Result;
}

#[macro_export]
macro_rules! strip_field_tys {
    ($name:ident $($field:ident: $ty:ty,)*) => {
        $name { $($field,)* }
    };
}

#[macro_export]
macro_rules! tasks {
    (builder_var: $builder:ident;
     $gvis:vis group $group:ident;
        $(
            $tvis:vis task $name:ident { $($fields:tt)* } -> $res:ty { $($body:tt)* }
        )*
    ) => {
        $gvis const $group: ::jobs::TaskGroup = {
            fn register(builder: &mut ::jobs::Builder) {
                $(
                    builder.register_task::<$name>();
                )*
            }

            ::jobs::TaskGroup(register)
        };
        $(
            #[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
            $tvis struct $name { $($fields)* }

            impl ::jobs::Task for $name {
                // FIXME: Make this a function and use a `static` inside for an unique address
                const IDENTIFIER: &'static str = concat!(module_path!(), "::", stringify!(#ident));
                const IS_INPUT: bool = false;

                type Result = $res;

                fn run($builder: &::jobs::Builder, key: Self) -> Self::Result {
                    let ::jobs::strip_field_tys!($name $($fields)*) = key;
                    $($body)*
                }
            }

        )*
    };
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
struct SerializedTask(Vec<u8>);

impl SerializedTask {
    fn new(task: &impl Task) -> SerializedTask {
        SerializedTask(bincode::serialize(&task).unwrap())
    }
    fn to_task<T: Task>(&self) -> T {
        bincode::deserialize(&self.0).unwrap()
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
struct SerializedResult(Vec<u8>);

impl SerializedResult {
    fn new<T: Task>(result: &T::Result) -> SerializedResult {
        SerializedResult(bincode::serialize(&result).unwrap())
    }
    fn to_result<T: Task>(&self) -> T::Result {
        bincode::deserialize(&self.0).unwrap()
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Debug)]
struct DepNode {
    name: Symbol,
    input: bool,
    task: SerializedTask,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DepNodeData {
    deps: Deps,
    result: SerializedResult,
}

#[derive(Serialize, Deserialize)]
struct Graph {
    data: Vec<(DepNode, DepNodeData)>
}

const PATH: &str = "build-index";

pub struct Builder {
    forcers: HashMap<Symbol, fn(&Builder, &SerializedTask) -> SerializedResult>,
    cached: Mutex<HashMap<DepNode, DepNodeState>>,
}

impl Builder {
    pub fn register(&mut self, group: TaskGroup) {
        group.0(self)
    }

    fn run_erased<T: Task>(builder: &Builder, task: &SerializedTask) -> SerializedResult {
        let value = T::run(builder, task.to_task::<T>());
        SerializedResult::new::<T>(&value)
    }

    pub fn register_task<T: Task>(&mut self) {
        self.forcers.insert(Symbol(T::IDENTIFIER), Builder::run_erased::<T>);
    }

    pub fn load(path: &str) -> Self {
        let mut file = if let Ok(file) = File::open(path) {
            file
        } else {
            return Builder {
                forcers: HashMap::new(),
                cached: Mutex::new(HashMap::new()),
            };
        };
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let data = bincode::deserialize::<Graph>(&data).unwrap();
        let mut map = HashMap::new();
        for (node, data) in data.data {
            //println!("loading {:?} {:?}", node, data);
            map.insert(node, DepNodeState::Cached(data));
        }
        Builder {
            forcers: HashMap::new(),
            cached: Mutex::new(map),
        }
    }

    pub fn save(self) {
        let mut graph = Vec::new();
        let map = self.cached.into_inner().unwrap();
        for (node, state) in map.into_iter() {
            match state {
                DepNodeState::Cached(data) |
                DepNodeState::Fresh(data, _)  => {
                    //println!("saving {:?} {:?}", node, data);
                graph.push((node, data))
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
}

#[derive(Clone)]
pub struct JobHandle(Arc<(Mutex<bool>, Condvar, Mutex<Deps>)>);

impl JobHandle {
    fn new() -> Self {
        JobHandle(Arc::new((Mutex::new(false), Condvar::new(), Mutex::new(Deps::empty()))))
    }

    fn await_task(&self) {
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
