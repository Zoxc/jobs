extern crate core;
#[macro_use]
extern crate lazy_static;
extern crate bincode;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate scoped_tls;

// Allows macros to refer to this crate as `::jobs`
extern crate self as jobs;

use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
pub use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use bincode::{deserialize, serialize};
pub use util::Symbol;

mod execute;
pub mod parallel;
pub mod util;

pub struct PanickedTask;

pub struct TaskGroup(pub fn(&mut Builder));

pub trait Task: Eq + Serialize + for<'a> Deserialize<'a> + Hash + Send + Clone + 'static {
    /// An unique string identifying the task
    const IDENTIFIER: &'static str;

    const EVAL_ALWAYS: bool;
    const EARLY_CUTOFF: bool;

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
macro_rules! declare_task {
    ($vis:vis $name:ident $($field:ident: $ty:ty,)*) => {
        #[derive(jobs::Serialize, jobs::Deserialize, Hash, Eq, PartialEq, Clone, Debug)]
        $vis struct $name { $($vis $field: $ty,)* }
    };
}

#[macro_export]
macro_rules! is_eval_always {
    () => {{
        false
    }};
    (eval_always$(, $modifiers:ident)*) => {{
        true
    }};
    ($other:ident$(, $modifiers:ident)*) => {
        ::jobs::is_eval_always!($($modifiers),*)
    };
}

#[macro_export]
macro_rules! is_early_cutoff {
    () => {{
        true
    }};
    (no_early_cutoff$(, $modifiers:ident)*) => {{
        false
    }};
    ($other:ident$(, $modifiers:ident)*) => {
        ::jobs::is_early_cutoff!($($modifiers),*)
    };
}

#[macro_export]
macro_rules! tasks {
    (builder_var: $builder:ident;
     $gvis:vis group $group:ident;
        $(
            $(#[$attr:ident])* $tvis:vis task $name:ident {
                $($fields:tt)*
            } -> $res:ty { $($body:tt)* }
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
            ::jobs::declare_task!($tvis $name $($fields)*);

            impl ::jobs::Task for $name {
                // FIXME: Make this a function and use a `static` inside for an unique address
                const IDENTIFIER: &'static str = concat!(module_path!(), "::", stringify!($name));
                const EVAL_ALWAYS: bool = ::jobs::is_eval_always!($($attr),*);
                const EARLY_CUTOFF: bool = ::jobs::is_early_cutoff!($($attr),*);

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
    eval_always: bool,
    early_cutoff: bool,
    task: SerializedTask,
}

impl DepNode {
    fn new<T: Task>(task: &T) -> Self {
        DepNode {
            name: Symbol(T::IDENTIFIER),
            eval_always: T::EVAL_ALWAYS,
            early_cutoff: T::EARLY_CUTOFF,
            task: SerializedTask::new(task),
        }
    }
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
    Active(ActiveTaskHandle),
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
    data: Vec<(DepNode, DepNodeData)>,
}

pub struct Builder {
    index: PathBuf,
    task_executors: HashMap<Symbol, fn(&Builder, &SerializedTask) -> SerializedResult>,
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
        self.task_executors
            .insert(Symbol(T::IDENTIFIER), Builder::run_erased::<T>);
    }

    pub fn new(path: &Path) -> Self {
        let mut builder = Builder {
            index: path.to_path_buf(),
            task_executors: HashMap::new(),
            cached: Mutex::new(HashMap::new()),
        };
        builder.register(util::TASKS);
        builder
    }

    pub fn load(path: &Path) -> Self {
        let mut builder = Builder::new(path);
        let mut file = if let Ok(file) = File::open(&builder.index) {
            file
        } else {
            return builder;
        };
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let data = bincode::deserialize::<Graph>(&data).unwrap();
        let map = builder.cached.get_mut();
        for (node, data) in data.data {
            //println!("loading {:?} {:?}", node, data);
            map.insert(node, DepNodeState::Cached(data));
        }
        builder
    }

    pub fn save(self) {
        let mut graph = Vec::new();
        let map = self.cached.into_inner();
        for (node, state) in map.into_iter() {
            match state {
                DepNodeState::Cached(data) | DepNodeState::Fresh(data, _) => {
                    //println!("saving {:?} {:?}", node, data);
                    graph.push((node, data))
                }
                DepNodeState::Panicked => (),
                DepNodeState::Active(..) => panic!(),
            }
        }
        let graph = Graph { data: graph };
        let data = bincode::serialize(&graph).unwrap();
        let mut file = File::create(self.index).unwrap();
        file.write_all(&data).unwrap();
        let data = serde_json::to_string_pretty(&graph).unwrap();
        let mut file = File::create("build-index.json").unwrap();
        file.write_all(data.as_bytes()).unwrap();
    }
}

#[derive(Clone)]
pub struct ActiveTaskHandle(Arc<(Mutex<bool>, Condvar)>);

impl ActiveTaskHandle {
    fn new() -> Self {
        ActiveTaskHandle(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn await_task(&self) {
        let mut done = (self.0).0.lock();
        while !*done {
            (self.0).1.wait(&mut done);
        }
    }

    fn signal(&self) {
        (self.0).1.notify_all();
    }
}
