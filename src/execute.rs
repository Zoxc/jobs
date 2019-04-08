use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::panic;
use serde::{Deserialize, Serialize};

use crate::util::Symbol;
use crate::{
    DEPS, DepNode, Builder, JobValue, JobHandle, DepNodeData, DepNodeState, PanickedJob,
    DepNodeChanges, Task, SerializedTask,
};

fn force_and_check(builder: &Builder, dep_node: &DepNode) -> bool {
    let forcer = builder.forcers.get(&dep_node.name).unwrap_or_else(|| {
        panic!("Task `{}` is not registered", dep_node.name);
    });

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
}

fn try_mark_up_to_date(builder: &Builder, dep_node: &DepNode) -> bool {
    if dep_node.input {
        return force_and_check(builder, dep_node);
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
        handle.await_task();
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
        force_and_check(builder, dep_node)
    }
}

fn force<
    C: FnOnce(&Builder, Vec<u8>) -> JobValue, 
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
                DEPS.set(&(handle.0).2, || compute(builder, dep_node.key.clone()))
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
        Action::Await(handle) => handle.await_task(),
    }
}

impl Builder {
    pub fn run<T: Task>(&mut self, task: T) {
        let dep_node = DepNode {
            name: Symbol(T::IDENTIFIER),
            input: T::IS_INPUT,
            task: SerializedTask::new(&task),
        };

        if DEPS.is_set() {
            //println!("adding dep {:?}", dep_node);
            DEPS.with(|deps| deps.lock().unwrap().from.insert(dep_node.clone()));
        }

        if self.forcers.get(&dep_node.name).is_none() {
            panic!("Task `{}` is not registered", dep_node.name);
        }

        let try_mark = match self.cached.lock().unwrap().get(&dep_node) {
            Some(&DepNodeState::Cached(..)) => true,
            _ => false,
        };

        if try_mark {
            if !try_mark_up_to_date(self, &dep_node) {
                let do_force = match state!(self, &dep_node) {
                    DepNodeState::Outdated |
                    DepNodeState::Cached(..) |
                    DepNodeState::Active(..) => true,
                    DepNodeState::Panicked |
                    DepNodeState::Fresh(..) => false,
                };
                if do_force {
                    force(self, &dep_node, Builder::run_erased::<T>)
                }
            }
        } else {
            force(self, &dep_node, Builder::run_erased::<T>)
        }

        match *self.cached.lock().unwrap().get(&dep_node).unwrap() {
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
}
