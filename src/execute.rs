use std::collections::hash_map::Entry;
use std::panic;

use crate::util::Symbol;
use crate::{
    DEPS, DepNode, Builder, JobHandle, DepNodeData, DepNodeState, PanickedJob,
    DepNodeChanges, Task, SerializedTask, SerializedResult,
};

fn force_and_check(builder: &Builder, dep_node: &DepNode) -> bool {
    let forcer = *builder.forcers.get(&dep_node.name).unwrap_or_else(|| {
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
    if dep_node.eval_always {
        // We can immediately execute `eval_always` queries
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

fn force(
    builder: &Builder,
    dep_node: &DepNode,
    compute: fn(&Builder, &SerializedTask) -> SerializedResult,
) {
    enum Action {
        Create(JobHandle, Option<SerializedResult>),
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
                        Action::Create(JobHandle::new(), Some(data.result.clone()))
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
        Action::Create(handle, old_result) => {
            let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                DEPS.set(&(handle.0).2, || compute(builder, &dep_node.task))
            }));

            let (new_state, panic) = match result {
                Ok(result) => {
                    let deps = handle.drain_deps();
                    //println!("gots deps {:?} for node {:?}", deps, dep_node);
                    if deps.from.is_empty() && !dep_node.eval_always {
                        eprintln!("warning: task `{}` has no dependencies \
                                   and isn't an #[eval_always] task, it won't be executed again", dep_node.name);
                    }
                    let changes = if old_result.map(|old_result| old_result == result).unwrap_or(false) {
                        DepNodeChanges::Unchanged
                    } else {
                        DepNodeChanges::Changed
                    };
                    (DepNodeState::Fresh(DepNodeData {
                        deps,
                        result,
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
    pub fn run<T: Task>(&self, task: T) -> T::Result {
        let dep_node = DepNode {
            name: Symbol(T::IDENTIFIER),
            eval_always: T::EVAL_ALWAYS,
            task: SerializedTask::new(&task),
        };

        if DEPS.is_set() {
            // Add this task to the list of dependencies for the current running task
            DEPS.with(|deps| deps.lock().unwrap().from.insert(dep_node.clone()));
        }

        // Make sure this task is registered
        if self.forcers.get(&dep_node.name).is_none() {
            panic!("Task `{}` is not registered", dep_node.name);
        }

        // Is there a cached result for this task?
        let cached_result_exists = match self.cached.lock().unwrap().get(&dep_node) {
            Some(&DepNodeState::Cached(..)) => true,
            _ => false,
        };

        if cached_result_exists {
            // Try to bring the cached result up to date
            if !try_mark_up_to_date(self, &dep_node) {
                let outdated = match state!(self, &dep_node) {
                    DepNodeState::Outdated |
                    DepNodeState::Cached(..) |
                    DepNodeState::Active(..) => true,
                    DepNodeState::Panicked |
                    DepNodeState::Fresh(..) => false,
                };
                if outdated {
                    // The result was outdated, force the task to run
                    force(self, &dep_node, Builder::run_erased::<T>)
                }
            }
        } else {
            // There was no cached result, force the task to run
            force(self, &dep_node, Builder::run_erased::<T>)
        }

        match *self.cached.lock().unwrap().get(&dep_node).unwrap() {
            DepNodeState::Outdated |
            DepNodeState::Cached(..) |
            DepNodeState::Active(..) => panic!(),
            DepNodeState::Panicked => (),
            DepNodeState::Fresh(ref data, _) => {
                return data.result.to_result::<T>()
            }
        };
        // Panic here so we don't poison the lock
        panic::resume_unwind(Box::new(PanickedJob))
    }
}
