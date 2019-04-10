use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::cmp;
use std::collections::hash_map::Entry;
use std::mem;
use std::panic;

use crate::{
    ActiveTaskHandle, Builder, DepNode, DepNodeData, DepNodeState, Deps, PanickedTask,
    SerializedResult, Task, DEPS,
};

enum Age {
    Stale,
    Session(u64),
}

impl Builder {
    fn node_state(&self, dep_node: &DepNode) -> MappedMutexGuard<'_, DepNodeState> {
        MutexGuard::map(self.cached.lock(), |cached| {
            cached.get_mut(dep_node).unwrap()
        })
    }

    fn try_mark_up_to_date(&self, dep_node: &DepNode) -> Age {
        if dep_node.eval_always {
            // We can immediately execute `eval_always` queries
            return self.force_and_check(dep_node);
        }

        let (session, deps) = loop {
            let handle = match *self.node_state(&dep_node) {
                DepNodeState::Active(ref handle) => {
                    // Await the result and then retry
                    handle.clone()
                }
                DepNodeState::Panicked => return Age::Stale,
                DepNodeState::Cached(ref data) => break (data.session, data.deps.from.clone()),
                DepNodeState::Fresh(ref data) => return Age::Session(data.session),
            };
            handle.await_task();
        };
        let deps_age = deps.into_iter().fold(None, |newest_age, node| {
            if let Age::Session(session) = self.try_mark_up_to_date(&node) {
                match newest_age {
                    Some(Age::Session(newest_session)) => {
                        Some(Age::Session(cmp::max(newest_session, session)))
                    }
                    Some(Age::Stale) => Some(Age::Stale),
                    None => Some(Age::Session(session)),
                }
            } else {
                // The dependency is stale
                Some(Age::Stale)
            }
        });

        let up_to_date = deps_age
            .map(|deps_age| {
                if let Age::Session(deps_session) = deps_age {
                    deps_session <= session
                } else {
                    false
                }
            })
            .unwrap_or(true);

        if up_to_date {
            // Mark the node as fresh
            let mut state = self.node_state(dep_node);
            let old_state = mem::replace(&mut *state, DepNodeState::Panicked);
            *state = match old_state {
                DepNodeState::Panicked | DepNodeState::Active(..) => panic!(),
                DepNodeState::Cached(data) |
                // Someone else marked it as fresh already
                DepNodeState::Fresh(data) => {
                    DepNodeState::Fresh(data)
                }
            };
            Age::Session(session)
        } else {
            self.force_and_check(dep_node)
        }
    }

    // Forces the task to run and return the age of its result
    fn force_and_check(&self, dep_node: &DepNode) -> Age {
        self.force(dep_node);

        // Check if its still outdated
        match *self.node_state(&dep_node) {
            DepNodeState::Cached(..) | DepNodeState::Active(..) => panic!(),
            DepNodeState::Panicked => Age::Stale,
            DepNodeState::Fresh(ref data) => Age::Session(data.session),
        }
    }

    fn force(&self, dep_node: &DepNode) {
        enum Action {
            Create(ActiveTaskHandle, Option<(u64, SerializedResult)>),
            Await(ActiveTaskHandle),
        }

        let compute = self.task_executors.get(&dep_node.name).unwrap_or_else(|| {
            panic!("Task `{}` is not registered", dep_node.name);
        });

        let action = {
            match self.cached.lock().entry(dep_node.clone()) {
                Entry::Occupied(mut entry) => {
                    let action = match *entry.get() {
                        DepNodeState::Active(ref handle) => Action::Await(handle.clone()),
                        DepNodeState::Fresh(..) | DepNodeState::Panicked => return,
                        DepNodeState::Cached(ref data) => Action::Create(
                            ActiveTaskHandle::new(),
                            Some((data.session, data.result.clone())),
                        ),
                    };
                    if let Action::Create(ref handle, _) = action {
                        *entry.get_mut() = DepNodeState::Active(handle.clone())
                    }
                    action
                }
                Entry::Vacant(entry) => {
                    let handle = ActiveTaskHandle::new();
                    entry.insert(DepNodeState::Active(handle.clone()));
                    Action::Create(handle, None)
                }
            }
        };

        match action {
            Action::Create(handle, old_result) => {
                let deps = Mutex::new(Deps::empty());
                let print = self.task_printers.get(&dep_node.name).unwrap();

                println!("running `{}`", print(&dep_node.task));
                let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    DEPS.set(&deps, || compute(self, &dep_node.task))
                }));

                let (new_state, panic) = match result {
                    Ok(result) => {
                        let deps = deps.into_inner();
                        //println!("gots deps {:?} for node {:?}", deps, dep_node);
                        if deps.from.is_empty() && !dep_node.eval_always {
                            let print = self.task_printers.get(&dep_node.name).unwrap();

                            eprintln!(
                                "warning: task `{}` has no dependencies \
                                 and isn't an #[eval_always] task, it won't be executed again",
                                print(&dep_node.task)
                            );
                        }
                        let session = match (dep_node.early_cutoff, &old_result) {
                            (true, Some((old_session, old_result))) if result == *old_result => {
                                *old_session
                            }
                            _ => self.session,
                        };
                        (
                            DepNodeState::Fresh(DepNodeData {
                                deps,
                                result,
                                session,
                            }),
                            None,
                        )
                    }
                    Err(panic) => (DepNodeState::Panicked, Some(panic)),
                };

                let mut state = self.node_state(&dep_node);

                match *state {
                    DepNodeState::Active(..) => (),
                    _ => panic!(),
                }

                *state = new_state;

                handle.signal();

                if let Some(panic) = panic {
                    panic::resume_unwind(panic)
                }
            }
            Action::Await(handle) => handle.await_task(),
        }
    }

    pub fn run<T: Task>(&self, task: T) -> T::Result {
        let dep_node = DepNode::new(&task);

        if DEPS.is_set() {
            // Add this task to the list of dependencies for the current running task
            DEPS.with(|deps| deps.lock().from.insert(dep_node.clone()));
        }

        // Is there a cached result for this task?
        let cached_result_exists = !dep_node.eval_always
            && match self.cached.lock().get(&dep_node) {
                Some(&DepNodeState::Cached(..)) => true,
                _ => false,
            };

        let outdated = !cached_result_exists || {
            // Try to bring the cached result up to date
            if let Age::Stale = self.try_mark_up_to_date(&dep_node) {
                match *self.node_state(&dep_node) {
                    DepNodeState::Cached(..) | DepNodeState::Active(..) => true,
                    DepNodeState::Panicked | DepNodeState::Fresh(..) => false,
                }
            } else {
                false
            }
        };

        if outdated {
            // There was no cached result, force the task to run
            self.force(&dep_node)
        }

        match *self.node_state(&dep_node) {
            DepNodeState::Cached(..) | DepNodeState::Active(..) => panic!(),
            DepNodeState::Panicked => (),
            DepNodeState::Fresh(ref data) => return data.result.to_result::<T>(),
        };
        // Panic here so we don't poison the lock
        panic::resume_unwind(Box::new(PanickedTask))
    }
}
