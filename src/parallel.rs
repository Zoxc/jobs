use crate::{Deps, DEPS};
use crossbeam_utils::thread;
use parking_lot::Mutex;
use std;

pub fn scope<'env, F, R>(f: F) -> std::thread::Result<R>
where
    F: FnOnce(Scope<'_, 'env>) -> R,
{
    if DEPS.is_set() {
        DEPS.with(|deps| {
            // Pretend `deps` have lifetime 'env .
            // This is fine because we only access `deps` inside the thread::scope call
            // and `deps` like 'env must outlive the call to this function.
            let deps: &'env Mutex<Deps> = unsafe { &*(deps as *const _) };
            thread::scope(|scope| {
                f(Scope {
                    scope,
                    deps: Some(deps),
                })
            })
        })
    } else {
        thread::scope(|scope| f(Scope { scope, deps: None }))
    }
}

#[derive(Copy, Clone)]
pub struct Scope<'a, 'env> {
    scope: &'a thread::Scope<'env>,
    deps: Option<&'env Mutex<Deps>>,
}

impl<'scope, 'env> Scope<'scope, 'env> {
    pub fn spawn<F, T>(self, f: F) -> thread::ScopedJoinHandle<'scope, T>
    where
        F: FnOnce(Scope<'_, 'env>) -> T,
        F: Send + 'env,
        T: Send + 'env,
    {
        let deps = self.deps;
        self.scope
            .builder()
            .spawn(move |scope| {
                if let Some(deps) = deps {
                    DEPS.set(deps, || {
                        f(Scope {
                            scope,
                            deps: Some(deps),
                        })
                    })
                } else {
                    f(Scope { scope, deps: None })
                }
            })
            .unwrap()
    }
}
