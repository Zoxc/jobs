use crate::{Builder, Deps, DEPS};
use crossbeam_utils::thread;
use parking_lot::Mutex;
use std;

impl Builder {
    fn with_new_token<R>(&self, f: impl FnOnce() -> R) -> R {
        let _token = self.jobserver.acquire().ok();
        let r = f();
        r
    }

    pub fn scope<'env, F, R>(&'env self, f: F) -> std::thread::Result<R>
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
                        builder: self,
                        deps: Some(deps),
                    })
                })
            })
        } else {
            thread::scope(|scope| {
                f(Scope {
                    scope,
                    builder: self,
                    deps: None,
                })
            })
        }
    }
}

#[derive(Copy, Clone)]
pub struct Scope<'a, 'env> {
    scope: &'a thread::Scope<'env>,
    builder: &'env Builder,
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
        let builder = self.builder;
        self.scope
            .builder()
            .spawn(move |scope| {
                if let Some(deps) = deps {
                    DEPS.set(deps, || {
                        builder.with_new_token(|| {
                            f(Scope {
                                scope,
                                builder,
                                deps: Some(deps),
                            })
                        })
                    })
                } else {
                    builder.with_new_token(|| {
                        f(Scope {
                            scope,
                            builder,
                            deps: None,
                        })
                    })
                }
            })
            .unwrap()
    }
}
