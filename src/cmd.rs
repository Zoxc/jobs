use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::collections::hash_map::Entry;
use std::panic;
use std::process::Command;

pub struct CommandRunner<'a, 'b> {
    builder: &'a Builder,
    cmd: &'b mut Command,
}

impl CommandRunner<'_, '_> {
    pub fn spawn(&mut self) -> io::Result<Child> {
        panic!()
    }

    pub fn output(&mut self) -> io::Result<Output> {
        let status = self.cmd.output();
        self.builder.unwind_if_aborted();
        status
    }

    pub fn status(&mut self) -> io::Result<ExitStatus> {
        let status = self.cmd.status();
        self.builder.unwind_if_aborted();
        status
    }
}

impl Builder {
    pub fn cmd(&'a self, cmd: &'b mut Command) -> CommandRunner<'a, 'b> {
        // Tell the process about our jobserver
        self.jobserver.configure(cmd);

        CommandRunner {
            builder,
            cmd,
        }
    }
}
