use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs;
use std::fmt;
use std::ops::Deref;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::{Mutex};
use std::collections::{HashMap};
use std::convert::AsRef;
use tasks;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Debug)]
pub struct Symbol(pub &'static str);

impl Symbol {
    pub fn new(s: &str) -> Self {
        *INTERNER.lock().unwrap().entry(s.to_string()).or_insert_with(|| {
            Symbol(Box::leak(s.to_string().into_boxed_str()))
        })
    }
}

impl<T: ?Sized> AsRef<T> for Symbol where str: AsRef<T> {
    fn as_ref(&self) -> &T {
        self.0.as_ref()
    }
}

impl Deref for Symbol {
    type Target = str;
    fn deref(&self) -> &str {
        self.0
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        self.0.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>
    {
        let r = <String as Deserialize<'de>>::deserialize(deserializer);
        r.map(|s| Symbol::new(&s))
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

lazy_static! {
    static ref INTERNER: Mutex<HashMap<String, Symbol>> = {
        Mutex::new(HashMap::new())
    };
}

/// Returns the last-modified time for `path`, or zero if it doesn't exist.
pub fn mtime_untracked(path: &Path) -> SystemTime {
    fs::metadata(path).and_then(|f| f.modified()).unwrap_or(UNIX_EPOCH)
}

tasks! {
    pub group Tasks;

    pub task use_mtime(path: PathBuf) -> SystemTime {
        println!("checking mtime of {:?}", path);
        mtime_untracked(&path)
    }
}

