mod dir;
mod stream;

use std::path::{Path, PathBuf};
use std::{fs, io};

use anyhow::{Context, Result, bail};
use bincode::Options;

pub use crate::db::dir::{Dir, Epoch, Rank};
pub use crate::db::stream::{Stream, StreamOptions};
use crate::{config, util};

pub struct Database {
    path: PathBuf,
    dirs: Vec<Dir>,
    dirty: bool,
}

impl Database {
    const VERSION: u32 = 3;

    pub fn open() -> Result<Self> {
        let data_dir = config::data_dir()?;
        Self::open_dir(data_dir)
    }

    pub fn open_dir(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let path = data_dir.join("db.zo");
        let path = fs::canonicalize(&path).unwrap_or(path);

        match fs::read(&path) {
            Ok(bytes) => {
                let dirs = Self::deserialize(&bytes)?;
                Ok(Self { path, dirs, dirty: false })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(data_dir).with_context(|| {
                    format!("unable to create data directory: {}", data_dir.display())
                })?;
                Ok(Self { path, dirs: Vec::new(), dirty: false })
            }
            Err(e) => {
                Err(e).with_context(|| format!("could not read from database: {}", path.display()))
            }
        }
    }

    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let bytes = Self::serialize(&self.dirs)?;
        util::write(&self.path, bytes).context("could not write to database")?;
        self.dirty = false;

        Ok(())
    }

    pub fn add(&mut self, path: impl AsRef<str> + Into<String>, by: Rank, now: Epoch) {
        let path_str = path.as_ref();
        match self.dirs.iter_mut().find(|dir| dir.path == path_str) {
            Some(dir) => dir.rank = (dir.rank + by).max(0.0),
            None => {
                self.dirs.push(Dir { path: path.into(), rank: by.max(0.0), last_accessed: now })
            }
        }
        self.dirty = true;
    }

    pub fn add_unchecked(&mut self, path: impl Into<String>, rank: Rank, now: Epoch) {
        self.dirs.push(Dir { path: path.into(), rank, last_accessed: now });
        self.dirty = true;
    }

    pub fn add_update(&mut self, path: impl AsRef<str> + Into<String>, by: Rank, now: Epoch) {
        let path_str = path.as_ref();
        match self.dirs.iter_mut().find(|dir| dir.path == path_str) {
            Some(dir) => {
                dir.rank = (dir.rank + by).max(0.0);
                dir.last_accessed = now;
            }
            None => {
                self.dirs.push(Dir { path: path.into(), rank: by.max(0.0), last_accessed: now })
            }
        }
        self.dirty = true;
    }

    pub fn remove(&mut self, path: impl AsRef<str>) -> bool {
        match self.dirs.iter().position(|dir| dir.path == path.as_ref()) {
            Some(idx) => {
                self.swap_remove(idx);
                true
            }
            None => false,
        }
    }

    pub fn swap_remove(&mut self, idx: usize) {
        self.dirs.swap_remove(idx);
        self.dirty = true;
    }

    pub fn age(&mut self, max_age: Rank) {
        let total_age = self.dirs.iter().map(|dir| dir.rank).sum::<Rank>();
        if total_age > max_age {
            let factor = 0.9 * max_age / total_age;
            for idx in (0..self.dirs.len()).rev() {
                let dir = &mut self.dirs[idx];
                dir.rank *= factor;
                if dir.rank < 1.0 {
                    self.dirs.swap_remove(idx);
                }
            }
            self.dirty = true;
        }
    }

    pub fn dedup(&mut self) {
        self.sort_by_path();

        for idx in (1..self.dirs.len()).rev() {
            if self.dirs[idx - 1].path != self.dirs[idx].path {
                continue;
            }
            let rank = self.dirs[idx].rank;
            let last_accessed = self.dirs[idx].last_accessed;
            self.dirs[idx - 1].last_accessed = self.dirs[idx - 1].last_accessed.max(last_accessed);
            self.dirs[idx - 1].rank += rank;
            self.dirs.swap_remove(idx);
            self.dirty = true;
        }
    }

    pub fn sort_by_path(&mut self) {
        self.dirs.sort_unstable_by(|dir1, dir2| dir1.path.cmp(&dir2.path));
        self.dirty = true;
    }

    pub fn sort_by_score(&mut self, now: Epoch) {
        self.dirs.sort_unstable_by(|dir1, dir2| dir1.score(now).total_cmp(&dir2.score(now)));
        self.dirty = true;
    }

    pub fn dirty(&self) -> bool {
        self.dirty
    }

    pub fn dirs(&self) -> &[Dir] {
        &self.dirs
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self { path: PathBuf::new(), dirs: Vec::new(), dirty: false }
    }

    fn serialize(dirs: &[Dir]) -> Result<Vec<u8>> {
        (|| -> bincode::Result<_> {
            let buffer_size =
                bincode::serialized_size(&Self::VERSION)? + bincode::serialized_size(&dirs)?;
            let mut buffer = Vec::with_capacity(buffer_size as usize);

            bincode::serialize_into(&mut buffer, &Self::VERSION)?;
            bincode::serialize_into(&mut buffer, &dirs)?;

            Ok(buffer)
        })()
        .context("could not serialize database")
    }

    fn deserialize(bytes: &[u8]) -> Result<Vec<Dir>> {
        const MAX_SIZE: u64 = 32 << 20;
        let deserializer = &mut bincode::options().with_fixint_encoding().with_limit(MAX_SIZE);

        let version_size = deserializer.serialized_size(&Self::VERSION).unwrap() as _;
        if bytes.len() < version_size {
            bail!("could not deserialize database: corrupted data");
        }
        let (bytes_version, bytes_dirs) = bytes.split_at(version_size);

        let version = deserializer.deserialize(bytes_version)?;
        let dirs = match version {
            Self::VERSION => {
                deserializer.deserialize(bytes_dirs).context("could not deserialize database")?
            }
            version => {
                bail!("unsupported version (got {version}, supports {})", Self::VERSION)
            }
        };

        Ok(dirs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add() {
        let data_dir = tempfile::tempdir().unwrap();
        let path = if cfg!(windows) { r"C:\foo\bar" } else { "/foo/bar" };
        let now = 946684800;

        {
            let mut db = Database::open_dir(data_dir.path()).unwrap();
            db.add(path, 1.0, now);
            db.add(path, 1.0, now);
            db.save().unwrap();
        }

        {
            let db = Database::open_dir(data_dir.path()).unwrap();
            assert_eq!(db.dirs().len(), 1);

            let dir = &db.dirs()[0];
            assert_eq!(dir.path, path);
            assert!((dir.rank - 2.0).abs() < 0.01);
            assert_eq!(dir.last_accessed, now);
        }
    }

    #[test]
    fn remove() {
        let data_dir = tempfile::tempdir().unwrap();
        let path = if cfg!(windows) { r"C:\foo\bar" } else { "/foo/bar" };
        let now = 946684800;

        {
            let mut db = Database::open_dir(data_dir.path()).unwrap();
            db.add(path, 1.0, now);
            db.save().unwrap();
        }

        {
            let mut db = Database::open_dir(data_dir.path()).unwrap();
            assert!(db.remove(path));
            db.save().unwrap();
        }

        {
            let mut db = Database::open_dir(data_dir.path()).unwrap();
            assert!(db.dirs().is_empty());
            assert!(!db.remove(path));
            db.save().unwrap();
        }
    }
}
