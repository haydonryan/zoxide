use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::str;

use anyhow::{Context, Result, anyhow, bail};

use crate::db::{Dir, Epoch};
use crate::import::{ImportError, Importer};

/// Parses a timestamp in `YYYY-MM-DD HH:MM:SS` format to a Unix epoch (u64).
fn parse_timestamp(s: &str) -> Result<u64> {
    let s = s.as_bytes();
    if s.len() != 19 || s[4] != b'-' || s[7] != b'-' || s[10] != b' ' || s[13] != b':' || s[16] != b':' {
        bail!("invalid timestamp format: expected YYYY-MM-DD HH:MM:SS");
    }
    let year  = (s[0] - b'0') as u64 * 1000 + (s[1] - b'0') as u64 * 100 + (s[2] - b'0') as u64 * 10 + (s[3] - b'0') as u64;
    let month = (s[5] - b'0') as u64 * 10 + (s[6] - b'0') as u64;
    let day   = (s[8] - b'0') as u64 * 10 + (s[9] - b'0') as u64;
    let hour  = (s[11] - b'0') as u64 * 10 + (s[12] - b'0') as u64;
    let min   = (s[14] - b'0') as u64 * 10 + (s[15] - b'0') as u64;
    let sec   = (s[17] - b'0') as u64 * 10 + (s[18] - b'0') as u64;

    if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || min > 59 || sec > 59 {
        bail!("invalid timestamp values");
    }

    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_month: [u64; 12] = [31, if is_leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if day > days_in_month[(month - 1) as usize] {
        bail!("invalid day for month");
    }

    let mut total_days = 0u64;
    for y in 1970..year {
        total_days += 365 + if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 1 } else { 0 };
    }
    for m in 1..month {
        let mi = (m - 1) as usize;
        total_days += days_in_month[mi];
    }
    total_days += day - 1;

    Ok(total_days * 86400 + hour * 3600 + min * 60 + sec)
}

#[derive(clap::Args, Clone, Debug)]
pub(crate) struct Atuin {}

impl Importer for Atuin {
    fn dirs(&self) -> Result<impl Iterator<Item = Result<Dir, ImportError>>> {
        // atuin renders `{time}` as `YYYY-MM-DD HH:MM:SS` in UTC.
        let mut child = Command::new("atuin")
            .args(["history", "list", "--format={time}\t{directory}", "--print0"])
            .stdout(Stdio::piped())
            .spawn()
            .context("failed to run `atuin`; is it installed and on PATH?")?;
        let stdout = child.stdout.take().expect("stdout piped");
        let reader = BufReader::new(stdout);
        Ok(Iter::new(reader, child))
    }
}

/// Iterates atuin's NUL-separated `{time}\t{directory}` records, emitting one
/// `Dir` per directory transition (consecutive same-path records collapse).
/// Owns the `Child` handle so the subprocess is reaped on Drop.
struct Iter {
    reader: BufReader<ChildStdout>,
    buf: Vec<u8>,
    line_num: usize,

    child: Child,
    prev_cwd: Option<String>,
}

impl Iter {
    fn new(reader: BufReader<ChildStdout>, child: Child) -> Self {
        Self { reader, buf: Vec::new(), line_num: 0, child, prev_cwd: None }
    }

    fn err(&self, source: anyhow::Error) -> ImportError {
        ImportError { path: None, line_num: self.line_num, source }
    }

    fn parse_line(&self, line: &[u8]) -> Result<Dir, ImportError> {
        let line =
            str::from_utf8(line).map_err(|e| self.err(anyhow!(e).context("invalid utf-8")))?;

        let (timestamp, path) =
            line.split_once('\t').ok_or_else(|| self.err(anyhow!("invalid entry: {line}")))?;

        let timestamp = parse_timestamp(timestamp)
            .map_err(|e| self.err(e.context(format!("invalid timestamp: {timestamp:?}"))))?;

        let dir = Dir {
            path: path.to_string(),
            rank: 1.0,
            last_accessed: timestamp as Epoch,
        };
        Ok(dir)
    }
}

impl Iterator for Iter {
    type Item = Result<Dir, ImportError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buf.clear();
            self.line_num += 1;

            match self.reader.read_until(b'\0', &mut self.buf) {
                Ok(0) => return None,
                Ok(_) => {
                    if self.buf.last() == Some(&b'\0') {
                        self.buf.pop();
                    }
                    if self.buf.is_empty() {
                        continue;
                    }

                    let result = self.parse_line(&self.buf);
                    match &result {
                        Ok(dir) => {
                            let path = dir.path.as_ref();
                            if self.prev_cwd.as_deref() == Some(path) {
                                continue; // dedup consecutive same-path entries
                            }
                            self.prev_cwd = Some(path.to_string());
                            return Some(result);
                        }
                        Err(_) => return Some(result),
                    }
                }
                Err(e) => {
                    return Some(Err(self.err(anyhow!(e).context("could not read from atuin"))));
                }
            }
        }
    }
}

impl Drop for Iter {
    fn drop(&mut self) {
        _ = self.child.kill();
        _ = self.child.wait();
    }
}
