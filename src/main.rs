#![feature(once_cell)]

mod utils;

extern crate core;

use std::panic;

use crate::utils::list_commits_with_stats;
use chrono::{DateTime, TimeZone, Utc};
use git2::{
    Branch, BranchType, Commit, Delta, Deltas, DescribeOptions, Diff, DiffDelta, DiffHunk,
    DiffLine, DiffLineType, DiffOptions, Error, Oid, ReflogEntry, Repository, Revwalk, Sort,
};
use itertools::Itertools;
use num_derive::FromPrimitive;
use num_traits::cast::ToPrimitive;
use num_traits::FromPrimitive;
use rusqlite::types::{Type, ValueRef};
use rusqlite::vtab::{
    eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor, Context, IndexInfo, VTab,
    VTabConnection, VTabCursor, Values,
};
use rusqlite::{Column, Connection, ErrorCode, Statement};
use std::any::Any;
use std::cell::{Cell, RefMut};
use std::collections::HashMap;
use std::fmt::{format, Debug, Display, Formatter, Write};
use std::io;
use std::lazy::OnceCell;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::os::raw::c_int;
use std::ptr::null;
use std::sync::Arc;

//  Shared -------------------------------------------------------------------------------------------------

#[derive(FromPrimitive)]
enum RepoRevParam {
    NONE_PASSED = 0,
    REV_PASSED = 1,
    REPO_PASSED = 2,
    BOTH_PASSED = 3,
}

impl Into<c_int> for RepoRevParam {
    fn into(self) -> c_int {
        match self {
            RepoRevParam::NONE_PASSED => 0,
            RepoRevParam::REV_PASSED => 1,
            RepoRevParam::REPO_PASSED => 2,
            RepoRevParam::BOTH_PASSED => 3,
        }
    }
}

#[derive(Debug)]
enum CustomError {
    git(git2::Error),
    sqlite(rusqlite::Error),
}

impl CustomError {
    fn to_sqlite_error(self) -> rusqlite::Error {
        match self {
            CustomError::git(g) => rusqlite::Error::ModuleError(g.message().to_string()),
            CustomError::sqlite(s) => s,
        }
    }
}

impl Into<rusqlite::Error> for CustomError {
    fn into(self) -> rusqlite::Error {
        match self {
            CustomError::git(g) => rusqlite::Error::ModuleError(g.message().to_string()),
            CustomError::sqlite(s) => s,
        }
    }
}

impl From<rusqlite::Error> for CustomError {
    fn from(e: rusqlite::Error) -> Self {
        CustomError::sqlite(e)
    }
}

impl From<git2::Error> for CustomError {
    fn from(e: Error) -> Self {
        CustomError::git(e)
    }
}

fn print_index_info(info: &mut IndexInfo) {
    println!("-- INDEX INFO --");
    for x in info.constraints() {
        println!("is_usable: {:#?}", x.is_usable());
        println!("operator: {:#?}", x.operator());
        println!("column: {:#?}", x.column());
    }
    println!("-- END OF INDEX INFO --");
}

fn to_sqlite_error(git_error: git2::Error) -> rusqlite::Error {
    rusqlite::Error::ModuleError(git_error.message().to_string())
}

// COmmits --------------------------------------------------------------------------------------------------

#[repr(C)]
struct GitCommit {
    base: sqlite3_vtab,
}

unsafe impl<'a> VTab<'a> for GitCommit {
    type Aux = ();
    type Cursor = GitCommitCursor;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let sql = r#"
        create table commits (
            hash            text,
            message         text,
            author_name     text,
            author_email    text,
            author_when     DATETIME,
            committer_name  text,
            committer_email text,
            committer_when  DATETIME,
            is_merge        bool,
            parent_1        text,
            parent_2        text,
            repository      hidden,
            ref             hidden
        )
        "#;
        Ok((
            sql.to_owned(),
            GitCommit {
                base: sqlite3_vtab::default(),
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        print_index_info(info);
        let mut counter = 0;
        let mut used_cols = info
            .constraints()
            .filter(|con| con.is_usable())
            .map(|con| con.column())
            .collect_vec();

        (0..used_cols.len()).for_each(|_| {
            let mut usage = &mut info.constraint_usage(counter);
            usage.set_argv_index((counter + 1) as c_int);
            counter += 1;
        });

        used_cols.sort();
        let index_num = match &used_cols[..] {
            &[a, b] if a == 11 && b == 12 => RepoRevParam::BOTH_PASSED,
            &[a] if a == 11 => RepoRevParam::REPO_PASSED,
            &[a] if a == 12 => RepoRevParam::REV_PASSED,
            &[] => RepoRevParam::NONE_PASSED,
            _ => RepoRevParam::NONE_PASSED,
        };

        info.set_idx_num(index_num.into());

        Ok(())
    }

    fn open(&self) -> rusqlite::Result<GitCommitCursor> {
        Ok(GitCommitCursor {
            base: sqlite3_vtab_cursor::default(),
            rev_param: None,
            repo_param: None,
            repo: OnceCell::new(),
            walk: vec![],
            i: 0,
        })
    }
}

#[derive(Debug)]
struct CommitShadow {
    hash: String,
    message: Option<String>,
    author_name: Option<String>,
    author_email: Option<String>,
    author_when: DateTime<Utc>,
    committer_name: Option<String>,
    committer_email: Option<String>,
    committer_when: DateTime<Utc>,
    is_merge: bool,
    parent_1: Option<String>,
    parent_2: Option<String>,
}

impl From<Commit<'_>> for CommitShadow {
    fn from(c: Commit) -> Self {
        CommitShadow {
            hash: c.id().to_string(),
            message: c.message().map(|msg| msg.to_string()),
            author_name: c.author().name().map(|msg| msg.to_string()),
            author_email: c.author().email().map(|msg| msg.to_string()),
            author_when: Utc.timestamp(c.author().when().seconds(), 0),
            committer_name: c.committer().name().map(|msg| msg.to_string()),
            committer_email: c.committer().email().map(|msg| msg.to_string()),
            committer_when: Utc.timestamp(c.committer().when().seconds(), 0),
            is_merge: c.parent_count() == 2,
            parent_1: c.parent(0).ok().map(|parent| parent.id().to_string()),
            parent_2: c.parent(1).ok().map(|parent| parent.id().to_string()),
        }
    }
}

#[repr(C)]
struct GitCommitCursor {
    base: sqlite3_vtab_cursor,
    rev_param: Option<String>,
    repo_param: Option<String>,
    repo: OnceCell<Repository>,
    walk: Vec<CommitShadow>,
    i: usize,
}

impl GitCommitCursor {
    fn init(&mut self, idx_num: c_int, vals: Vec<ValueRef>) -> Result<(), CustomError> {
        match idx_num {
            0 => {
                self.repo_param = None;
                self.rev_param = None;
                self.repo.set(Repository::open(".")?);
                let mut walk = self.repo.get().unwrap().revwalk()?;
                walk.push_head()?;

                self.walk = walk
                    .map(|oid| self.repo.get().unwrap().find_commit(oid?))
                    .map(|c| c.unwrap().into())
                    .collect();
                self.i = 0;
                Ok(())
            }
            1 => {
                self.repo_param = None;
                self.rev_param = vals
                    .first()
                    .and_then(|v| v.as_str().ok())
                    .map(|v| v.to_string());
                self.repo.set(Repository::open(".").unwrap());
                let commit_oid = Oid::from_str(&self.rev_param.as_ref().unwrap())?;
                let mut walk = self.repo.get().unwrap().revwalk()?;
                walk.push(commit_oid)?;
                self.walk = walk
                    .map_ok(|oid| self.repo.get().unwrap().find_commit(oid).unwrap())
                    .filter_map(|c| c.ok())
                    .map(|c| c.into())
                    .collect();
                Ok(())
            }
            2 => {
                let repo_path = vals
                    .first()
                    .and_then(|v| v.as_str().ok())
                    .map(|v| v.to_string())
                    .unwrap();
                self.repo_param = Some(repo_path.to_owned());
                self.rev_param = None;
                self.repo.set(Repository::open(&repo_path)?);
                let mut walk = self.repo.get().unwrap().revwalk()?;
                walk.push_head()?;
                self.walk = walk
                    .map_ok(|oid| self.repo.get().unwrap().find_commit(oid))
                    .filter_map(|c| c.ok().and_then(|c| c.ok()))
                    .map(|c| c.into())
                    .collect();
                Ok(())
            }
            3 => {
                let repo_path = vals
                    .first()
                    .and_then(|v| v.as_str().ok())
                    .map(|v| v.to_string())
                    .unwrap();
                self.repo_param = vals.get(0).map(|v| v.as_str().unwrap().to_string());
                self.rev_param = vals.get(1).map(|v| v.as_str().unwrap().to_string());
                self.repo
                    .set(Repository::open(&repo_path)?)
                    .map_err(|_| rusqlite::Error::ModuleError("unable to set repo".to_string()))?;
                let commit_oid = Oid::from_str(&self.rev_param.as_ref().unwrap())?;
                let mut walk = self.repo.get().unwrap().revwalk()?;
                walk.push(commit_oid)?;
                self.walk = walk
                    .map_ok(|oid| self.repo.get().unwrap().find_commit(oid))
                    .filter_map(|c| c.ok().and_then(|c| c.ok()))
                    .map(|c| c.into())
                    .collect();
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

unsafe impl VTabCursor for GitCommitCursor {
    fn filter(
        &mut self,
        idx_num: c_int,
        idx_str: Option<&str>,
        args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        let vals = args.iter().collect_vec();
        self.init(idx_num, vals).map_err(|e| e.to_sqlite_error())?;

        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.i = self.i + 1;

        Ok(())
    }

    fn eof(&self) -> bool {
        match self.rev_param {
            None => self.i >= self.walk.len(),
            Some(_) => self.i > 0,
        }
    }

    /*
    create table commits (
            hash            text,
            message         text,
            author_name     text,
            author_email    text,
            author_when     DATETIME,
            committer_name  text,
            committer_email text,
            committer_when  DATETIME,
            is_merge        bool,
            parent_1        text,
            parent_2        text,
            repository      hidden,
            ref             hidden
        ) WITHOUT ROWID

     */
    fn column(&self, ctx: &mut Context, i: c_int) -> rusqlite::Result<()> {
        let current_commit = &self.walk[self.i];
        match i {
            0 => ctx.set_result(&current_commit.hash),
            1 => ctx.set_result(&current_commit.message),
            2 => ctx.set_result(&current_commit.author_name),
            3 => ctx.set_result(&current_commit.author_email),
            4 => ctx.set_result(&current_commit.author_when),
            5 => ctx.set_result(&current_commit.committer_name),
            6 => ctx.set_result(&current_commit.committer_email),
            7 => ctx.set_result(&current_commit.committer_when),
            8 => ctx.set_result(&current_commit.is_merge),
            9 => ctx.set_result(&current_commit.parent_1),
            10 => ctx.set_result(&current_commit.parent_2),
            11 => ctx.set_result(&self.repo_param),
            12 => ctx.set_result(&self.rev_param),
            _ => Ok(()),
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(1)
    }
}

//  STATS ------------------------------------------------------------------------------------------------

#[repr(C)]
struct GitStats {
    base: sqlite3_vtab,
}

unsafe impl<'a> VTab<'a> for GitStats {
    type Aux = ();
    type Cursor = GitStatsCursor;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        Ok((
            "create table stats(file_name text, additions integer, deletions integer, hash hidden, repo hidden)"
                .to_string(),
            GitStats {
                base: sqlite3_vtab::default(),
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        print_index_info(info);
        let mut counter = 0;
        let mut used_cols = info
            .constraints()
            .filter(|con| con.is_usable())
            .map(|con| con.column())
            .collect_vec();

        (0..used_cols.len()).for_each(|_| {
            let mut usage = &mut info.constraint_usage(counter);
            usage.set_argv_index((counter + 1) as c_int);
            counter += 1;
        });

        used_cols.sort();
        let index_num = match &used_cols[..] {
            &[a, b] if a == 3 && b == 4 => RepoRevParam::BOTH_PASSED,
            &[a] if a == 3 => RepoRevParam::REPO_PASSED,
            &[a] if a == 4 => RepoRevParam::REV_PASSED,
            &[] => RepoRevParam::NONE_PASSED,
            _ => RepoRevParam::NONE_PASSED,
        };

        info.set_idx_num(index_num.into());

        Ok(())
    }

    fn open(&self) -> rusqlite::Result<GitStatsCursor> {
        Ok(GitStatsCursor {
            base: Default::default(),
            diffs: vec![],
            i: 0,
            hash: "".to_string(),
            repo: OnceCell::new(),
            repo_param: None,
            rev_param: None,
        })
    }
}

#[repr(C)]
struct GitStatsCursor {
    base: sqlite3_vtab_cursor,
    diffs: Vec<(String, u64, u64)>,
    i: usize,
    hash: String,
    repo: OnceCell<Repository>,
    repo_param: Option<String>,
    rev_param: Option<String>,
}

impl Debug for GitStatsCursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str = format!(
            "GitStatsCursor {{ \n  diffs: {:#?},\n  i: {:#?},\n  hash: {:#?}\n}}",
            self.diffs, self.i, self.hash
        );
        f.write_str(&str)
    }
}

impl GitStatsCursor {
    fn compute_diff(&self) -> Result<Vec<(String, u64, u64)>, CustomError> {
        let commit = self
            .repo
            .get()
            .unwrap()
            .find_commit(Oid::from_str(&self.hash)?)?;
        println!("{:#?}", commit);
        let (tree, parent_tree) = match commit.parent_count() {
            1 => {
                let tree = self.repo.get().unwrap().find_tree(commit.tree_id())?;
                let parent_tree = self
                    .repo
                    .get()
                    .unwrap()
                    .find_tree(commit.parent(0)?.tree_id())?;
                (tree, parent_tree)
            }
            2 => {
                let tree = self
                    .repo
                    .get()
                    .unwrap()
                    .find_tree(commit.parent(1)?.tree_id())?;
                let parent_tree = self
                    .repo
                    .get()
                    .unwrap()
                    .find_tree(commit.parent(0)?.tree_id())?;
                (tree, parent_tree)
            }
            0 => {
                let tree = self.repo.get().unwrap().find_tree(commit.tree_id())?;
                let tree2 = self.repo.get().unwrap().find_tree(commit.tree_id())?;
                (tree, tree2)
            }
            _ => {
                panic!("Commit has more than 2 parents")
            }
        };
        let mut diff_options = DiffOptions::new();

        diff_options
            .ignore_blank_lines(true)
            .ignore_filemode(true)
            .context_lines(0)
            .ignore_whitespace(true)
            .ignore_submodules(true)
            .ignore_whitespace_eol(true)
            .ignore_whitespace_change(true);

        let diff = self.repo.get().unwrap().diff_tree_to_tree(
            Some(&parent_tree),
            Some(&tree),
            Some(&mut diff_options),
        );
        let mut map: HashMap<String, (u64, u64)> = HashMap::new();
        let mut line_cb =
            |diff_delta: DiffDelta, _: Option<DiffHunk>, line_dif: DiffLine| -> bool {
                let file_name = diff_delta
                    .new_file()
                    .path()
                    .and_then(|path| path.to_str())
                    .unwrap()
                    .to_string();
                match line_dif.origin_value() {
                    DiffLineType::Addition => {
                        match map.get(&file_name.to_owned()) {
                            None => map.insert(file_name.to_owned(), (1, 0)),
                            Some(entry) => map.insert(file_name.to_owned(), (entry.0 + 1, entry.1)),
                        };
                    }
                    DiffLineType::Deletion => {
                        match map.get(&file_name.to_owned()) {
                            None => map.insert(file_name.to_owned(), (0, 1)),
                            Some(entry) => map.insert(file_name.to_owned(), (entry.0, entry.1 + 1)),
                        };
                    }
                    _ => {}
                };
                true
            };
        diff.unwrap()
            .foreach(
                &mut |delta, n| true,
                None,
                Some(&mut |a, b| true),
                Some(&mut line_cb),
            )
            .unwrap();
        //println!("Map after foreach{:#?}",map);
        //println!("Vector after foreach: {:#?}",wut);
        Ok(map
            .iter()
            .map(|(k, v)| (k.to_string(), v.0, v.1))
            .collect_vec())
    }

    fn print_if(&self, function_name: &str) {
        // let predicate = self.hash == "f7d8eb622db00faf916e3002c3f555c84dfe9c97";
        let predicate = false;
        if predicate {
            println!("{} called with state: {:#?}", function_name, &self);
        } else {
            ()
        }
    }
}

unsafe impl VTabCursor for GitStatsCursor {
    fn filter(
        &mut self,
        idx_num: c_int,
        idx_str: Option<&str>,
        args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        let vals = args
            .iter()
            .map(|value_ref| value_ref.as_str().unwrap())
            .collect_vec();
        println!("{:#?}", vals);
        match idx_num {
            0 => {
                self.repo_param = None;
                self.rev_param = None;
                self.repo.set(Repository::open(".").unwrap());
                self.hash = self
                    .repo
                    .get()
                    .unwrap()
                    .head()
                    .unwrap()
                    .target()
                    .unwrap()
                    .to_string();
                self.i = 0;
            }
            1 => {
                self.repo_param = None;
                self.rev_param = vals.first().map(|v| v.to_string());
                self.hash = self.rev_param.as_ref().unwrap().to_string();
                self.repo.set(Repository::open(".").unwrap());
            }
            2 => {
                let repo_path = vals.first().map(|v| v.to_string()).unwrap();
                self.repo_param = Some(repo_path.to_owned());
                self.rev_param = None;
                self.repo.set(Repository::open(&repo_path).unwrap());
                self.hash = self
                    .repo
                    .get()
                    .unwrap()
                    .head()
                    .unwrap()
                    .target()
                    .unwrap()
                    .to_string();
            }
            3 => {
                let repo_path = vals.first().map(|v| v.to_string()).unwrap();
                self.repo_param = vals.get(0).map(|v| v.to_string());
                self.rev_param = vals.get(1).map(|v| v.to_string());
                self.repo
                    .set(Repository::open(&repo_path).unwrap())
                    .map_err(|_| rusqlite::Error::ModuleError("unable to set repo".to_string()))?;
                self.hash = self.rev_param.as_ref().unwrap().to_string();
            }
            _ => (),
        }
        self.diffs = self.compute_diff().unwrap();
        println!("{:#?} {:#?}", self.rev_param, self.repo_param);
        println!("{:#?}", self.diffs);
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.i = self.i + 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.i >= self.diffs.len()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> rusqlite::Result<()> {
        let (filename, additions, deletions) = &self.diffs[self.i];
        match i {
            0 => ctx.set_result(filename),
            1 => ctx.set_result(additions),
            2 => ctx.set_result(deletions),
            3 => ctx.set_result(&self.repo_param.as_ref().unwrap()),
            4 => ctx.set_result(&self.rev_param.as_ref().unwrap()),
            _ => Ok(()),
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(1)
    }
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() -> std::io::Result<()> {
    let db = Connection::open_in_memory().unwrap();
    let commit_module = eponymous_only_module::<GitCommit>();
    let stat_module = eponymous_only_module::<GitStats>();

    db.create_module("commits", commit_module, None).unwrap();
    db.create_module("stats", stat_module, None).unwrap();

    // list_all_comits(&db);
    list_commits_with_stats(&db);
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::{GitCommit, GitStats};
    use chrono::{DateTime, TimeZone, Utc};
    use itertools::assert_equal;
    use rusqlite::vtab::eponymous_only_module;
    use rusqlite::Connection;

    #[test]
    fn commits() -> Result<(), rusqlite::Error> {
        let db = Connection::open_in_memory().unwrap();
        let commit_module = eponymous_only_module::<GitCommit>();
        db.create_module("commits", commit_module, None).unwrap();

        let sql = r#"
    SELECT hash, message, author_when
    FROM commits('./tests') ORDER BY author_when ASC;
    "#;
        let mut stmt = db.prepare(sql)?;
        let mut query_res = stmt.query([])?;
        let row = query_res.next()?.unwrap();

        let hash: String = row.get(0).unwrap();
        let msg: String = row.get(1).unwrap();
        let when: DateTime<Utc> = row.get(2).unwrap();

        assert_eq!(
            hash,
            String::from("6bf8ee6cd03eac57b7039756edc58c4aed6f6882")
        );
        assert_eq!(msg, "First commit\n");
        assert_eq!(when, Utc.ymd(2022, 7, 1).and_hms(17, 55, 57));

        Ok(())
    }

    #[test]
    fn stats() -> Result<(), rusqlite::Error> {
        let db = Connection::open_in_memory().unwrap();
        let stat_module = eponymous_only_module::<GitStats>();
        db.create_module("stats", stat_module, None).unwrap();

        let sql = r#"SELECT file_name, additions, deletions FROM stats('./tests', '9096bf0343aecaa4a592da68c10874fd9fe35918')"#;
        let mut stmt = db.prepare(sql)?;
        let mut query_res = stmt.query([])?;
        let row = query_res.next()?.unwrap();

        let filename: String = row.get(0).unwrap();
        let additions: i64 = row.get(1).unwrap();
        let deletions: i64 = row.get(2).unwrap();

        assert_eq!(filename, String::from("hello.txt"));
        assert_eq!(additions, 1);
        assert_eq!(deletions, 0);

        Ok(())
    }

    #[test]
    fn combined() -> Result<(), rusqlite::Error> {
        let db = Connection::open_in_memory().unwrap();
        let commit_module = eponymous_only_module::<GitCommit>();
        let stat_module = eponymous_only_module::<GitStats>();
        db.create_module("commits", commit_module, None).unwrap();
        db.create_module("stats", stat_module, None).unwrap();

        let sql = r#"SELECT hash, message, author_when, file_name, additions, deletions FROM commits('./tests') JOIN stats('./tests', '9096bf0343aecaa4a592da68c10874fd9fe35918') ON hash = commit_hash ORDER BY author_when ASC"#;
        let mut stmt = db.prepare(sql)?;
        let mut query_res = stmt.query([])?;
        let row = query_res.next()?.unwrap();

        let hash: String = row.get(0).unwrap();
        let msg: String = row.get(1).unwrap();
        let when: DateTime<Utc> = row.get(2).unwrap();
        let filename: String = row.get(3).unwrap();
        let additions: i64 = row.get(4).unwrap();
        let deletions: i64 = row.get(5).unwrap();

        assert_eq!(
            hash,
            String::from("6bf8ee6cd03eac57b7039756edc58c4aed6f6882")
        );
        assert_eq!(msg, "First commit\n");
        assert_eq!(when, Utc.ymd(2022, 7, 1).and_hms(17, 55, 57));
        assert_eq!(filename, String::from("hello.txt"));
        assert_eq!(additions, 1);
        assert_eq!(deletions, 0);

        Ok(())
    }
}
