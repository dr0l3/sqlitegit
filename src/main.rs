#![feature(once_cell)]

extern crate core;

use std::panic;

use chrono::{DateTime, TimeZone, Utc};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
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
use tui::widgets::{Row, Table};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans, Text},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};

#[repr(C)]
struct GitCommit {
    base: sqlite3_vtab,
}

#[derive(FromPrimitive)]
enum GitCommitParams {
    NONE_PASSED = 0,
    REV_PASSED = 1,
    REPO_PASSED = 2,
    BOTH_PASSED = 3,
}

impl Into<c_int> for GitCommitParams {
    fn into(self) -> c_int {
        match self {
            GitCommitParams::NONE_PASSED => 0,
            GitCommitParams::REV_PASSED => 1,
            GitCommitParams::REPO_PASSED => 2,
            GitCommitParams::BOTH_PASSED => 3,
        }
    }
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
            &[a, b] if a == 11 && b == 12 => GitCommitParams::BOTH_PASSED,
            &[a] if a == 11 => GitCommitParams::REPO_PASSED,
            &[a] if a == 12 => GitCommitParams::REV_PASSED,
            &[] => GitCommitParams::NONE_PASSED,
            _ => GitCommitParams::NONE_PASSED,
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

#[repr(C)]
struct GitStats {
    base: sqlite3_vtab,
    repo: Repository,
    hash: String,
    map: HashMap<String, (u64, u64)>,
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

unsafe impl<'a> VTab<'a> for GitStats {
    type Aux = ();
    type Cursor = GitStatsCursor;

    fn connect(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, Self)> {
        let repo = Repository::open(".").map_err(to_sqlite_error)?;
        let mut revwalk = repo.revwalk().map_err(to_sqlite_error)?;
        revwalk.push_head().map_err(to_sqlite_error)?;
        let head_had = revwalk
            .next()
            .unwrap()
            .map_err(to_sqlite_error)?
            .to_string();
        Ok((
            "create table stats(file_name text, additions integer, deletions integer, hash hidden)"
                .to_string(),
            GitStats {
                base: sqlite3_vtab::default(),
                repo: Repository::open(".").map_err(to_sqlite_error)?,
                hash: head_had,
                map: Default::default(),
            },
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        let mut counter = 0;
        let usable_constraints = &info.constraints().filter(|con| con.is_usable()).count();

        (0..usable_constraints.to_i16().unwrap()).for_each(|_| {
            let mut usage = &mut info.constraint_usage(counter);
            usage.set_argv_index((counter + 1) as c_int);
            counter += 1;
        });

        Ok(())
    }

    fn open(&self) -> rusqlite::Result<GitStatsCursor> {
        Ok(GitStatsCursor {
            base: Default::default(),
            diffs: vec![],
            i: 0,
            hash: self.hash.to_string(),
            repo: Repository::open("/home/rdp/dixa/listing-service").unwrap(),
        })
    }
}

#[repr(C)]
struct GitStatsCursor {
    base: sqlite3_vtab_cursor,
    diffs: Vec<(String, u64, u64)>,
    i: usize,
    hash: String,
    repo: Repository,
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

impl GitStatsCursor {
    fn compute_diff(&self) -> Result<Vec<(String, u64, u64)>, CustomError> {
        let commit = self.repo.find_commit(Oid::from_str(&self.hash)?)?;
        let (tree, parent_tree) = match commit.parent_count() {
            1 => {
                let tree = self.repo.find_tree(commit.tree_id())?;
                let parent_tree = self.repo.find_tree(commit.parent(0)?.tree_id())?;
                (tree, parent_tree)
            }
            2 => {
                let tree = self.repo.find_tree(commit.parent(1)?.tree_id())?;
                let parent_tree = self.repo.find_tree(commit.parent(0)?.tree_id())?;
                (tree, parent_tree)
            }
            0 => {
                let tree = self.repo.find_tree(commit.tree_id())?;
                let tree2 = self.repo.find_tree(commit.tree_id())?;
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

        let diff =
            self.repo
                .diff_tree_to_tree(Some(&parent_tree), Some(&tree), Some(&mut diff_options));
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
        args.iter().for_each(|arg| {
            self.hash = arg.as_str().unwrap().to_string();
        });

        self.diffs = self.compute_diff().map_err(|e| e.to_sqlite_error())?;
        self.i = 0;
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
            3 => ctx.set_result(&self.hash.to_string()),
            _ => Ok(()),
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(1)
    }
}

/**
hash            text, 0
message         text, 1
author_name     text, 2
author_email    text, 3
author_when     DATETIME, 4
committer_name  text, 5
committer_email text, 6
committer_when  DATETIME, 7
is_merge        bool, 8
parent_1        text, 9
parent_2        text, 10
repository      hidden, 11
ref             hidden 12
 */
fn list_all_comits(db: &Connection) {
    let sql = r#"
    SELECT hash, message, author_when
    FROM commits('/home/rdp/dixa/listing-service')
    "#;
    let mut stmt = db.prepare(sql).unwrap();

    execute_and_pretty_print(&mut stmt);
}

fn execute_and_format(stmt: &mut Statement) -> Vec<String> {
    let col_count = stmt.column_count();
    let result_rows = stmt
        .query_map([], |row| {
            let mut row_array: Vec<String> = vec![];
            (0..col_count).for_each(|i| {
                let col_ref = row.get_ref_unwrap(i);
                match col_ref.data_type() {
                    Type::Null => {
                        row_array.push("NULL".to_string());
                        //row_str.push_str("NULL");
                    }
                    Type::Integer => {
                        row_array.push(col_ref.as_i64().unwrap().to_string());
                    }
                    Type::Real => {
                        row_array.push(col_ref.as_f64().unwrap().to_string());
                    }
                    Type::Text => {
                        row_array.push(col_ref.as_str().unwrap().to_string().lines().join(""));
                    }
                    Type::Blob => {
                        row_array.push(
                            String::from_utf8(Vec::from(col_ref.as_blob().unwrap())).unwrap(),
                        );
                    }
                };
            });
            Ok(row_array)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect_vec();

    let mut init = (0..col_count).map(|_| 0).collect_vec();
    let col_names = stmt
        .column_names()
        .iter()
        .map(|str| str.to_string())
        .collect_vec();
    let col_names_and_rows = [vec![col_names.to_owned()], result_rows.to_owned()].concat();
    let max_size = col_names_and_rows.iter().fold(init, |mut acc, vec| {
        (0..col_count).for_each(|i| {
            if acc[i] < vec[i].len() {
                acc[i] = std::cmp::min(vec[i].len(), 50)
            }
        });
        acc
    });

    let headers = {
        (0..col_count)
            .map(|(i)| {
                let max_size = max_size[i];
                let mut str: String = col_names[i].to_owned();
                let length = std::cmp::min(std::cmp::max(max_size, str.len()), 50);
                str.truncate(length);
                format!("{:width$}", str, width = length as usize)
            })
            .join(" | ")
    };

    let line = {
        let lenth =
            (0..col_count).fold(0, |acc, next| acc + max_size[next]) + 2 + (col_count * 3) - 1;
        format!(
            "{}",
            String::from((0..lenth).map(|_| '-').collect::<String>())
        )
    };

    let formatted_rows = result_rows
        .iter()
        .enumerate()
        .flat_map(|(i, row_vec)| {
            print!("| ");
            let cols = (0..col_count)
                .map(|(i)| {
                    let max_size = max_size[i];
                    let mut str: String = row_vec[i].to_owned();
                    let length = std::cmp::min(std::cmp::max(max_size, str.len()), 50);
                    str.truncate(length);
                    format!("{:width$}", str, width = length as usize)
                })
                .join(" | ");
            println!("");
            if i == 0 {
                let lenth =
                    (0..col_count).fold(0, |acc, next| acc + max_size[next]) + 2 + (col_count * 3)
                        - 1;
                println!(
                    "{}",
                    String::from((0..lenth).map(|_| '-').collect::<String>())
                );
            }

            let line = format!("|{}|", cols);

            vec![line]
        })
        .collect_vec();

    [vec![headers], vec![line], formatted_rows].concat()
}

fn execute_and_pretty_print(stmt: &mut Statement) {
    let col_count = stmt.column_count();
    let result_rows = stmt
        .query_map([], |row| {
            let mut row_array: Vec<String> = vec![];
            (0..col_count).for_each(|i| {
                let col_ref = row.get_ref_unwrap(i);
                match col_ref.data_type() {
                    Type::Null => {
                        row_array.push("NULL".to_string());
                        //row_str.push_str("NULL");
                    }
                    Type::Integer => {
                        row_array.push(col_ref.as_i64().unwrap().to_string());
                    }
                    Type::Real => {
                        row_array.push(col_ref.as_f64().unwrap().to_string());
                    }
                    Type::Text => {
                        row_array.push(col_ref.as_str().unwrap().to_string().lines().join(""));
                    }
                    Type::Blob => {
                        row_array.push(
                            String::from_utf8(Vec::from(col_ref.as_blob().unwrap())).unwrap(),
                        );
                    }
                };
            });
            Ok(row_array)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect_vec();

    let mut init = (0..col_count).map(|_| 0).collect_vec();
    let col_names = stmt
        .column_names()
        .iter()
        .map(|str| str.to_string())
        .collect_vec();
    let col_names_and_rows = [vec![col_names], result_rows].concat();
    let max_size = col_names_and_rows.iter().fold(init, |mut acc, vec| {
        (0..col_count).for_each(|i| {
            if acc[i] < vec[i].len() {
                acc[i] = std::cmp::min(vec[i].len(), 50)
            }
        });
        acc
    });

    col_names_and_rows
        .iter()
        .enumerate()
        .for_each(|(i, row_vec)| {
            print!("| ");
            (0..col_count).for_each(|(i)| {
                let max_size = max_size[i];
                let mut str: String = row_vec[i].to_owned();
                let length = std::cmp::min(std::cmp::max(max_size, str.len()), 50);
                str.truncate(length);
                print!("{}", format!("{:width$}", str, width = length as usize));
                print!(" | ");
            });
            println!("");
            if i == 0 {
                let lenth =
                    (0..col_count).fold(0, |acc, next| acc + max_size[next]) + 2 + (col_count * 3)
                        - 1;
                println!(
                    "{}",
                    String::from((0..lenth).map(|_| '-').collect::<String>())
                );
            }
        });

    //println!("{:#?}", wut);
}

fn list_commits_with_stats(db: &Connection) {
    let sql = r#"
    SELECT commits.hash, stats.file_name, SUM(stats.additions), SUM(stats.deletions)
    FROM commits('/home/rdp/dixa/listing-service') left outer join stats() on commits.hash = stats.hash
    WHERE commits.is_merge is true
    group by commits.hash, stats.file_name
    "#;
    let mut stmt = db.prepare(sql).unwrap();
    let start = std::time::Instant::now();
    execute_and_pretty_print(&mut stmt);

    //println!("{:#?}", iter.collect_vec());
}

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
