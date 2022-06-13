use std::os::raw::c_int;
use git2::{BranchType, Commit, ReflogEntry, Repository, Oid};
use rusqlite::Connection;
use rusqlite::vtab::{Context, IndexInfo, Values, VTab, VTabConnection, VTabCursor, sqlite3_vtab, sqlite3_vtab_cursor, eponymous_only_module};

#[repr(C)]
struct Git {
    base: sqlite3_vtab,
    repo: Repository
}

unsafe impl<'a> VTab<'a> for Git {
    type Aux = ();
    type Cursor = GitCursor<'a>;

    fn connect(db: &mut VTabConnection, aux: Option<&Self::Aux>, args: &[&[u8]]) -> rusqlite::Result<(String, Self)> {
        Ok(("create table x(time, parent_1, parent_2, body, message, summary)".to_owned(), Git {
            base: sqlite3_vtab::default(),
            repo: Repository::open("/home/rdp/dixa/listing-service").unwrap()
        }))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        Ok(())
    }

    fn open<'vtab>(&'vtab self) -> rusqlite::Result<GitCursor<'vtab>> {
        let repo = &self.repo;
        let mut iter = repo.revwalk().unwrap().into_iter();
        let id = iter.next().unwrap().unwrap();
        let first_commit = repo.find_commit(id).unwrap();

        Ok(GitCursor {
            base: sqlite3_vtab_cursor::default(),
            repo: repo,
            current_commit: first_commit
        })
    }
}

#[repr(C)]
struct GitCursor<'a> {
    base: sqlite3_vtab_cursor,
    repo: &'a Repository,
    current_commit: Commit<'a>
}

unsafe impl VTabCursor for GitCursor<'_> {
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values<'_>) -> rusqlite::Result<()> {
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.current_commit = self.current_commit.parents().next().unwrap();
        Ok(())
    }

    fn eof(&self) -> bool {
        self.current_commit.parents().next().is_none()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> rusqlite::Result<()> {
        match i {
            0  => {
                ctx.set_result(&self.current_commit.time().seconds());
            },
            1 => {
                ctx.set_result(&self.current_commit.parent_id(0).unwrap().to_string());
            },
            2 => {
                ctx.set_result(&self.current_commit.parent_id(1).unwrap().to_string());
            },
            3 => {
                ctx.set_result(&self.current_commit.body().unwrap());
            },
            4 => {
                ctx.set_result(&self.current_commit.message().unwrap());
            },
            5 =>  {
                ctx.set_result(&self.current_commit.summary().unwrap());
            }
            _ => {}
        }
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(1)
    }
}

#[cfg(test)]
mod test {
    use std::marker::PhantomData;
    use std::os::raw::c_int;
    use git2::{Commit, Repository};
    use rusqlite::{Connection, Error, Row};
    use rusqlite::vtab::{Context, IndexInfo, Values, VTab, VTabConnection, VTabCursor, eponymous_only_module, sqlite3_vtab, sqlite3_vtab_cursor};
    use crate::Git;

    #[repr(C)]
    struct Hello {
        base: sqlite3_vtab,
    }

    unsafe impl<'vtab> VTab<'vtab> for Hello {
        type Aux = ();
        type Cursor = HelloCursor<'vtab>;

        fn connect(db: &mut VTabConnection, aux: Option<&Self::Aux>, args: &[&[u8]]) -> rusqlite::Result<(String, Self)> {
            let vtab = Hello {
                base: sqlite3_vtab::default()
            };
            Ok(("create table x(value)".to_owned(), vtab))
        }

        fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
            info.set_estimated_cost(1.);
            Ok(())
        }

        fn open(&'vtab self) -> rusqlite::Result<Self::Cursor> {
            Ok(HelloCursor::default())
        }
    }

    #[derive(Default, Debug)]
    #[repr(C)]
    struct HelloCursor<'vtab> {
        base: sqlite3_vtab_cursor,
        row_id: i64,
        phantom: PhantomData<&'vtab Hello>
    }

    unsafe impl VTabCursor for HelloCursor<'_> {
        fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Values<'_>) -> rusqlite::Result<()> {
            for arg in args {
                println!("{:#?}",arg);
            }
            println!("filter params {:#?} {:#?}",idx_num, idx_str);
            println!("calling filter with {:#?}",self);
            Ok(())
        }

        fn next(&mut self) -> rusqlite::Result<()> {
            println!("Calling next with {:#?}",self);
            self.row_id += 1;
            Ok(())
        }

        fn eof(&self) -> bool {
            println!("Calling eof {:#?}",self);
            self.row_id > 10
        }

        fn column(&self, ctx: &mut Context, i: c_int) -> rusqlite::Result<()> {
            println!("Calling column with {:#?}",self);
            ctx.set_result(&self.row_id)
        }

        fn rowid(&self) -> rusqlite::Result<i64> {
            println!("Calling rowid with {:#?}",self);
            Ok(self.row_id)
        }
    }



    #[test]
    fn something() {
        let db = Connection::open_in_memory().unwrap();
        let module = eponymous_only_module::<Hello>();

        db.create_module("hello", module, None).unwrap();
        let mut stmt = db.prepare("select value from hello() where value > 5").unwrap();
        let iter = stmt.query_map([], |row| {
            Ok(row.get::<_, i64>(0).unwrap())
        }).unwrap();

        let wut: Vec<i64> = iter.map(|v|v.unwrap()).collect();

        println!("{:#?}",wut);


    }

}

fn main() {
    /*let db = Connection::open_in_memory().unwrap();
    let module = eponymous_only_module::<Git>();

    db.create_module("git", module, None).unwrap();
    let mut stmt = db.prepare("select * from git()").unwrap();
    let iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0).unwrap(),
            row.get::<_, String>(1).unwrap(),
            row.get::<_, String>(2).unwrap(),
            row.get::<_, String>(3).unwrap(),
            row.get::<_, String>(4).unwrap(),
            row.get::<_, String>(5).unwrap(),
        ))
    }).unwrap();

    let wut: Vec<(i64, String, String, String, String, String)> = iter.map(|v|v.unwrap()).collect();

    println!("{:#?}",wut);*/

    let repo = Repository::open(".").unwrap();
    println!("{:#?}",repo.message());

    let revwalk = repo.revwalk().unwrap();

    revwalk.for_each(|rev_entry| {
        let commit = repo.find_commit(rev_entry.unwrap()).unwrap();

        println!("{:#?}",commit);
    });
}
