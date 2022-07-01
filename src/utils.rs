use itertools::Itertools;
use rusqlite::types::Type;
use rusqlite::{Connection, Statement};

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
pub fn list_all_comits(db: &Connection) {
    let sql = r#"
    SELECT hash, message, author_when
    FROM commits('/home/rdp/dixa/listing-service')
    "#;
    let mut stmt = db.prepare(sql).unwrap();

    execute_and_pretty_print(&mut stmt);
}

pub fn execute_and_format(stmt: &mut Statement) -> Vec<String> {
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
            .map(|i| {
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

pub fn execute_and_pretty_print(stmt: &mut Statement) {
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
            (0..col_count).for_each(|i| {
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

pub fn list_commits_with_stats(db: &Connection) {
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
