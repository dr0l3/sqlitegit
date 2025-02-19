# GitQLite 

**GitQLite** bridges the gap between Git repositories and SQL by exposing Git data through SQLite virtual tables. Query your Git repositories with familiar SQL syntax to extract insights, track changes, and analyze commit histories seamlessly.

⚠️ This project is neither polished, functional nor maintained!

## 🚀 Features

- **SQLite Virtual Tables**: Query Git repositories as if they were SQL tables.
  - `commits`: Access Git commit metadata.
  - `merges`: Focused view of merge commits.
  - `stats`: Analyze file-level changes (additions/deletions).

- **Powerful Queries**: Combine commit data with file stats, filter specific files, and analyze merges directly with SQL.

## 🛠️ Usage

### Querying Commits

```sql
SELECT hash, message, author_when
FROM commits('./path/to/repo')
ORDER BY author_when ASC;
```

### Querying File Stats

```sql
SELECT file_name, additions, deletions
FROM stats('./path/to/repo', 'commit_hash');
```

### Combining Commits and Stats

```sql
SELECT c.hash, c.message, c.author_when, s.file_name, s.additions, s.deletions
FROM commits('./path/to/repo') c
LEFT JOIN stats('./path/to/repo') s ON c.hash = s.hash
ORDER BY author_when DESC;
```

### Analyzing Merges

```sql
SELECT author_email, COUNT(m.hash) AS merges, AVG(time_to_merge/3600) AS ttm, 
       SUM(COALESCE(additions, 0)) AS additions, SUM(COALESCE(deletions, 0)) AS deletions
FROM merges('./path/to/repo') m
LEFT JOIN stats('./path/to/repo') s ON m.hash = s.hash
GROUP BY author_email
ORDER BY ttm ASC;
```

## ✅ Current Capabilities

- Query commits, merges, and file stats.
- Join commit data with file-level changes.
- Filter commits and stats based on file names and authors.
- Analyze merge commit patterns and calculate time-to-merge.

## ⚠️ Limitations

Many!