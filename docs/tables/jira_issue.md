# Table: jira_issue

**Issues** are the building blocks of any Jira project. An issue could represent a story, a bug, a task, or another issue type in your project.

## Examples

### Basic info

```sql
select
  key,
  project_key,
  created,
  creator_display_name,
  status,
  summary
from
  jira_issue;
```

### List issues for a specific project

```sql
select
  id,
  key,
  project_key,
  created,
  creator_display_name,
  assignee_display_name,
  status,
  summary
from
  jira_issue
where
  project_key = 'TEST';
```

### List all issues assigned to a specific user

```sql
select
  id,
  key,
  summary,
  project_key,
  status,
  assignee_display_name,
  assignee_account_id
from
  jira_issue
where
  assignee_display_name = 'Lalit Bhardwaj';
```

### List issues due in the next week
```sql
select
  id,
  key,
  summary,
  project_key,
  status,
  assignee_display_name,
  assignee_account_id,
  duedate
from
  jira_issue
where
  duedate > current_date
  and duedate <= (current_date + interval '7' day);
```



### Get issues belonging to a sprint

```sql
select
  id,
  key,
  summary,
  project_key,
  status,
  assignee_display_name,
  assignee_account_id,
  duedate
from
  jira_issue
where
  sprint_ids @> '2';
```

#### List all issues in status category 'Done'

```sql
select
  id,
  key,
  summary,
  status,
  status_category,
  assignee_display_name
from
  jira_issue
where
  status_category = 'Done';
```

#### List all issues in status Waiting for Support

```sql
select
  id,
  key,
  summary,
  status,
  status_category,
  assignee_display_name
from
  jira_issue
where
  status = 'Waiting for support';
```

#### List all possible status for each status_category for a speficic project

```sql
select distinct
  project_key,
  status_category,
  status
from
  jira_issue
where
  project_key = 'PROJECT-KEY'
order by
  status_category;
```