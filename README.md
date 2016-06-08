# BeSync
BeSync is a powerful MySQL database copy tool:

- Written in golang and target binary has no dependencies
- Allow you transfer data to remote MySQL server without creating external access to you database (in daemon mode)
- Can use multiple threads for much faster data transfer instead of standard mysqldump tool
- Uses MySQL protocol and prepared statements instead of plain text "INSERT" queries
- Can work as daemon and has simple HTTP-api for starting data transfer (of course, works in CLI-mode)


## Building
### With Docker
Simply run container with docker-compose: `docker-compose up`

### Manually
- Install Go 1.6 and configure env variables
- Install libsqlite3-dev. For example, in ubuntu execute command `apt-get install libsqlite3-dev`
- Build the executable: `go get -v && go build -v --tags "libsqlite3 linux"`

## Working modes
BeSync can works in two modes: as regular cli command and as daemon

### cli command
In cli mode BeSync can copy data from one mysql database to another like `mysqldump ... | mysql ...`, but much faster.

Usage is simple:
`./besync --mode=cli --cli-config=config.json`

Structure of `config.json` is described below.

### Daemon mode
In this mode BeSync provide simple HTTP REST-like api for starting, stopping, get statuses of database copy tasks.
This mode also needed if you using proxy-mode (`WithoutProxy: false` in your configuration).

This mode is useful for periodical mysql database transfer tasks. For example, in BeGet shared hosting we're using this mode
for copying user's databases between servers. BeSync daemons are running on every server and getting tasks from central control service.

Start daemon:
`./besync --mode=http --http-host=myhost --http-port=8081`

Now you can make http-requests.

#### `POST /sync/start`
Starts database copy with given parameters.
Parameters is json config (similar to cli-mode) which must sent in request body.

For example: `curl -XPOST --data "@config.json" http://myshost:8081/sync/start`

Output has following format:
```
{"Id":1465390580840960058}
```

With given Id you may get status of task with next request

#### `GET /sync/{syncId}`
Gets info about task with given `Id`.

For example: `curl http://myhost:8081/sync/1465390580840960058`
```
{"Id":1465390580840960058,"Status":"success","Error":""}
```

## Configuration
All configuration made by json config.

Example of this config you may view in `config.json.example` file.

### SourceDb
This section contains credentials for connect to source mysql server for data selection;

### TargetDb
This section contains credentials for connect to target mysql server for insert selected data.

**Note**: if you using proxy-mode, this credentials are used for connecting **from proxy** to mysql server!
For example, if you run your proxy in same machine as your target database,
you may specify `host` as `localhost` (of course, if your mysql is listen on it)

### Export
- `WithoutProxy` (default false) - if set to false,
BeSync will use proxy mode and trying to connect to proxy server which is described in `Proxy` config section
- `AddDropTable` (default false) - execute DROP TABLE statement before each CREATE TABLE statement
- `AddDropTrigger` (default false) - execute DROP TRIGGER IF EXISTS before any CREATE TRIGGER statement
- `AddDropProcedure` (default false) - execute DROP PROCEDURE statement before dump each procedure
- `NoCreateTable` (default false) - do not execute CREATE TABLE statements that re-create each copying table
- `NoData` (default false) - Do not dump table contents
- `IncludeTables` (default empty) - list of tables/views to dump. If empty, all tables was processed
- `ExcludeTables` (default empty) - list of tables/views to exclude from dump. If empty, no tables was excluded
- `ExcludeTriggers` (default empty) - list of triggers to exclude from dump. If empty, no triggers was excluded
- `NoLockTables` (default false) - do not execute LOCK TABLES before starting transaction
- `NoTransaction` (default false) - do not start transaction with consistent snapshot on source database

### Proxy
This section is required if you specify `WithoutProxy: false` in `Export` config section.

- `Host` - host which your proxy is listening on
- `Port` - port which your proxy is listening on
- `ListenAddr` - host, which will be using for mysql-connections to proxy (usually equals `Host`)

