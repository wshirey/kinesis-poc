## Configuration

This program uses the [DotNetEnv](https://github.com/tonerdo/dotnet-env) nuget package to load environment variables.
Add a `/kinesis-producer-dotnet.env` text file and
make sure it has the following values defined:

```
AWS_ACCESS_KEY_ID=<value>
AWS_SECRET_ACCESS_KEY=<value>
AWS_DEFAULT_REGION=<value>
KINESIS_STREAM=<value>
SQL_HOST=<value>
SQL_USER=<value>
SQL_PASS=<value>
SQL_DB=<value>
```

## Build, Run

- `dotnet build`
- `dotnet run`