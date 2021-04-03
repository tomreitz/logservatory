# Logservatory

Logservatory is a Python tool for analyzing web access logs in several common formats.

Log analysis is possible with other tools or custom code. But Logservatory facilitates log analysis using standard database queries (no need to code besides SQL), and runs queries in parallel on in-memory data, minimizing disk reads, for fast performance.

### Two Modes

Logservatory can be run in two modes:

1. `live.py` processed piped live logs for near real-time processing.

2. `historical.py` lets you analyze many historical log files.

In either mode, you can specify arbitrary SQL queries to be run over the logs.

In `historical` mode, Logservatory also supports sampling: rather than run queries over *all* log files in a time range, it can sample *a percentage* of them, which accelerates iterative query development over large log volumes.

### How it works

Logservatory works by loading log files -- as many as can fit at once -- sequentially in time into an in-memory SQLite database. It then runs multiple queries over the database before reloading further data. This technique facilitates analysis of TB of logs in mere hours on a single machine.

Performance is phenomenal because each log file is only read from disk once, even if hundreds of queries are run against it. (In most DBMS systems, tables larger than memory would be read in separately for each query. Caching may prevent some, but not all, disk reads.)

However, this high performance comes with a tradeoff of limitations on the kind of queries that make sense. Queries that work well with Logservatory include:

* Filtering queries, like "all requests for URL X" or "all requests by IP address Y"

* Global counts and averages, like "total requests for URL X over all time" or "average number of requests per minute over all time"

* Time-series aggregation queries over time-scales that fit in memory, such as "N most popular URLs per hour" or "number of distinct IP addresses per minute"

Other types of queries are not possible or may produce inaccurate results, including:

* Aggregation over time-scales that do not fit in memory, like "N most popular URLs per month"

* Global distinct counts, such as "number of distinct client IP addresses over all time"

But such queries are often still possible to answer by re-writing the query and post-processing results. For example, to answer "number of distinct client IP addresses over all time", one could emit distinct IP addresses per hour, then count distinct IP addresses in the output file. To answer "N most popular URLs per month", one could emit the most popular URLs per hour (or whatever time-scale fits in memory), then aggregate the results per month.

### Usage Examples

To process live logs:

```bash
tail -f path/to/access_log | python live.py \
    --format ncsa-common \
    --queries path/to/queries.sql \
    --output path/to/output/ \
    --buffer 100\
    --period 5
```

(This ingests logs in batches of 100 and runs `queries.sql` every 5 wall-clock seconds.)

To process historical log files, first build an index over the files with

```bash
python build-index.py \
    --input /path/to/logs/ \
    --format ncsa-common \
    --encoding ISO-8859-1 > log-index.csv
```

This will build a list of all the log files with, for each, the file size, number of lines, and earliest and latest timestamps. Performance depends on your machine and disk, but log indexing is designed to be fast, processing up to 1.8M lines or 750MB per second.

Once you've build the index, query over the logs with

```bash
python historical.py \
    --start 20200201 \
    --end 20200301 \
    --format aws-elb-classic \
    --index path/to/log-index.csv \
    --queries path/to/queries.sql \
    --output path/to/output/
```

(This runs `queries.sql` over each memory-batch of logs.)

### Parameters

Parameters common to `live.py`, `historical.py`, and `build-index.py` are:

- `format`: (required) The log format, one of
  
  - `aws-elb-classic`: [AWS Classic Elastic Load Balancer log format](https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format)
  
  - `aws-elb-application`: [AWS Application Load Balancer log format](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-log-entry-format)
  
  - `ncsa-common`: [Common Log Format](https://httpd.apache.org/docs/trunk/logs.html#common), used by Apache server and others
  
  - `ncsa-combined`: [Combined Log Format](https://httpd.apache.org/docs/trunk/logs.html#combined), the same as Common Log Format but also includes referrer and user agent string
  
  - `elf`: [Extended Log File Format](https://www.w3.org/TR/WD-logfile)

- `encoding`: (optional, default=`ISO-8859-1`) The character encoding of the log files, used when parsing them.

`build-index.py` takes one additional parameter:

- `input`: (required) The path to a folder containing the log files. This folder will be recursively searched for log files matching the specified `format` to index.

Additional parameters common to `live.py` and `historical.py` include:

- `queries`: (required) The path to a specially-formatted SQL file containing queries to run over the data. In this file, lines beginning with `#` are ignored. Queries should be separated by 10 `#` on their own line (`##########`). Schema details and example queries can be found in sections below.

- `output`: (required) The path to a directory to which query output will be written. For each query, a file *queryN.csv* is created, where *N* is the query number.

- `memory`: (optional, default=`100000000`) A positive integer, the memory limit. This is a target size of memory Logservatory's in-memory database shouldn't exceed. You should set this to as large a number as your system can reasonably handle, especially if you're processing high volume.

`live.py` takes two additional parameters:

- `buffer`: (optional, default=`100`) A positive integer, the buffer size. As requests stream into Logservatory (either from live piped logs or from log files via an index), they are buffered and inserted into SQLite in batches to improve performance. Your `buffer` size should be less than the typical number of requests per second your site receives times `period` (see below) in order to ensure accurate query results.

- `period`: (optional, default=`60`) How often to run the queries, in seconds (wall-clock time). Setting `period` to a larger number runs the queries less frequently, which improves performance. But `period` should not be more than the smallest aggregation time-scale of any of your `queries` to ensure accurate query results.

Finally, `historical.py` takes the following additional parameters:

* `index`: (required) The path to a log index file created with `build-index.py`.

* `start` and `end`: (optional) The date or full timestamp strings indicating the time range to query over. Any format [`dateutil.parser`](https://dateutil.readthedocs.io/en/stable/parser.html) understands should work. If not specified, queries will run over all logs in the index.

* `sample`: (optional) A float number between 0 and 1 -- the sample rate, or approximate fraction of logs to run queries over. For example, if you have a huge amount of logs, specifying `sample=0.001` runs queries over only 0.1% of the logs, which is *much* faster. This facilitates iterative query development and debugging.

### Schema

In order to write queries over log data, you need to know the data schema, which is fairly simple. One `logs` table has schema

```sql
CREATE TABLE logs (
    -- Fields available for all log formats:
    timestamp int, --------------- seconds since UNIX epoch (1970-01-01 00:00:00 UTC)
    request_ip string, ----------- IP address of client (12.34.56.78)
    request_status_code string, -- HTTP status code (200, 301, 403, etc.)
    request_verb string, --------- HTTP verb (GET, POST, etc.)
    request_url string, ---------- URL (http://test.co:80/page.html or /favicon.ico)
    request_protocol string,------ Protocol (HTTP/1.1 or similar)
    sent_bytes bigint, ----------- number of bytes in sent to client
    -- Fields available for all "aws-elb-*" formats:
    elb_name string, ------------- name of the load balancer
    request_port int, ------------ port request came in on
    request_proc_time double, ---- seconds taken to queue/route the request to a backend
    backend_proc_time double, ---- seconds taken for backend to process request
    response_proc_time double, --- seconds taken to process the whole request
    backend_ip string, ----------- (local) IP of the backend that processed the request
    backend_port int, ------------ backend port the request was sent to
    backend_status_code string, -- HTTP status code returned by backend
    received_bytes bigint, ------- incoming payload size (for PUT, POST, etc.)
    ssl_cipher string, ----------- SSL cipher used for HTTPS requests
    ssl_protocol string, --------- SSL protocol used for HTTPS requests
    -- Fields available for "ncsa-combined" format:
    referrer string, ------------- referrer header, if any
    user_agent string, ----------- user agent string, if any
);
```

**Notes:**

* All timestamps are UTC Unix timestamps which helps to standardize processing. You can convert to a more human-readable string (or extract the hour, month, etc.) using [SQLite date and time functions](https://sqlite.org/lang_datefunc.html).

* Query strings are not split from the `request_url`. Obtain the query string alone with `substr(request_url, 1+instr(request_url,'?'))`. Obtain the URL without query string with `substr(request_url,0,length(request_url)-instr(request_url,'?')+1)`.

### Queries

As discussed previously, arbitrary SQL queries can be run against log data with Logservatory. Here we provide some examples to help you get started.

**Example:** Simple time series count, number of 404 errors per second

```sql
SELECT timestamp, COUNT(*)
FROM logs WHERE request_status_code='404'
GROUP BY timestamp;
```

**Example:** Distinct search terms with counts

```sql
SELECT REPLACE(request_url, '/search?q=', '') AS term, COUNT(*)
FROM logs WHERE request_url LIKE '/search?q=%'
GROUP BY term;
```

**Note:** Logservatory will output rows for each memory-batch of logs, so output will likely contain duplicate terms with different counts. <u>You may need to postprocess the output</u> by deduping terms and summing the individual counts for each term.

**Example:** IP addresses exceeding 30/min rate limit

```sql
WITH results(ip, ts, n_reqs) AS (
    SELECT request_ip, timestamp, count(*) OVER w
    FROM logs
    WINDOW w AS (
        PARTITION BY request_ip
        ORDER BY timestamp
        RANGE BETWEEN 60 PRECEDING AND CURRENT ROW
    )
) SELECT * FROM results WHERE n_reqs>30
GROUP BY ip, ts, n_reqs;
```

**Note:** The above query makes use of [SQLite's window functions](https://www.sqlite.org/windowfunctions.html). Logservatory will output a row for every (IP, second) combination for which the IP exceeded  30 requests in the last minute. So if an IP address makes 31 requests all in one second, and no further requests, there will be 60 output rows, since for the next 60 seconds there would have been more than 30 requests by that IP in the last minute. To reduce the amount of output, you can either group differently (by minute, with `MAX(n_reqs)` for example) or postprocess Logservatory output.

### Performance

Logservatory's performance depends on many factors including your machine's disk, memory, and CPU specs. But here are some benchmarks on two different machines:

1. 2011 custom desktop, 16GB memory, 2TB HDD, ??? CPU ("old, slow machine")

2. 2020 MacBook Pro laptop, 16GB memory, 500GB SSD, ??? CPU ("modern, fast machine")

with three different workloads:

* **Workload 1:** `live.py`  processing (simulated) 1000 requests per second

* **Workload 2:** `historical.py` processing 20 GB of log files ()

* **Workload 3:** `historical.py` processing 1.4 TB of log files (564,704 log files, 3.284 BN requests)

| Workload  | Machine 1 Time | Machine 2 Time |
| --------- | -------------- | -------------- |
| 1         |                |                |
| 2 (index) |                |                |
| 2         |                |                |
| 3 (index) | 5.7 hours      | -              |
| 3         | ~20 ? hours    | -              |

1.4 TB logs (564,704 log files, 3.284 BN lines) should be indexed in 5.7 hours; index file will be about 104MB in size, about 565k lines.
