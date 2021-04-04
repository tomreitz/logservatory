import os, re, sys, getopt, sqlite3, math, csv
from datetime import datetime
from dateutil.parser import parse
#from csv import writer

# default args / global variables:
index = '' # path to log index file (for mode=logs)
start = '' # start date or timestamp to process (for mode=logs)
end = '' # end date or timestamp to process (for mode=logs)
format = 'aws-elb-classic' # or 'aws-elb-application', 'ncsa-common', 'ncsa-combined', 'elf'
queries_file = '' # path to file with queries to run
output = '' # path to directory for query output (queryN.csv)
sample = 1.0 # what fraction of logs to sample and query over
buffer_size = 100 # how large to let the log buffer get before ingesting it into SQLite
memory = 100000000 # maximum amount of memory (in bytes) the system can use
period = 60 # how many seconds to wait between successive query runs
encoding = 'utf-8'

queries = []
buffer = []
fields = []
connection = ''


def parse_args(mode='live'):
	global index, start, end, format, queries_file, output, sample, buffer_size, memory, period, queries, encoding

	if mode=='static':
		opts_array = ["index=", "start=", "end=", "format=", "queries=", "output=", "buffer=", "memory=", "period=", "sample=", "encoding="]
	else:
		opts_array = ["format=", "queries=", "output=", "buffer=", "memory=", "period=", "sample=", "encoding="]

	# parse cli args:
	argv = sys.argv[1:]
	try: 
		opts, args = getopt.getopt(argv, "", opts_array)
	except: 
		print("Error")
		exit()
	for opt, arg in opts:
		if opt in ['--index']:
			index = arg
		elif opt in ['--start']:
			start = arg
		elif opt in ['--end']:
			end = arg
		elif opt in ['--format']:
			format = arg
		elif opt in ['--queries']:
			queries_file = arg
		elif opt in ['--output']:
			output = arg
		elif opt in ['--buffer']:
			buffer_size = arg
		elif opt in ['--memory']:
			memory = arg
		elif opt in ['--period']:
			period = arg
		elif opt in ['--sample']:
			sample = arg
		elif opt in ['--encoding']:
			encoding = arg

def validate_args(mode='live'):
	global index, start, end, format, queries_file, output, sample, buffer_size, memory, period, queries

	# validate args:
	if format!='aws-elb-classic' and format!='ncsa-common' and format!='ncsa-combined':
		print('Argument "format" (the log format) must be one of "aws-elb-classic", "aws-elb-application", "ncsa-common", "ncsa-combined", or "elf". See the documentation for details.')
		exit()
	#if mode=='logs' and index file doesn't exist:
	#	print('Argument "index" must be a valid log index file. You can create one with build-log-index.py.')
	#	exit()
	try:
		f = open(queries_file, "r")
		fl = f.readlines()
		f.close()
		q = ''
		for x in fl:
			if x.strip(" \n\t\r")=='##########':
				queries.append(q.strip(" \n\t\r"))
				q = ''
			elif x[:1]=='#':
				continue
			else:
				q = q + ' ' + x
		if q!='':
			queries.append(q.strip(" \n\t\r"))
	except Exception as e:
		print(e)
		print('Argument "queries" must be a valid query file. See the documentation for format details.')
		exit()
	if not os.path.isdir(output):
		print('Argument "output" must be a directory. Query results will output here, one CSV file per query.')
		exit()

	try:
		buffer_size = int(buffer_size)
	except:
		print('Argument "buffer" (the buffer size) must be an integer.')
		exit()
	if buffer_size<=0:
		print('Argument "buffer" (the buffer size) must be a positive integer.')
		exit()

	try:
		memory = int(memory)
	except:
		print('Argument "memory" (the memory limit) must be an integer.')
		exit()
	if memory<=0:
		print('Argument "memory" (the memory limit) must be a positive integer.')
		exit()

	try:
		period = int(period)
	except:
		print('Argument "period" (the processing period) must be an integer.')
		exit()
	if period<=0:
		print('Argument "period" (the processing period) must be a positive integer.')
		exit()

	if start!='':
		try:
			start = datetime.timestamp(parse(start))
		except:
			print('Argument "start" (processing start date) could not be parsed.')
			exit()

	if end!='':
		try:
			end = datetime.timestamp(parse(end))
		except:
			print('Argument "end" (processing end date) could not be parsed.')
			exit()

	try:
		sample = float(sample)
	except:
		print('Argument "sample" (the sample rate) must be a floating point number. To query over all logs (and not sample), leave out this Argument.')
		exit()
	if sample<=0 or sample>1:
		print('Argument "sample" (the sample rate) must be a float between 0 and 1. To query over all logs (and not sample), leave out this Argument.')
		exit()


def start_database():
	global connection, format, fields
	connection = sqlite3.connect(':memory:')
	c = connection.cursor()

	# 1. create logs table based on log format
	if format=='aws-elb-classic':
		fields = ["timestamp", "elb", "client_ip", "client_port", "backend_ip", "backend_port",
			"request_processing_time", "backend_processing_time", "response_processing_time",
			"request_status_code", "backend_status_code", "received_bytes", "sent_bytes",
			"request_verb", "request_url", "request_protocol", "user_agent",
			"ssl_cipher", "ssl_protocol",
		]
		c.execute("""CREATE TABLE IF NOT EXISTS logs (
timestamp int,  elb_name string, request_ip string, request_port int, backend_ip string, backend_port int,
request_processing_time double, backend_processing_time double, client_response_time double,
request_status_code string, backend_status_code string, received_bytes bigint, sent_bytes bigint,
request_verb string, request_url string, request_protocol string, user_agent string, ssl_cipher string, ssl_protocol string)
""")

	if format=='ncsa-common':
		fields = ["request_ip", "auth_user", "timestamp", "request_verb", "request_url", "request_protocol",
			"request_status_code", "sent_bytes",
		]
		c.execute("""CREATE TABLE IF NOT EXISTS logs (
request_ip string, auth_user string, timestamp int, request_verb string, request_url string, request_protocol string,
request_status_code string, sent_bytes bigint)
""")

	if format=='ncsa-combined':
		fields = ["request_ip", "auth_user", "timestamp", "request_verb", "request_url", "request_protocol",
			"request_status_code", "sent_bytes", "referrer", "user_agent"
		]
		c.execute("""CREATE TABLE IF NOT EXISTS logs (
request_ip string, auth_user string, timestamp int, request_verb string, request_url string, request_protocol string,
request_status_code string, sent_bytes bigint, referrer string, user_agent string)
""")
		c.execute("""CREATE INDEX timestamp_idx ON logs (timestamp)""")

	connection.commit()


def load_index():
	global connection, index
	cur = connection.cursor()
	cur.execute("CREATE TABLE logs_idx (file, size_bytes, n_lines, min_ts, max_ts);")

	with open(index,'r') as fin:
		dr = csv.DictReader(fin)
		to_db = [(i['file'], i['size_bytes'], i['n_lines'], i['min_ts'], i['max_ts']) for i in dr]

	cur.executemany("INSERT INTO logs_idx (file, size_bytes, n_lines, min_ts, max_ts) VALUES (?, ?, ?, ?, ?);", to_db)
	connection.commit()


def fetch_log_files(start, end, sample):
	global connection
	cur = connection.cursor()
	if start!='' and end!='':
		count_query = "SELECT COUNT(*), MIN(min_ts), MAX(max_ts) FROM logs_idx WHERE min_ts>="+str(start)+" AND max_ts<="+str(end)
	else:
		count_query = "SELECT COUNT(*), MIN(min_ts), MAX(max_ts) FROM logs_idx"
	cur.execute(count_query)
	rows = cur.fetchall()
	num_rows = rows[0][0]
	cur = connection.cursor()
	if sample<1.0:
		query = "SELECT * FROM ( " + count_query.replace("COUNT(*)","*") + " ORDER BY RANDOM() LIMIT " + str(math.ceil(sample*num_rows)) + ") ORDER BY min_ts ASC, max_ts ASC, file ASC"
	else:
		query = count_query.replace("COUNT(*)","*") + " ORDER BY min_ts ASC, max_ts ASC, file ASC"
	cur.execute(query)
	rows = cur.fetchall()
	return rows


def ingest_logs():
	global connection, buffer, format, fields, buffer_size
	#print('Ingesting batch of '+str(buffer_size)+' lines...')
	log_values = []
	for log in buffer:
		# Note: for Python 2.7 compatibility, use ur"" to prefix the regex and u"" to prefix the test string and substitution.
		if format=='aws-elb-classic':
			# REFERENCE: https://docs.aws.amazon.com/athena/latest/ug/application-load-balancer-logs.html#create-alb-table
			regex = r"([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"
			#regex = r"([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" ($|\"[^ ]*\")(.*)"
		elif format=='ncsa-common':
			regex = '([(\d\.)]+) - (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+)'
		elif format=='ncsa-combined':
			regex = '([(\d\.)]+) - (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

		matches = re.search(regex, log.strip())
		if matches:
			values = []
			for i, field in enumerate(fields):
				if field=='timestamp' and (format=='ncsa-common' or format=='ncsa-combined'):
					values.append(datetime.timestamp(parse(matches.group(i+1).replace(':',' ',1))))
				else:
					values.append(matches.group(i+1))
			log_values.append(values)
	cur = connection.cursor()

	if format=='aws-elb-classic':
		cur.executemany("""INSERT INTO logs (
timestamp, elb_name, request_ip, request_port, backend_ip, backend_port,
request_processing_time, backend_processing_time, client_response_time,
request_status_code, backend_status_code, received_bytes, sent_bytes,
request_verb, request_url, request_protocol, user_agent, ssl_cipher, ssl_protocol
) VALUES ( strftime('%s', ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
""", log_values)
	elif format=='ncsa-common':
		cur.executemany("""INSERT INTO logs (
request_ip, auth_user, timestamp, request_verb, request_url, request_protocol, request_status_code,
sent_bytes) VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )
""", log_values)
	elif format=='ncsa-combined':
		cur.executemany("""INSERT INTO logs (
request_ip, auth_user, timestamp, request_verb, request_url, request_protocol, request_status_code,
sent_bytes, referrer, user_agent) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
""", log_values)

	connection.commit()


def run_queries(mode='live', params=()):
	global connection, queries, output
	#print('Running queries...')
	for i, q in enumerate(queries):
		cur = connection.cursor()
		# prevent accidentally dumping tons of data to console!
		if len(params)>0:
			cur.execute(q, params)
		else:
			cur.execute(q)
		rows = cur.fetchall()
		#print(rows[0:3])
		if mode=='live': flag = 'w'
		else: flag = 'a'
		with open(output + 'query' + str(i) + '.csv', flag) as write_obj:
			csv_writer = csv.writer(write_obj)
			for row in rows:
				csv_writer.writerow(row)


def print_db_stats():
	min_ts = get_db_stat('SELECT MIN(timestamp) FROM logs')
	if min_ts: min_time = datetime.utcfromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')
	else: min_time = ''
	max_ts = get_db_stat('SELECT MAX(timestamp) FROM logs')
	if max_ts: max_time = datetime.utcfromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')
	else: max_time = ''
	n_rows = get_db_stat('SELECT COUNT(*) FROM logs')
	page_size = get_db_stat('PRAGMA page_size')
	page_count = get_db_stat('PRAGMA page_count')
	print("Database is now "+min_time+" to "+max_time+" ("+str(n_rows)+" rows). Memory used ~= "+str(page_size*page_count)+" bytes.")


def get_db_stat(q):
	global connection
	cur = connection.cursor()
	cur.execute(q)
	rows = cur.fetchall()
	if rows and rows[0]: return rows[0][0]
	else: return None
