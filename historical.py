import os, re, sys, getopt, time
import sys, time
import logservatory


if __name__ == "__main__":

	logservatory.parse_args('static')
	logservatory.validate_args('static')
	logservatory.start_database()
	start_timestamp = int(time.time())
	page_size = logservatory.get_db_stat('PRAGMA page_size')

	# start processing logs from index:
	logservatory.load_index()

	if logservatory.start!='': global_start = logservatory.start
	else: global_start = logservatory.get_db_stat('SELECT MIN(min_ts) FROM logs_idx')
	if logservatory.end!='': global_end = logservatory.end
	else: global_end = logservatory.get_db_stat('SELECT MAX(max_ts) FROM logs_idx')

	logs_idx_rows = logservatory.fetch_log_files(global_start, global_end, logservatory.sample)
	# log rows contain columns: 0=file, 1=size_bytes, 2=n_lines, 3=min_ts, 4=max_ts

	buffer_size = 0
	n_logs = 0
	n_requests = 0
	n_bytes = 0
	for log_idx_row in logs_idx_rows:
		with open(log_idx_row[0], 'r', encoding=logservatory.encoding) as file:
			for line in file: logservatory.buffer.append(line.strip())
		buffer_size += int(log_idx_row[1])
		n_logs += 1
		n_requests += int(log_idx_row[2])
		n_bytes = int(log_idx_row[1])

		if buffer_size>= 10000 # hard-code buffer_size of 10k lines
			logservatory.ingest_logs()
			logservatory.buffer = []
			buffer_size = 0
			logservatory.print_db_stats()
			logservatory.run_queries(mode='static')

			# empty logs table to make room for new data
			page_count = logservatory.get_db_stat('PRAGMA page_count')
			database_size = page_size * page_count
			if database_size >= 0.9 * logservatory.memory
				cur = logservatory.connection.cursor()
				cur.execute("DELETE FROM logs")
				logservatory.connection.commit()

	# process final queries after logs are done ingesting
	logservatory.ingest_logs()
	logservatory.buffer = []
	buffer_size = 0
	logservatory.print_db_stats()
	logservatory.run_queries(mode='static')

	end_timestamp = int(time.time())
	print("Done. Processed "+str(n_logs)+" log files, "+str(n_requests)+" requests, "+str(n_bytes)+" bytes of data in "+str(end_timestamp-start_timestamp)+" seconds.")


#		logservatory.buffer.append(line)

#		# every buffer_size lines, process buffer
#		if len(logservatory.buffer) >= logservatory.buffer_size:
#			logservatory.ingest_logs()
#			logservatory.buffer = [] # empty buffer

#			page_count = logservatory.get_db_stat('PRAGMA page_count')
#			database_size = page_size * page_count

#			if database_size > 0.9 * logservatory.memory: # some extra room for SQLite overheads
#				# delete oldest ~25% of log entries from the logs table
#				min_ts = logservatory.get_db_stat('SELECT MIN(timestamp) FROM logs')
#				avg_ts = logservatory.get_db_stat('SELECT AVG(timestamp) FROM logs')
#				target_ts = round((avg_ts+min_ts)/2)
#				cur = logservatory.connection.cursor()
#				cur.execute("DELETE FROM logs WHERE timestamp<"+str(target_ts))
#				logservatory.connection.commit()


#		# every period seconds, run queries
#		if int(time.time()) - last_run_timestamp >= logservatory.period:
#			logservatory.run_queries()
#			last_run_timestamp = int(time.time())
#			logservatory.print_db_stats()
