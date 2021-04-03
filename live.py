import sys, time
import logservatory


if __name__ == "__main__":

	logservatory.parse_args('live')
	logservatory.validate_args('live')
	logservatory.start_database()
	page_size = logservatory.get_db_stat('PRAGMA page_size')

	# start processing logs:
	last_run_timestamp = 0
	for line in sys.stdin:
		if line=='\x04\n': break

		# add line to a buffer
		logservatory.buffer.append(line)

		# every buffer_size lines, process buffer
		if len(logservatory.buffer) >= logservatory.buffer_size:
			logservatory.ingest_logs()
			logservatory.buffer = [] # empty buffer

			page_count = logservatory.get_db_stat('PRAGMA page_count')
			database_size = page_size * page_count

			if database_size > 0.9 * logservatory.memory: # some extra room for SQLite overheads
				# delete oldest ~25% of log entries from the logs table
				min_ts = logservatory.get_db_stat('SELECT MIN(timestamp) FROM logs')
				avg_ts = logservatory.get_db_stat('SELECT AVG(timestamp) FROM logs')
				target_ts = round((avg_ts+min_ts)/2)
				cur = logservatory.connection.cursor()
				cur.execute("DELETE FROM logs WHERE timestamp<"+str(target_ts))
				logservatory.connection.commit()


		# every period seconds, run queries
		if int(time.time()) - last_run_timestamp >= logservatory.period:
			logservatory.run_queries(mode='live')
			last_run_timestamp = int(time.time())
			logservatory.print_db_stats()
