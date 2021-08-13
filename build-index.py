import os, re, sys, getopt, time
from dateutil import parser
#from csv import writer
#import pandas as pd


def tail(fName, lines=20):
	total_lines_wanted = lines
	BLOCK_SIZE = 1024
	with open(fName,'rb') as f:
		f.seek(0, 2)
		block_end_byte = f.tell()
		lines_to_go = total_lines_wanted
		block_number = -1
		blocks = []
		while lines_to_go > 0 and block_end_byte > 0:
			if (block_end_byte - BLOCK_SIZE > 0):
				f.seek(block_number*BLOCK_SIZE, 2)
				blocks.append(f.read(BLOCK_SIZE))
			else:
				f.seek(0,0)
				blocks.append(f.read(block_end_byte))
			lines_found = blocks[-1].count(b'\n')
			lines_to_go -= lines_found
			block_end_byte -= BLOCK_SIZE
			block_number -= 1
		all_read_text = b''.join(reversed(blocks))
	return all_read_text.splitlines()[-total_lines_wanted:]


def rawcount(filename):
	with open(filename, 'rb') as f:
		lines = 0
		buf_size = 1024 * 1024
		read_f = f.raw.read
		buf = read_f(buf_size)
		while buf:
			lines += buf.count(b'\n')
			buf = read_f(buf_size)
	return lines


def process_logs(path, regex, fields):
	results = []
	nLines = 10
	for fName in os.listdir(path):
		if os.path.isdir(path+fName): # recurse into subdir
			subresults = process_logs(path+fName+'/', regex, fields)
			for r in subresults:
				results.append(r)
		else:
			# for each file, count lines, get file size
			fSize = os.path.getsize(path+fName)
			fLines = rawcount(path+fName)
			lines = [l.decode() for l in tail(path+fName, nLines)]
			if len(lines)>0:
				del lines[0]
			with open(path+fName) as f:
				lines.extend(f.readline() for i in range(nLines))
			# get min/max timestamp from firstNlines and lastNlines
			fMinTs = 99999999999999999
			fMaxTs = 0
			has_matches = False
			for l in lines:
				matches = re.search(regex, l.strip())
				if matches:
					has_matches = True
					values = []
					for i, field in enumerate(fields):
						values.append(matches.group(i+1))
					ts = values[fields.index('timestamp')]
					ts = int(parser.parse(ts).strftime('%s'))
					if ts>fMaxTs:
						fMaxTs = ts
					elif ts<fMinTs:
						fMinTs = ts
			if has_matches:
				results.append([path+fName, str(fSize), str(fLines), str(fMinTs), str(fMaxTs)])
	return results


if __name__ == "__main__":
	# default args:
	input = ''
	format = 'aws-eb-classic'

	# parse cli args:
	argv = sys.argv[1:]
	try: 
		opts, args = getopt.getopt(argv, "", ["input=", "format=", "encoding="]) 
	except: 
		print("Error")
		exit()
	for opt, arg in opts:
		if opt in ['--input']:
			input = arg
		elif opt in ['--format']:
			format = arg

	# validate args:
	if format!='aws-elb-classic' and format!='aws-elb-application' and format!='ncsa-common' and format!='ncsa-combined':
		print('Argument "format" (the log format) must be one of "aws-elb-classic", "aws-elb-application", "ncsa-common", "ncsa-combined", or "elf". See the documentation for details.')
		exit()
	if not os.path.isdir(input):
		print('Argument "input" must be a directory where log files are stored.')
		exit()

	if format=='aws-elb-classic':
		fields = ["timestamp", "elb", "client_ip", "client_port", "backend_ip", "backend_port",
			"request_processing_time", "backend_processing_time", "response_processing_time",
			"elb_status_code", "backend_status_code", "received_bytes", "sent_bytes",
			"request_verb", "request_url", "request_protocol", "user_agent",
			"ssl_cipher", "ssl_protocol",
		]
		regex = r"([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"

	if format=='aws-elb-application':
		fields = ["type", "timestamp", "elb", "client_ip", "client_port", "backend_ip", "backend_port",
			"request_processing_time", "backend_processing_time", "response_processing_time",
			"elb_status_code", "backend_status_code", "received_bytes", "sent_bytes",
			"request_verb", "request_url", "request_protocol", "user_agent",
			"ssl_cipher", "ssl_protocol",
		]
		regex = r"([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^ ]*)\" \"([^\s]+?)\" \"([^\s]+)\" \"([^ ]*)\" \"([^ ]*)\""
		#([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"


	# process logs:
	results = process_logs(input, regex, fields)
	print('file,size_bytes,n_lines,min_ts,max_ts')
	for r in results:
		print(','.join(r))
