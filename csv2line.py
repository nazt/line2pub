import glob, os, sys
import argparse
import pandas as pd
from tqdm import tqdm
from subprocess import call
import sys
import click

assert sys.version[:1] == "3"


@click.group()
def cli():
	"""A CLI wrapper for CSV2Line"""


if __name__ == '__main__':
	cli()


def filename(path):
	return os.path.basename(path).split('.csv')[0]


# @click.option('--output-dir', required=True, type=str, help='Output directory')
@click.option('--csv-file', required=True, type=str, help='CSVInput directory')
@cli.command("convert")
def cc(csv_file):
	"""convert csv to influx line protocol !!!"""

	processing_dir = os.path.normpath(os.path.dirname(csv_file))
	output_dir = processing_dir

	csv_file_input = csv_file
	done_dir = "{}/.done".format(output_dir)
	file_name = os.path.basename(csv_file)
	done_flag_file = "{}/.done/{}".format(output_dir, file_name)

	if not os.path.isdir(done_dir):
		os.makedirs(done_dir, exist_ok=True)

	if os.path.exists(done_flag_file):
		print("{0} exists\r\nSKIPPED!".format(done_flag_file))
		return

	# t = '{}/done.txt'.format(os.path.abspath(output_dir))

	# if os.path.exists(t):
	# 	print("{0} SKIPPED!".format(csv_dir.split("/")[-1:][0]))
	# return

	# folders = sorted(glob.glob('{0}/*.csv'.format(csv_dir)))
	# files = [{'name': filename(file)} for file in folders]
	# directory = csv_dir.split("/")[-3:]

	# db = directory[0]
	# measurement = directory[1]
	# month = directory[2]

	# p1bar = tqdm(total=len(files), unit='files')
	# write_meta(db=db, output_dir=output_dir)

	# for file in files:
	target_file = '{}/LP_{}.txt'.format(os.path.abspath(output_dir), file_name)

	df = pd.read_csv(csv_file_input)
	df = df.fillna(0)

	# measurement = df.iloc[0]['name']
	# p1bar.set_description('[{}] {}/{}'.format(file['name'].split("_")[0], "dummy-db", measurement))

	with open(target_file, "w") as out_file:
		pbar = tqdm(total=len(df), leave=False, unit='lines')
		rows = df.iterrows()
		for idx, row in rows:
			s = to_line(row)
			out_file.write(s)
			pbar.update(1)
			pbar.set_postfix(file=target_file)
		pbar.close()

	# p1bar.update(1)

	# p1bar.close()

	# t = '{}/done.txt'.format(os.path.abspath(output_dir))
	with open(done_flag_file, 'w') as meta:
		meta.write("")


def to_line(row):
	time = row['time']
	name = row['name']
	topic = row['topic']
	if 'host' in row:
		row = row.drop(labels=['time', 'name', 'topic', 'host'])
	else:
		row = row.drop(labels=['time', 'name', 'topic'])

	row = row[row != 0]
	s = "{},topic={} ".format(name, topic)
	for (key, val) in row.iteritems():
		s += "{}={},".format(key, val)
	s = s[:-1] + ' ' + str(time) + '\n'
	return s

# def write_meta(db, output_dir):
# 	lines = [
# 		"# DDL",
# 		"CREATE DATABASE {}".format(db),
# 		"",
# 		"# DML",
# 		"# CONTEXT-DATABASE: {}".format(db),
# 		""
# 	]
# 	with open(os.path.abspath(output_dir) + '/../meta.txt', 'w') as meta:
# 		meta.write("\n".join(lines))
