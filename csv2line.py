import glob, os, sys
import argparse
import pandas as pd
from tqdm import tqdm
from subprocess import call
import click


@click.group()
def cli():
	"""A CLI wrapper for CSV2Line"""


if __name__ == '__main__':
	cli()


@click.option('--csv-dir', required=True, type=str, help='CSVInput directory')
@click.option('--output-dir', required=True, type=str, help='Output directory')
@cli.command("convert")
def cc(csv_dir, output_dir):
	"""convert csv to influx line protocol !!!"""
	p = '{}/*.csv'.format(csv_dir)
	folders = sorted(glob.glob(p))
	files = [{'dir': os.path.dirname(item), 'filename': os.path.basename(item).split('.csv')[0]} for item in folders]
	# call('clear')
	directory = files[0]['dir'].split("/")[-2:]

	db = directory[0]
	measurement = directory[1]

	p1bar = tqdm(total=len(files), unit='files')
	lines = [
		"# DDL",
		"CREATE DATABASE {}".format(db),
		"",
		"# DML",
		"# CONTEXT-DATABASE: {}".format(db),
		""
	]
	with open(os.path.abspath(output_dir) + '/meta.txt', 'w') as meta:
		meta.write("\n".join(lines))

	for f in files:
		p1bar.set_description('[{}] {}/{}'.format(f['filename'].split("_")[0], db, measurement))
		file = '{}/{}.csv'.format(f['dir'], f['filename'])
		target_file = '{}/LP_{}.txt'.format(os.path.abspath(output_dir), f['filename'])
	
		df = pd.read_csv(file)
		df = df.fillna(0)

		with open(target_file, "w") as out_file:
			pbar = tqdm(total=len(df), leave=False, unit='lines')
			rows = df.iterrows()
			for idx, row in rows:
				s = toline(row)
				out_file.write(s)
				pbar.update(1)
				pbar.set_postfix(file=target_file)
			pbar.close()
		p1bar.update(1)
	p1bar.close()
	print('done')


def toline(row):
	time = row['time']
	name = row['name']
	topic = row['topic']
	row = row.drop(labels=['time', 'name', 'topic', 'host'])
	row = row[row != 0]
	s = "{},topic={} ".format(name, topic)
	for (key, val) in row.iteritems():
		s += "{}={},".format(key, val)
	s = s[:-1] + ' ' + str(time) + '\n'
	return s
