import csv

from keboola import docker
#
# from kbc.env_handler import KBCEnvHandler
#
# DEFAULT_TABLE_INPUT = "/data/in/tables/"
# DEFAULT_FILE_INPUT = "/data/in/files/"
#
# DEFAULT_FILE_DESTINATION = "/data/out/files/"
# DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
#
# # #### Keep for debug
# KEY_DEBUG = 'debug'
#
# # list of mandatory parameters => if some is missing, component will fail with readable message on initialization.
# MANDATORY_PARS = []
# MANDATORY_IMAGE_PARS = []
#
# APP_VERSION = '0.0.1'

def run(datadir):
    cfg = docker.Config(datadir)
    parameters = cfg.get_parameters()
    print("Hello World!")
    print(parameters)
    in_file = datadir + '/in/tables/source.csv'
    out_file = datadir + '/out/tables/destination.csv'
    with open(in_file, mode='rt', encoding='utf-8') as in_file, \
            open(out_file, mode='wt', encoding='utf-8') as out_file:
        lazy_lines = (line.replace('\0', '') for line in in_file)
        reader = csv.DictReader(lazy_lines, dialect='kbc')
        writer = csv.DictWriter(out_file, dialect='kbc',
                                fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow({'id': int(row['id']) * 42,
                             'sound': row['sound'] + 'ping'})
