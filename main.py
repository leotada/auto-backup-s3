import boto3
import lzma
import re
import os
import subprocess
from logging import log

BUCKET = 'tada-backup-falcao'


def dump_db():
    """
        Realiza o backup do sistema utilizando a ferramenta pg_dump do postgreSQL.
        No windows, o pg_dump precisa estar no PATH do sistema.
    """
    try:
        s = 'SQLALCHEMY_DATABASE_URI'
        user, password = re.search('//(.*)@', s).group(1).split(':')
        host, db = re.search('@(.*)', s).group(1).split('/')

        # Define a variavel de ambiente com a senha para o pg_dump.
        arq_backup = 'falcao.backup'
        if password != '':
            # Linux
            if os.name == 'posix':
                os.system('PGPASSWORD="{}"'.format(password))
                arq_backup = '/tmp/' + arq_backup
            # Windows TODOS
            else:
                os.system('SET PGPASSWORD="{}"'.format(password))
        r = subprocess.check_call(['pg_dump', '-d', db, '-h', host, '-U', user, '-f', arq_backup, '-F', 't', '-w'])
    except Exception as e:
            log.exception('admin, _backup. Erro pg_dump: {}.'.format(e))
    return r == 0


def compress_file(input_name, output_name):
    with open(input_name, "rb") as r:
        data = r.read()
        with lzma.open(output_name, "w") as f:
            f.write(data)


def upload_file(filename):
    s3 = boto3.client('s3')
    bucket_name = BUCKET
    s3.upload_file(filename, bucket_name, filename)


def schedule_backup():
    pass
