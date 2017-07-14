import boto3
import lzma
import re
import os
import subprocess
import sched, time
from logging import log

BUCKET = 'tada-backup-falcao'
DATABASE_URI = 'postgresql+psycopg2://postgres:1@192.168.0.101:5432/falcao'
FILENAME = 'falcao.backup'
AGENDAR = False
MINUTES = 2


def dump_db():
    """
        Realiza o backup do sistema utilizando a ferramenta pg_dump do postgreSQL.
        No windows, o pg_dump precisa estar no PATH do sistema.
    """
    try:
        s = DATABASE_URI
        user, password = re.search('//(.*)@', s).group(1).split(':')
        host, db = re.search('@(.*)', s).group(1).split('/')
        try:
            host, porta = host.split(':')
        except:
            porta = '5432'

        # Define a variavel de ambiente com a senha para o pg_dump.
        arq_backup = FILENAME
        if password != '':
            # Linux
            if os.name == 'posix':
                os.system('PGPASSWORD="{}"'.format(password))
            # Windows TODOS
            else:
                os.system('SET PGPASSWORD="{}"'.format(password))
        r = subprocess.check_call(['pg_dump', '-d', db, '-h', host, '-p', porta, '-U', user, '-f', arq_backup, '-F', 't', '-w'])
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


def backup_on_s3():
    "Processo completo"
    upload_filename = FILENAME+'.7z'
    t1 = time.time()
    print('1. Realizando dump do banco de dados...')
    dump_db()
    print('2. Comprimindo arquivo...')
    compress_file(FILENAME, upload_filename)
    print('3. Carregando arquivo para nuvem...')
    upload_file(upload_filename)
    print('Backup realizado com sucesso!')
    print('Tempo: {0:.2f} segundos'.format(time.time() - t1))


def schedule_backup(minutes):
    "Agenda rotina"
    print('Agendando tarefa...')
    s = sched.scheduler()
    s.enter(minutes*60, 1, backup_on_s3)
    s.run()


def main():
    if AGENDAR:
        schedule_backup(MINUTES)
    else:
        backup_on_s3()


if __name__ == '__main__':
    main()
