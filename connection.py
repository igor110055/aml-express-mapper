import psycopg2
from sshtunnel import SSHTunnelForwarder
import paramiko
import config

class Connection:
    connection_id_ssh = config.CONNECTION_IDS['oasis']['SSH_CONNECTION']
    POSTGRESQL_PORT = 5432
    tunnel = SSHTunnelForwarder(('3.35.189.150', 22),
                                ssh_username='ec2-user',
                                ssh_password='',
                                remote_bind_address=(
                                    'oasis-production-database-main.cyojdge9rvsp.ap-northeast-2.rds.amazonaws.com',
                                    POSTGRESQL_PORT),
                                local_bind_address=('localhost', 1234),
                                ssh_pkey='/home/ubuntu/aml-express-mapper/oasis-production-bastion.pem',
                                ssh_private_key_password='')
    tunnel.start()
    db = psycopg2.connect(host=tunnel.local_bind_host,
                          dbname='exchange',
                          user='oasis',
                          password='QDKCYCgzwZjb436F7gPi',
                          port=tunnel.local_bind_port)


def test_select():
    conn = Connection()
    cur = conn.db.cursor()
    cur.execute("select * from \"user\" limit 1;")
    data = cur.fetchall()
    print(data)

