CONNECTION_IDS = {
    'oasis': {
        'SSH_CONNECTION': {
            'REMOTE_SERVER': ('3.35.189.150', 22),
            'SSH_USERNAME': 'ec2-user',
            'SSH_PRIVATE_KEY': 'oasis-production-bastion.pem',
            'REMOTE_BIND_ADDRESS': ('oasis-production-database-main.cyojdge9rvsp.ap-northeast-2.rds.amazonaws.com', 5432),
            'LOCAL_BIND_ADDRESS': ('localhost', 1234)
        },

        'DB_CONNECTION': {
            'DB_USERNAME': 'oasis',
            'DB_PASSWORD': 'QDKCYCgzwZjb436F7gPi',
            'DB_LOCAL_ADDRESS': 'localhost',
            'DB_LOCAL_PORT': '5432',
            'DB_NAME': 'exchange'
        }
    },
}