## To Run:
this project not integrated yet, so you should execute each script manually

1. `$ python processing_prod_db.py`
2. `$ python processing_uncompleted_userdata.py`
3. `$ python update_aml_db.py 6` (6 is temporary option, run all options in that script)

you can integrate all script to main.py if you want;

but there is an issue with ssh tunnel forwarder, so you should edit connection.py first. 

## requirements(not configured requirements.txt):
- tqdm (for progress)
- Fernet (for de/encryption)
- sshtunnel (for rds connection)
- pymysql (for aml db insertion)
- psycopg2 - pip install psycopg2

[//]: # (- logging)

## WARNING:
currently we're using local-fixed version of SSHTunnelForwarder because of bug:

https://github.com/pahaz/sshtunnel/pull/242

to avoid this bug, comment out `sshtunnel.py`:1312..1316
