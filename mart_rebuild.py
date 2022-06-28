from datetime import datetime
from datetime import timedelta
import os
import sys

def run_cmd(cmd):
    os.system(cmd)
    # print(cmd)

if __name__ == '__main__':
    begin_date = '202203010'
    date_format = '%Y%m%d'

    end_date_str = sys.argv[1]
    end_date = datetime.strptime(end_date_str, date_format)

    index = 0
    prev_month = 0
    current_date_fmt = begin_date

    while(True):
        current_date = datetime.strptime(current_date_fmt, date_format) + timedelta(days=1)
        current_date_fmt = current_date.strftime(date_format)

        current_date_eve = current_date - timedelta(days=1)
        current_date_eve_fmt = current_date_eve.strftime(date_format)

        # if index == 0:
        #     cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/ETL_REV.sh', end_date_str, end_date_str]
        #     print('==============ETL_REV==============')
        #     run_cmd(' '.join(cmd))

        cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/KYC_WLF.sh', 'DAY', current_date_fmt, current_date_fmt, '0']
        print('==============KYC_WLF DAY==============')
        run_cmd(' '.join(cmd))

        cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/KYC_RA.sh', 'RAI', current_date_fmt, '0']
        print('==============KYC_RA RAI==============')
        run_cmd(' '.join(cmd))

        cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/TMS_STR.sh', current_date_eve_fmt, current_date_fmt, '0']
        print('==============TMS_STR==============')
        run_cmd(' '.join(cmd))

        if current_date.month != prev_month and prev_month != 0:
            # 월 1회 실행
            cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/KYC_RA.sh', 'RAB', current_date_fmt, '0']
            print('==============KYC_RA RAB==============')
            run_cmd(' '.join(cmd))

            cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/KYC_WLF.sh', 'MTH', current_date_fmt, current_date_fmt, '0']
            print('==============KYC_WLF MTH==============')
            run_cmd(' '.join(cmd))

        if (end_date - current_date).days == 0:
            cmd = ['. ~/.profile; /home/ubuntu/AMLExpress_6_0_Batch/dist/ETL_SND.sh', current_date_fmt, current_date.strftime('%Y')]
            print('==============ETL_SND==============')
            run_cmd(' '.join(cmd))

            break

        index += 1
        prev_month = current_date.month