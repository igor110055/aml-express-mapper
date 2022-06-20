import sys
import csv
# import logging
import pymysql
from tqdm import tqdm
from sshtunnel import SSHTunnelForwarder

MYSQL_PORT = 3306
aml_tunnel = SSHTunnelForwarder(('3.37.44.106', 22),
                            ssh_username='ubuntu',
                            ssh_password='',
                            remote_bind_address=('aml-database.cyojdge9rvsp.ap-northeast-2.rds.amazonaws.com', MYSQL_PORT),
                            local_bind_address=('localhost', 1234),
                            ssh_pkey='AML.pem',
                            ssh_private_key_password='')
aml_tunnel.start()

#rds info
host = 'aml-database.cyojdge9rvsp.ap-northeast-2.rds.amazonaws.com'
username = 'aml60'
database = 'aml60'
pw = 'oasisexc1103!'

try:
    conn = pymysql.connect(host=aml_tunnel.local_bind_host,
                           user=username,
                           password=pw,
                           db=database,
                           port=aml_tunnel.local_bind_port,
                           use_unicode=True,
                           charset='utf8')
except:
    # logging.error('couldn\'t connect to rds')
    print('couldn\'t connect to rds')


def insert_from_csv_t_base():
    cursor = conn.cursor()
    file = open('./processed/kyc_processed_data.csv', 'r')
    csv_data = csv.reader(file)
    # columns = 'JOB_DT, CUST_CODE, CUST_DIV_CD, CDD_CS_TYP_CD, AGNT_RET_CD, RNM_CNFR_CD, RNM_NO, CUST_NM, CUST_ENG_NM, NTN_CD, BRDT, SEX_CD, RSDNC_NTN_CD, RSDNC_F, HOME_PST_NO, HOME_ADDR, HOME_TEL_NO, WORK_PST_NO, WORK_ADDR, WORK_TEL_NO, CELL_PHONE, JOB_CD, PAPT_NO, RSK_GRD_CD, RSK_GRD_MRK, CDD_MAKE_MN_ID, CDD_MAKE_BR_CD, CDD_REG_DT, EDD_REG_DT, TR_OBJ_USE_CD, TR_OBJ_USE_ETC_NM, CAP_SRC_CD, CAP_SRC_ETC_NM, WP_NM, BUSI_NM, BSMN_RNO, EMP_CNT, YY_INCM_AMT_CD, ESTB_OBJ_NM, FNDTN_DT, ENTP_SCL_CD, LSTNG_YN_CD, LSTNG_INFO_CD, MAIN_GDS_SRVC_ETC, CS_ST_CD, CS_RGST_DT, CS_MDFY_DT, CS_DLT_DT, EMAIL, SECU_CERT_LVL, API_USE_YN, O_STO_F, EDD_EXE_YN, RE_KYC_DATE, INTERVIEW_CCD, IND_CLS_GB, IND_CLS_DT, CLOSE_DT, TL_ASTS_AMT, LG_AMT_ASTS_F, REST_F, REST_DT, ACDT_F, INDST_DVD_CD, DISPLAY_USER_NO, ENCRP_RNM_NO, IN_BLACKLIST_YN, RISK_TARGET_YN, TRUTH_ORG_CD, INFO_IMPOS_ORG_YN, NTN_PBLIC_GROUP_F, PMSN_F, AML_REF_DT'
    values = ['VALUES(']
    for row in tqdm(csv_data):
        if type(values) is list:
            for i in range(0, row.__len__()):
                if (i > 0):
                    values.append(', ')
                values.append('%s')
            values.append(')')
            values = ''.join(values)
        parsed = []
        for data in row:
            if data == 'NULL':
                data = None
            parsed.append(data)
        cursor.execute('INSERT IGNORE INTO t_kyc_base(JOB_DT, CUST_CODE, CUST_DIV_CD, CDD_CS_TYP_CD, AGNT_RET_CD, RNM_CNFR_CD, RNM_NO, CUST_NM, CUST_ENG_NM, NTN_CD, BRDT, SEX_CD, RSDNC_NTN_CD, RSDNC_F, HOME_PST_NO, HOME_ADDR, HOME_TEL_NO, WORK_PST_NO, WORK_ADDR, WORK_TEL_NO, CELL_PHONE, JOB_CD, PAPT_NO, RSK_GRD_CD, RSK_GRD_MRK, CDD_MAKE_MN_ID, CDD_MAKE_BR_CD, CDD_REG_DT, EDD_REG_DT, TR_OBJ_USE_CD, TR_OBJ_USE_ETC_NM, CAP_SRC_CD, CAP_SRC_ETC_NM, WP_NM, BUSI_NM, BSMN_RNO, EMP_CNT, YY_INCM_AMT_CD, ESTB_OBJ_NM, FNDTN_DT, ENTP_SCL_CD, LSTNG_YN_CD, LSTNG_INFO_CD, MAIN_GDS_SRVC_ETC, CS_ST_CD, CS_RGST_DT, CS_MDFY_DT, CS_DLT_DT, EMAIL, SECU_CERT_LVL, API_USE_YN, O_STO_F, EDD_EXE_YN, RE_KYC_DATE, INTERVIEW_CCD, IND_CLS_GB, IND_CLS_DT, CLOSE_DT, TL_ASTS_AMT, LG_AMT_ASTS_F, REST_F, REST_DT, ACDT_F, INDST_DVD_CD, DISPLAY_USER_NO, ENCRP_RNM_NO, IN_BLACKLIST_YN, RISK_TARGET_YN, TRUTH_ORG_CD, INFO_IMPOS_ORG_YN, NTN_PBLIC_GROUP_F, PMSN_F, AML_REF_DT) ' \
                       f'{values}',
                       parsed)
    # close the connection to the database.
    conn.commit()
    cursor.close()


def update_decrypted_columns():
    cursor = conn.cursor()
    file = open('./processed/kyc_decrypted_data.csv', 'r')
    csv_data = csv.reader(file)
    for row in csv_data:
        cursor.execute(f'UPDATE t_kyc_base SET CELL_PHONE=\'{row[1]}\', EMAIL=\'{row[2]}\' WHERE CUST_CODE=\'{row[0]}\';')
    conn.commit()
    cursor.close()

def update_unmapped_columns_t_kyc_base():
    cursor = conn.cursor()
    file = open('./processed/kyc_processed_data.csv', 'r')
    csv_data = csv.reader(file)
    for row in tqdm(csv_data):
        cursor.execute(f'UPDATE t_kyc_base SET CDD_CS_TYP_CD=\'{row[3]}\' WHERE CUST_CODE=\'{row[1]}\';')
    conn.commit()
    cursor.close()


def insert_from_csv_ac_prod():
    cursor = conn.cursor()
    file = open('./processed/t_ac_prod.csv', 'r')
    csv_data = csv.reader(file)
    # columns = 'JOB_DT,GNL_AC_NO,GDS_NO,AC_ST_CCD,CUST_CODE,RNM_NO,OGN_CD,OGN_BRN_CD,OGN_BRN_NM,GDS_CD,AC_OPN_DT,HANDO_EXP_DT,ACDT_F,MN_DL_DPRT_CD,OPN_BRN_CD,REST_F,REST_DT,RGST_DT,MDFY_DT,AML_REF_DT'
    values = ['VALUES(']
    for row in tqdm(csv_data):
        if type(values) is list:
            for i in range(0, row.__len__()):
                if (i > 0):
                    values.append(', ')
                values.append('%s')
            values.append(')')
            values = ''.join(values)
        cursor.execute('INSERT IGNORE INTO t_ac_prod(JOB_DT,GNL_AC_NO,GDS_NO,AC_ST_CCD,CUST_CODE,RNM_NO,OGN_CD,OGN_BRN_CD,OGN_BRN_NM,GDS_CD,AC_OPN_DT,HANDO_EXP_DT,ACDT_F,MN_DL_DPRT_CD,OPN_BRN_CD,REST_F,REST_DT,RGST_DT,MDFY_DT,AML_REF_DT) ' \
                       f'{values}',
                       row)
    # close the connection to the database.
    conn.commit()
    cursor.close()


def insert_from_csv_kyc_token_address():
    cursor = conn.cursor()
    file = open('./processed/t_kyc_token_address.csv', 'r')
    csv_data = csv.reader(file)
    # columns = 'JOB_DT,GNL_AC_NO,WALLET_PLATFORM_ID,CURRENCY_ID,ADDRESS,DESTINATION_TAG,CUST_CODE,INPUT_DATE,CHG_DATE,DEL_YN,AML_REF_DT'
    values = ['VALUES(']
    for row in tqdm(csv_data):
        if type(values) is list:
            for i in range(0, row.__len__()):
                if (i > 0):
                    values.append(', ')
                values.append('%s')
            values.append(')')
            values = ''.join(values)
        parsed = []
        for data in row:
            if data == 'NULL':
                data = None
            parsed.append(data)
        cursor.execute('INSERT IGNORE INTO t_kyc_token_address(JOB_DT,GNL_AC_NO,WALLET_PLATFORM_ID,CURRENCY_ID,ADDRESS,DESTINATION_TAG,CUST_CODE,INPUT_DATE,CHG_DATE,DEL_YN,AML_REF_DT) ' \
                       f'{values}',
                       parsed)
    # close the connection to the database.
    conn.commit()
    cursor.close()


def insert_from_csv_t_tms_dl(method):
    cursor = conn.cursor()
    file = open(f'./processed/t_tms_dl_{method}.csv', 'r')
    csv_data = csv.reader(file)
    # columns = 'JOB_DT,GNL_AC_NO,DL_DT,DL_SQ,DL_CHNNL_CD,DL_TYP_CD,DL_WY_CD,STBD_CODE,STBD_NM,DEAL_FLUC_QTY,DL_MD_CCD,FXCH_DL_OBJ_CD,DL_TM,DL_AMT,TFND_RA,OGN_CD,HANDLNG_BRN_CD,CPRTY_TRSNS_OGN_CD,CPRTY_TRSNS_FOGN_NM,CPRTY_TRSNS_DPRT_CD,CPRTY_TRSNS_AC_NO,CPRTY_TRSNS_AC_NM,BUSI_CD,CNCL_F,CNCL_DY_TM,WLT_ADR,DESTINATION_TAG,CHNANL_SOR,INVST_WRN_ITEM_TRD_F,AML_REF_DT'
    values = ['VALUES(']
    for row in csv_data:
        if type(values) is list:
            for i in range(0, row.__len__()):
                if (i > 0):
                    values.append(', ')
                values.append('%s')
            values.append(')')
            values = ''.join(values)
        cursor.execute('INSERT IGNORE INTO t_tms_dl(JOB_DT,GNL_AC_NO,DL_DT,DL_SQ,DL_CHNNL_CD,DL_TYP_CD,DL_WY_CD,STBD_CODE,STBD_NM,DEAL_FLUC_QTY,DL_MD_CCD,FXCH_DL_OBJ_CD,DL_TM,DL_AMT,TFND_RA,OGN_CD,HANDLNG_BRN_CD,CPRTY_TRSNS_OGN_CD,CPRTY_TRSNS_FOGN_NM,CPRTY_TRSNS_DPRT_CD,CPRTY_TRSNS_AC_NO,CPRTY_TRSNS_AC_NM,BUSI_CD,CNCL_F,CNCL_DY_TM,WLT_ADR,DESTINATION_TAG,CHNANL_SOR,INVST_WRN_ITEM_TRD_F,AML_REF_DT) ' \
                       f'{values}',
                       row)
    # close the connection to the database.
    conn.commit()
    cursor.close()


def update_aml_express_database():
    insert_from_csv_t_base()
    update_decrypted_columns()
    insert_from_csv_ac_prod()
    insert_from_csv_kyc_token_address()
    insert_from_csv_t_tms_dl('buycrypto')
    insert_from_csv_t_tms_dl('sellcrypto')
    insert_from_csv_t_tms_dl('transfercrypto')


if __name__ == '__main__':
    param = sys.argv[1]
    if param:
        num = int(param)
        if num == 0:
            insert_from_csv_t_base()
        elif num == 1:
            update_decrypted_columns()
        elif num == 2:
            insert_from_csv_ac_prod()
        elif num == 3:
            insert_from_csv_kyc_token_address()
        elif num == 4:
            insert_from_csv_t_tms_dl('buycrypto')
            insert_from_csv_t_tms_dl('sellcrypto')
            insert_from_csv_t_tms_dl('transfercrypto')
        elif num == 5:
            update_unmapped_columns_t_kyc_base()
        elif num == 6:
            update_aml_express_database()
        else:
            print('unhandled parameter')

