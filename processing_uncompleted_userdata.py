import csv
import connection
from tqdm import tqdm


class AMLUserKeyMap:
    key_to_index = {
        'JOB_DT': 0,
        'CUST_CODE': 1,
        'CUST_DIV_CD': 2,
        'CDD_CS_TYP_CD': 3,
        'AGNT_RET_CD': 4,
        'RNM_CNFR_CD': 5,
        'RNM_NO': 6,
        'CUST_NM': 7,
        'CUST_ENG_NM': 8,
        'NTN_CD': 9,
        'BRDT': 10,
        'SEX_CD': 11,
        'RSDNC_NTN_CD': 12,
        'RSDNC_F': 13,
        'HOME_PST_NO': 14,
        'HOME_ADDR': 15,
        'HOME_TEL_NO': 16,
        'WORK_PST_NO': 17,
        'WORK_ADDR': 18,
        'WORK_TEL_NO': 19,
        'CELL_PHONE': 20,
        'JOB_CD': 21,
        'PAPT_NO': 22,
        'RSK_GRD_CD': 23,
        'RSK_GRD_MRK': 24,
        'CDD_MAKE_MN_ID': 25,
        'CDD_MAKE_BR_CD': 26,
        'CDD_REG_DT': 27,
        'EDD_REG_DT': 28,
        'TR_OBJ_USE_CD': 29,
        'TR_OBJ_USE_ETC_NM': 30,
        'CAP_SRC_CD': 31,
        'CAP_SRC_ETC_NM': 32,
        'WP_NM': 33,
        'BUSI_NM': 34,
        'BSMN_RNO': 35,
        'EMP_CNT': 36,
        'YY_INCM_AMT_CD': 37,
        'ESTB_OBJ_NM': 38,
        'FNDTN_DT': 39,
        'ENTP_SCL_CD': 40,
        'LSTNG_YN_CD': 41,
        'LSTNG_INFO_CD': 42,
        'MAIN_GDS_SRVC_ETC': 43,
        'CS_ST_CD': 44,
        'CS_RGST_DT': 45,
        'CS_MDFY_DT': 46,
        'CS_DLT_DT': 47,
        'EMAIL': 48,
        'SECU_CERT_LVL': 49,
        'API_USE_YN': 50,
        'O_STO_F': 51,
        'EDD_EXE_YN': 52,
        'RE_KYC_DATE': 53,
        'INTERVIEW_CCD': 54,
        'IND_CLS_GB': 55,
        'IND_CLS_DT': 56,
        'CLOSE_DT': 57,
        'TL_ASTS_AMT': 58,
        'LG_AMT_ASTS_F': 59,
        'REST_F': 60,
        'REST_DT': 61,
        'ACDT_F': 62,
        'INDST_DVD_CD': 63,
        'DISPLAY_USER_NO': 64,
        'ENCRP_RNM_NO': 65,
        'IN_BLACKLIST_YN': 66,
        'RISK_TARGET_YN': 67,
        'TRUTH_ORG_CD': 68,
        'INFO_IMPOS_ORG_YN': 69,
        'NTN_PBLIC_GROUP_F': 70,
        'PMSN_F': 71,
        'AML_REF_DT': 72,
    }


def get_value(container, key):
    return container[AMLUserKeyMap.key_to_index[key]]


def set_value(container, key, data):
    container[AMLUserKeyMap.key_to_index[key]] = data


def test_process_raw_userdata():
    input_file = open('./processed/kyc_base_uncompleted.csv', 'r')
    csv_data = csv.reader(input_file)
    process_raw_userdata(csv_data=csv_data)


def process_raw_userdata(csv_data):
    output_file = open('./processed/kyc_processed_data.csv', 'w', newline='')
    output = csv.writer(output_file)

    for row in tqdm(csv_data):
        nation_code = get_value(row, 'NTN_CD')
        real_nation_code = get_value(row, 'RSDNC_NTN_CD')
        if '82' in nation_code:
            #  +82에서 KO로
            set_value(row, 'NTN_CD', 'KR')
        if 'RSDNCNTNCD' in real_nation_code or real_nation_code == 'ru':
            set_value(row, 'RSDNC_NTN_CD', 'KR')
        if real_nation_code == 'al' or real_nation_code == 'FAIL':
            set_value(row, 'RSDNC_NTN_CD', '')
        # postcode에 상세주소 들어가있던 이슈
        home_postcode = get_value(row, 'HOME_PST_NO')
        home_address = get_value(row, 'HOME_ADDR')
        if home_postcode.__len__() > 5:
            set_value(row, 'HOME_ADDR', home_address + home_postcode[5:])
            set_value(row, 'HOME_PST_NO', home_postcode[:5])
        gender = get_value(row, 'SEX_CD')
        if gender.__len__() == 0:
            set_value(row, 'SEX_CD', '9')
        customer_type = get_value(row, 'CDD_CS_TYP_CD')
        if customer_type.__len__() == 0:
            set_value(row, 'CDD_CS_TYP_CD', '01')

        # remove trash data
        for key in AMLUserKeyMap.key_to_index:
            if get_value(row, key) in ('Not collecting', 'NOTCOL', 'NOTCOLLECT', 'NOTCOLLECT'):
                set_value(row, key, '')
        output.writerow(row)


if __name__ == '__main__':
    test_process_raw_userdata()
