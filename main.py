import config
from processing_prod_db import aml_t_ac_prod, aml_t_kyc_token_address, aml_t_tms_dl_transfercrypto, \
    aml_t_tms_dl_sell_crypto, aml_t_tms_dl_buy_crypto, aml_t_kyc_base_decrypted_data, aml_t_kyc_base
from processing_uncompleted_userdata import test_process_raw_userdata
from update_aml_db import update_aml_express_database

if __name__ == '__main__':
    print('start fetching production database')
    print('fetching base userdata...')
    aml_t_kyc_base()
    print('fetching encrypted userdata...')
    aml_t_kyc_base_decrypted_data()
    print('fetching trading logs...')
    aml_t_tms_dl_buy_crypto()
    aml_t_tms_dl_sell_crypto()
    print('fetching crypto transfer logs...')
    aml_t_tms_dl_transfercrypto()
    print('fetching accounts...')
    aml_t_ac_prod()
    print('fetching token addresses...')
    aml_t_kyc_token_address()
    print('finished fetching, process uncompleted userdata...')
    test_process_raw_userdata()
    print('finished processing, update aml express database...')
    update_aml_express_database()
    print('finished.')

