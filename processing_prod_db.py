# -*- coding: utf-8 -*-
import csv
from connection import Connection
from tqdm import tqdm
from cryptography.fernet import Fernet

conn = Connection()

def aml_update_usd_value():
    query_text = '''
update crypto_transfer as aa
set
    amount = "volume" * bb.amount
from (
         select id,
                case a.symbol
                    when 'USDT' then 1
                    else
                        coalesce(
                                (select
                                     close_price
                                 from (
                                          select
                                              abs(EXTRACT(EPOCH FROM (a.created - created))) as diff,
                                              close_price
                                          from candles
                                          where
                                                  split_part(trading_pair_name, '-', 1) = a.symbol
                                            and created between a.created - (interval '712 day') and a.created + (interval '365 day')
                                      ) as b
                                 order by diff
                                 limit 1)
                            , 0)
                    end amount
         from crypto_transfer a
         where
            crypto_transfer_status in ('COMPLETED')
            and transaction_done_at > to_timestamp(to_char(now() - interval '30 day', 'YYYY-MM-DD'), 'YYYY-MM-DD')
            and symbol <> 'KOC'
            and amount = 0
     ) as bb
where
        aa.id = bb.id
;'''

    cur = conn.db.cursor()
    result = cur.execute(query_text)
    conn.db.commit()

    query_text = '''
update crypto_transfer as aa
set
    amount = "volume" * bb.amount
from (
         select id,
                case a.symbol
                    when 'USDT' then 1
                    else
                        coalesce(
                                (select
                                     close_price
                                 from (
                                          select
                                              abs(EXTRACT(EPOCH FROM (a.created - created))) as diff,
                                              close_price
                                          from candles
                                          where
                                                  split_part(trading_pair_name, '-', 1) = a.symbol
                                            and created between a.created - (interval '712 day') and a.created + (interval '365 day')
                                      ) as b
                                 order by diff
                                 limit 1)
                            , 0)
                    end amount
         from crypto_transfer a
         where
            crypto_transfer_status in ('COMPLETED')
            and transaction_done_at > to_timestamp(to_char(now() - interval '1 day', 'YYYY-MM-DD'), 'YYYY-MM-DD')
            and symbol = 'KLAY'
     ) as bb
where
        aa.id = bb.id
;'''

    cur = conn.db.cursor()
    result = cur.execute(query_text)
    conn.db.commit()

def aml_t_kyc_base():
    query_text = '''
select to_char(now(), 'YYYYMMDD'),
       p.unique_key,
       (case when u.user_type like '%INDIVIDUAL' then '1' else '2' end)::varchar,
       '01'::varchar,
       null,
       '99'::varchar,
       p.unique_key,
       p.name,
       p.customer_english_name::varchar,
       (case when country_code = '+82' then 'KR' else '' end)::varchar,
       p.birth_date,
       (case
            when p.gender is null or p.gender = '' then '9'
            when p.gender = 'male' then '1'
            when p.gender = 'female' then '2'
            else '9' end),
       (case when country_code = '+82' then 'KR' else '' end)::varchar,
       (case when country_code = '+82' then '1' else '0' end)::varchar,
       p.home_post_number::varchar,
       p.home_address::varchar,
       p.home_tel_number::varchar,
       p.work_post_number::varchar,
       p.work_address::varchar,
       p.work_tel_number::varchar,
       p.phone_number::varchar,
       p.job_code::varchar,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       (case
           when p.transaction_object_code = '99' or p.transaction_object_code is null then '99'
           else concat('0', p.transaction_object_code)
           end)::varchar as TR_OBJ_USE_CD,
       null,
       (case
           when p.capital_source_code is null then '9'
           else p.capital_source_code
           end)::varchar,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       '0'::varchar,
       coalesce(to_char(id_photo_kyc_last_censored_at, 'YYYYMMDD'), '20220210'),
       to_char(u.modified, 'YYYYMMDD'),
       '99991231'::varchar,
       u.email::varchar,
       null,
       null,
       null,
       null,
       null,
       '0'::varchar,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       null,
       (case when coalesce(u.is_blacklisted, false) = true then '1' else '0' end)::varchar,
       null,
       null,
       null,
       null,
       '2'::varchar,
       '2'::varchar,
       '99991231'::varchar
from "user" u
    left join (select *, row_number() over (partition by unique_key) as r from "profile") p on u.id = p.user_id and p.r = 1
    left join "verification" v on v.user_id = u.id
where
    id_photo_verification_status = 'VERIFIED'
    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and u.level > 2;'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/kyc_base_uncompleted.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/kyc_base_uncompleted.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)

def aml_t_kyc_base_decrypted_data():
    query_text = """
select to_char(now(), 'YYYYMMDD') as jobdt,
       p.unique_key               as cust_code,
       p.phone_number_first,
       p.phone_number_middle,
       p.phone_number_last,
       u.email_username,
       u.email_domain
from "user" u
         left join (select *, row_number() over (partition by unique_key) as r
                    from "profile") p on u.id = p.user_id and p.r = 1
         left join "verification" v on u.id = v.user_id
         left join "accounts" a on u.id = a.user_id and a.symbol = 'KRW'
where p.unique_key is not null
  and length(p.unique_key) > 0
  and p.unique_key != 'NULL'
  and u.level > 2;"""

    cur = conn.db.cursor()
    cur.execute(query_text)
    rows = cur.fetchall()
    cipher_suite = Fernet('IAZ488hye4PRwe72IvOYHq6FOQwlwuK02998rtpJ2co='.encode('utf-8'))
    DECRYPTION_ERR_STR = 'DECRYPT_ERR'

    with open('./processed/kyc_decrypted_data.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    f = open('./processed/kyc_decrypted_data.csv', 'w', newline='')
    output = csv.writer(f)
    for row in tqdm(rows):
        unique_key = row[1]
        phone_number = ''
        email = ''
        if row[3] and row[3].__len__() > 0:
            try:
                decrypted_middle_number = cipher_suite.decrypt(bytes(row[3].encode())).decode('utf-8')
                phone_number = f'{row[2]}{decrypted_middle_number}{row[4]}'
            except:
                print(f'phonenumber decryption failed, {row}')
                phone_number = DECRYPTION_ERR_STR
        if row[6] and row[6].__len__() > 0:
            email_username = row[5]
            encrypted_email_domain = row[6]
            try:
                decrypted_email_domain = cipher_suite.decrypt(bytes(encrypted_email_domain.encode())).decode('utf-8')
                email = f'{email_username}{decrypted_email_domain}'
            except:
                print(f'email decryption failed, {row}')
                email = DECRYPTION_ERR_STR

        # re_encrypted_phone = DECRYPTION_ERR_STR if phone_number == DECRYPTION_ERR_STR \
        #     else cipher_suite.encrypt(phone_number.encode('utf-8')).decode('utf-8')
        # re_encrypted_email = DECRYPTION_ERR_STR if email == DECRYPTION_ERR_STR \
        #     else cipher_suite.encrypt(email.encode('utf-8')).decode('utf-8')
        # output.writerow([unique_key, re_encrypted_phone, re_encrypted_email])  # len 100 len 120

        output.writerow([unique_key, phone_number, email])  # len 100 len 120

    f.close()


def aml_t_kyc_base_update_dormant():
    cur = conn.db.cursor()


def aml_t_kyc_base_update_exit():
    cur = conn.db.cursor()


def aml_t_ac_prod():
    query_text = '''
select to_char(now(), 'YYYYMMDD'),
       u.id,
       '99',                               -- 상품번호 기타
       '1',                                -- 계좌상태 정상 2: 비정상
       p.unique_key,
       p.unique_key,
       '999',                              -- 기관코드: 가상자산 어떻게 해야하는지 문의
       null,
       null,
       null,
       coalesce(to_char(id_photo_kyc_last_censored_at, 'YYYYMMDD'), '20220210'), -- 계좌개설일자
       null,
       '9',                                -- 사고여부 9: 알 수 없음
       null,
       null,
       '2',                                -- 휴면여부 1: 여 2: 부 9: 알 수 없음
       '',                                 -- 휴면일 YYYYMMDD
       coalesce(to_char(id_photo_kyc_last_censored_at, 'YYYYMMDD'), '20220210'),   -- 데이터 내부 입력일시(YYYYMMDDHH24MISS)
       to_char(u.modified, 'YYYYMMDD'),     -- 데이터 내부 수정일시
       '99991231'::varchar      -- 99991231'
from "user" u
         left join (select *, row_number() over (partition by unique_key) as r
                    from "profile") p on u.id = p.user_id and p.r = 1
         left join "verification" v on v.user_id = u.id
where
    id_photo_verification_status = 'VERIFIED'
    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and u.level > 2
;'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/t_ac_prod.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/t_ac_prod.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)


def aml_t_kyc_token_address():
    query_text = '''
select to_char(now(), 'YYYYMMDD'),
       u.id,
       w.network_name,
       w.symbol,
       wa.address,
       null, -- 목표꼬리표
       p.unique_key,
       coalesce(to_char(id_photo_kyc_last_censored_at, 'YYYYMMDD'), '20220210'),
       coalesce(u.modified, '20220210'),
       '2',
       '99991231'::varchar
from wallet_address wa
         left join wallet w on wa.wallet_id = w.id
         left join "user" u on wa.user_id = u.id
         left join (select *, row_number() over (partition by unique_key) as r from "profile") p on u.id = p.user_id and p.r = 1
         left join "verification" v on v.user_id = u.id
where
    id_photo_verification_status = 'VERIFIED'
    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and u.level > 2;'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/t_kyc_token_address.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/t_kyc_token_address.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)


def aml_t_tms_dl_buy_crypto():
    query_text = '''
select to_char(t.buy_order_at + interval '1 day', 'YYYYMMDD'),                                                                                      -- 작업일자	JOB_DT              TO_CHAR(SYSDATE,'YYYYMMDD')
       u.id,                                                                                                            -- 계좌번호	GNL_AC_NO
       to_char(t.buy_order_at, 'YYYYMMDD'),                                                                             -- 거래일자	DL_DT               거래일자(원거래)
       t.uuid,                                                                                                          -- 거래일련번호	DL_SQ               일련번호(원거래)
       '19',                                                                                                                   -- 거래채널코드	DL_CHNNL_CD
       '31',                                                                                                                   -- 거래종류코드	DL_TYP_CD
       '13',                                                                                                                   -- 거래수단코드	DL_WY_CD
       t.base_symbol,                                                                                                         -- 매매종목코드	STBD_CODE           코인 마켓심볼
       asset.english_name,                                                                                                          -- 매매종목명	STBD_NM             코인명
       t.volume,                                                                                                               -- 매매수량	DEAL_FLUC_QTY
       '5',                                                                                                                    -- 거래매체구분코드	DL_MD_CCD
       null,                                                                                                                   -- 국제수지코드	FXCH_DL_OBJ_CD
       to_char(t.modified, 'HH24MISS'),                                                                                        -- 거래시간	DL_TM               거래시간(반영시간)
       (t.amount * 1207.63)                                                                                     as DL_AMT,     -- 거래금액	DL_AMT              거래금액(원화환산가액)
       0,                                                                                                                      -- 거래후잔액	TFND_RA             제외 --> 0으로 등록
       'oasisexc',                                                                                                             -- 취급기관코드	OGN_CD              기관코드(오아시스)
       null,                                                                                                                   -- 취급지점코드	HANDLNG_BRN_CD
       'LS0031',                                                                                                               -- 이체금융기관코드	CPRTY_TRSNS_OGN_CD          코인출고 상대편 : 예)m00134
       'oasisexc',                                                                                                             -- 이체금융기관명	CPRTY_TRSNS_FOGN_NM         코인출고 상대편 : 오아시스
       null,                                                                                                                   -- 이체금융기관지점코드	CPRTY_TRSNS_DPRT_CD     코인출고 상대편 : null
       (select waa.address::text
       from (select address from wallet_address where symbol=t.base_symbol and t.seller_id=user_id limit 1) as waa) as rcvr_addr,                  -- 이체은행금융기관계좌번호	CPRTY_TRSNS_AC_NO
       p.unique_key,                                                                                                           -- 상대대체계좌명	CPRTY_TRSNS_AC_NM           코인출고 상대편 : 상대방 계정명
       null,                                                                                                                   -- 업체코드	BUSI_CD
       '2',                                                                                                                    -- 거래취소여부	CNCL_F
       null,                                                                                                                   -- 거래취소일시	CNCL_DY_TM
       (select waa.address::text
       from (select address from wallet_address where symbol=t.base_symbol and t.buyer_id=user_id limit 1) as waa) as sndr_addr,                  -- 지갑주소	WLT_ADR
       null,                                                                                                                   -- 목표꼬리표	DESTINATION_TAG                 목표꼬리표(리플,이오스)
       null,                                                                                                                   -- 체인아널리시스스코어	CHNANL_SOR              FDS
       '2',                                                                                                                    -- 투자유의종목매매여부	INVST_WRN_ITEM_TRD_F    1:여/2:부
       '99991231'::varchar                                                                                                     -- AML반영일자	AML_REF_DT
from "trades" t
    left join "user" u on t.buyer_id = u.id -- 10: buy crypto
    left join (select *, row_number() over (partition by unique_key) as r from "profile") p on t.seller_id = p.user_id and p.r = 1
    left join "verification" v on v.user_id = u.id
    left join asset on t.base_symbol = asset.symbol
where
    t.created > to_timestamp('2022-02-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    and id_photo_verification_status = 'VERIFIED'
    and u.level > 2
    and t.buyer_id != t.seller_id
    and t.base_symbol is not null

    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and t.buy_order_at between
            to_timestamp(to_char(now() - interval '1 day', 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
        and to_timestamp(to_char(now(), 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
;'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/t_tms_dl_buycrypto.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/t_tms_dl_buycrypto.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)


def aml_t_tms_dl_sell_crypto():
    query_text = '''
select to_char(t.sell_order_at + interval '1 day', 'YYYYMMDD'),                                                                                        -- 작업일자	JOB_DT              TO_CHAR(SYSDATE,'YYYYMMDD')
       u.id,                                                                                                                   -- 계좌번호	GNL_AC_NO
       to_char(t.sell_order_at, 'YYYYMMDD'),                                                                                        -- 거래일자	DL_DT               거래일자(원거래)
       t.uuid,                                                                                                                 -- 거래일련번호	DL_SQ               일련번호(원거래)
       '19',                                                                                                                   -- 거래채널코드	DL_CHNNL_CD
       '32',                                                                                                                   -- 거래종류코드	DL_TYP_CD
       '13',                                                                                                                   -- 거래수단코드	DL_WY_CD
       t.base_symbol,                                                                                                          -- 매매종목코드	STBD_CODE           코인 마켓심볼
       asset.english_name,                                                                                                     -- 매매종목명	STBD_NM             코인명
       t.volume,                                                                                                               -- 매매수량	DEAL_FLUC_QTY
       '5',                                                                                                                    -- 거래매체구분코드	DL_MD_CCD
       null,                                                                                                                   -- 국제수지코드	FXCH_DL_OBJ_CD
       to_char(t.modified, 'HH24MISS'),                                                                                        -- 거래시간	DL_TM               거래시간(반영시간)
       (t.amount * 1207.63)                                                                                     as DL_AMT,     -- 거래금액	DL_AMT              거래금액(원화환산가액)
       0,                                                                                                                      -- 거래후잔액	TFND_RA             제외 --> 0으로 등록
       'oasisexc',                                                                                                             -- 취급기관코드	OGN_CD              기관코드(오아시스)
       null,                                                                                                                   -- 취급지점코드	HANDLNG_BRN_CD
       'LS0031',                                                                                                               -- 이체금융기관코드	CPRTY_TRSNS_OGN_CD          코인출고 상대편 : 예)m00134
       'oasisexc',                                                                                                             -- 이체금융기관명	CPRTY_TRSNS_FOGN_NM         코인출고 상대편 : 오아시스
       null,                                                                                                                   -- 이체금융기관지점코드	CPRTY_TRSNS_DPRT_CD     코인출고 상대편 : null
       (select waa.address::text
       from (select address from wallet_address where symbol=t.base_symbol and t.buyer_id=user_id limit 1) as waa) as rcvr_addr,                  -- 이체은행금융기관계좌번호	CPRTY_TRSNS_AC_NO
       p.unique_key,                                                                                                           -- 상대대체계좌명	CPRTY_TRSNS_AC_NM           코인출고 상대편 : 상대방 계정명
       null,                                                                                                                   -- 업체코드	BUSI_CD
       '2',                                                                                                                    -- 거래취소여부	CNCL_F
       null,                                                                                                                   -- 거래취소일시	CNCL_DY_TM
       (select waa.address::text
       from (select address from wallet_address where symbol=t.base_symbol and t.seller_id=user_id limit 1) as waa) as sndr_addr,                  -- 지갑주소	WLT_ADR
       null,                                                                                                                   -- 목표꼬리표	DESTINATION_TAG                 목표꼬리표(리플,이오스)
       null,                                                                                                                   -- 체인아널리시스스코어	CHNANL_SOR              FDS
       '2',                                                                                                                    -- 투자유의종목매매여부	INVST_WRN_ITEM_TRD_F    1:여/2:부
       '99991231'::varchar                                                                                                     -- AML반영일자	AML_REF_DT
from "trades" t
         left join "user" u on t.seller_id = u.id
         left join (select *, row_number() over (partition by unique_key) as r from "profile") p on t.buyer_id = p.user_id and p.r = 1
         left join "verification" v on v.user_id = u.id
        left join asset on t.base_symbol = asset.symbol
where
    t.created > to_timestamp('2022-02-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    and id_photo_verification_status = 'VERIFIED'
    and u.level > 2
    and t.buyer_id != t.seller_id
    and t.base_symbol is not null

    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and t.sell_order_at between
            to_timestamp(to_char(now() - interval '1 day', 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
        and to_timestamp(to_char(now(), 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
;'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/t_tms_dl_sellcrypto.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/t_tms_dl_sellcrypto.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)


def aml_t_tms_dl_transfercrypto():
    query_text = '''
select to_char(t.transaction_done_at + interval '1 day', 'YYYYMMDD'),
       u.id,
       to_char(t.transaction_done_at, 'YYYYMMDD'),
       t.uuid,
       '19',
       (case when t.transfer_type = 'DEPOSIT' then '33' else '34' end), -- sell crypto
       '13',                                                            -- 거래수단, 기타
       t.symbol,
       asset.english_name,
       t.volume,
       '5',
       null,
       to_char(t.modified, 'HH24MISS'),
       (t.amount * 1207.63),
       0,
       'oasisexc',
       null,
       'LS0031',                                                        -- 상대편 기관코드
       'oasisexc',
       null,                                                            -- 상대편 취급지점코드
       (case when t.transfer_type = 'DEPOSIT' then t.sender_address else receiver_address end),
       (case
            when t.is_internal_transfer then (
                select
                    pp.unique_key::text
                from profile pp
                     inner join (select user_id
                                 from wallet_address
                                 where address = (case
                                                      when t.transfer_type = 'DEPOSIT' then t.sender_address
                                                      else t.receiver_address end)) as waa
                                on pp.user_id = waa.user_id
                where pp.user_id = waa.user_id
                  and pp.unique_key is not null
                  and length(pp.unique_key) > 0
                  and pp.unique_key != 'NULL'
                limit 1)
            else '' end) as CPRTY_TRSNS_AC_NM,
       null,
       (case
            when t.crypto_transfer_status = 'CANCELLED' or t.crypto_transfer_status = 'INSUFFICIENT' or
                 t.crypto_transfer_status = 'FAIL' then '2'
            when t.crypto_transfer_status = 'COMPLETED' then '1'
            else '9' end),
       null,
       t.sender_address,
       null,                                                            -- 목표꼬리표: 리플, 이오스
       null,                                                            -- chainalysis 스코어
       '2',                                                             -- 투자유의종목여부
       '99991231'::varchar
from "crypto_transfer" t
    left join "user" u on t.user_id = u.id
    left join (select *, row_number() over (partition by unique_key) as r from "profile") p on u.id = p.user_id and p.r = 1
    left join "verification" v on v.user_id = u.id
    left join asset on t.symbol = asset.symbol
where
    t.crypto_transfer_status in ('COMPLETED')
    and t.created > to_timestamp('2022-02-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    and id_photo_verification_status = 'VERIFIED'
    and t.transaction_done_at is not null
    and u.level > 2
    and t.sender_address is not null and receiver_address is not null
    and p.unique_key is not null
    and length(p.unique_key) > 0
    and p.unique_key != 'NULL'
    and amount > 0
    and t.transaction_done_at between
            to_timestamp(to_char(now() - interval '1 day', 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
        and to_timestamp(to_char(now(), 'YYYYMMDD'), 'YYYYMMDD HH24:MI:SS')
;
'''

    cur = conn.db.cursor()
    cur.execute(query_text)
    data = cur.fetchall()

    with open('./processed/t_tms_dl_transfercrypto.csv', 'r+', newline='') as f:
        f.read()
        f.seek(0)
        f.truncate()

    with open('./processed/t_tms_dl_transfercrypto.csv', 'w', newline='') as f:
        output = csv.writer(f)
        for row in data:
            output.writerow(row)


def test_decrypt_phonenumber():
    input_file = open('./processed/exchange_public_profile.csv', 'r')
    csv_data = csv.reader(input_file)
    targets = []
    index = 0
    for row in csv_data:
        targets.append({
            'first': row[2],
            'middle': row[3],
            'last': row[4],
            'mail_domain': row[1],
            'mail_username': row[0],
        })
        index += 1
    manual_decrypt_phonenumber(targets)


def manual_decrypt_phonenumber(target_list):
    cipher_suite = Fernet('IAZ488hye4PRwe72IvOYHq6FOQwlwuK02998rtpJ2co='.encode('utf-8'))
    DECRYPTION_ERR_STR = 'DECRYPT_ERR'
    for row in target_list:
        try:
            decrypted_middle_number = cipher_suite.decrypt(bytes(row['middle'].encode())).decode('utf-8')
        except:
            decrypted_middle_number = '복호화 실패, 데이터 에러'
        n1 = row['first']
        n3 = row['last']
        phone_number = f'{n1}{decrypted_middle_number}{n3}'
        print(phone_number)
        if 'mail_domain' in row:
            email_username = row['mail_username']
            encrypted_email_domain = row['mail_domain']
            try:
                decrypted_email_domain = cipher_suite.decrypt(bytes(encrypted_email_domain.encode())).decode('utf-8')
            except:
                decrypted_email_domain = '복호화 실패, 데이터 에러'
            email = f'{email_username}{decrypted_email_domain}'
            print(email)


if __name__ == '__main__':
    aml_update_usd_value()
    aml_t_kyc_base()
    aml_t_kyc_base_decrypted_data()
    aml_t_tms_dl_buy_crypto()
    aml_t_tms_dl_sell_crypto()
    aml_t_tms_dl_transfercrypto()
    aml_t_ac_prod()
    aml_t_kyc_token_address()

