#-*- coding: UTF-8 -*-

from db.DB_connetion_pool import getPCSDConnection;
import numpy as np
import pandas as pd
import traceback
import warnings

warnings.filterwarnings('ignore')


def \
        query_all():
    with getPCSDConnection() as db:
       result= []
       try:
           query_sql = "select user_id,song_id,action,DATE_FORMAT(created_date, '%Y-%m-%d %T') from fdm_audio_calc_song_play_da where DATE_FORMAT(created_date, '%Y-%m-%d %T')<'2018-07-02 15:20:00' and DATE_FORMAT(created_date, '%Y-%m-%d %T')>'2018-06-02 15:20:00'"
           # query_sql = "select user_id,song_id,action,DATE_FORMAT(created_date, '%Y-%m-%d %T') from fdm_audio_calc_song_play_da "
           db.cursor.execute(query_sql)
           result_data = db.cursor.fetchall()
           print('共查找出', db.cursor.rowcount, '条数据')
           for r in result_data:
               if r[1]:
                result.append([bytes.decode(r[0]), bytes.decode(r[1]), bytes.decode(r[2]),str(r[3], encoding="utf-8")])
       except:
           traceback.print_exc()

    return result

def inser_many(result=[]):
    with getPCSDConnection() as db:
        try:
            query_sql = "SELECT login_id FROM t_name_gender "
            db.cursor.execute(query_sql)
            result_data = db.cursor.fetchall()
            list_ids = []#数据库中所有login_id
            if result_data:
               for r in result_data:
                  list_ids.append([bytes.decode(r[0]) if r[0] else None,1])
               dict_result = dict(list_ids)
               insert_data=[]
               for a in result:#过滤掉在数据库中的login_id,只留新增的
                   if a[0] not in dict_result.keys():
                       insert_data.append(a)
               result=insert_data
            insert_sql = """  insert into t_name_gender (login_id, login_name, gender) VALUES (%s, %s, %s)"""
            db.cursor.executemany(insert_sql, result)
            db.conn.commit()
        except:
            traceback.print_exc()

def delete_data( ):
    with getPCSDConnection() as db:
        try:
            delete_sql = """ delete from t_name_gender"""
            db.cursor.execute(delete_sql)
            db.conn.commit()
        except:
            traceback.print_exc()

def load_data_from_excel_to_db():
    data_x = pd.read_excel("../data/yangben.xlsx", sheetname='整理完数据', usecols=[1, 2, 3], index=False)
    data_x = data_x.astype(object).where(pd.notnull(data_x), None)
    train_data = np.array(data_x)
    train_x_list = train_data.tolist()
    print(train_x_list)
    result = [tuple(item) for item in train_x_list]
    inser_many()
if __name__ == '__main__':
    query_all()