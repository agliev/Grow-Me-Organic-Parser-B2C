import sqlite3
import pandas as pd
from source import get_industries



def sql_connect(path:str='data.db'):

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    return cursor

def industries_to_sql(table_exist:bool=True):

    cursor = sql_connect()
    industries = get_industries()

    if table_exist == False:
        cursor.execute('''
                create table industries(
                                    industry varchar(50)) 
                    ''')

    for industry in industries:
        cursor.execute(
            f"""
            insert into industries
            values('{industry}')
            """ )
            
    cursor.execute(
            f"""
            commit
            """ )



def sql_to_industries() -> list:

    cursor = sql_connect()
    
    industries = list(pd.DataFrame(
            cursor.execute(
                """
                select * 
                from industries            
                """
            ))[0])

    return industries 