#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import pymysql
from sqlalchemy import create_engine # to insert pandas dataframe to db

from excel_and_pandas import _excel_read


"""
note: databases are in mysql default location: admin:///var/lib/mysql
"""


""" 
in this example an article sql database is created
later a table giving article details will be added to the database
as well as a table giving author details
"""
def _create_article_db():
    
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3')
    
    cursor=connection.cursor()
    
    new_db_request = "CREATE DATABASE IF NOT EXISTS articles_mysql"  

    # Execute the create database SQL statment through the cursor instance
    cursor.execute(new_db_request)
    connection.commit()

    cursor.close()
    connection.close()
    


"""
in this function a table containing article details is 
created from a pandas DataFrame object which contains the
relevant information
"""
#TODO: first check table to see if entry exists
# can be accroding to title && authors && year or something similar which
# uniquely characterizes the article
def _make_article_table_from(articles_df):
    """
    Parameters
    ----------
    articles_df is a DataFrame with article_id, year
    
    Creates
    -------
    sql table consisting of the articles_df info
    
    """
    
    _create_article_db()
    # connect to database to fill in excel data
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3',
                         db='articles_mysql')
    cursor=connection.cursor()
    
    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="saul",
                               pw="Tyxd.eghj3",
                               db="articles_mysql"))
    
    articles_df.to_sql('article_details', con = engine, if_exists = 'append', chunksize = 1000)

    cursor.close()
    

"""
in this function a table containing author details is 
created from a pandas DataFrame object which contains the
relevant information
"""
def _make_author_table_from(join_df):
    """
    Parameters
    ---------
    join_df is a DataFrame with article_id, author_ids
    
    Creates
    -------
    mysql db consisting only of the author ids
    
    """
    
    _create_article_db()
    # connect to database
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3',
                         db='articles_mysql')
    cursor=connection.cursor()
    
    # make new DataFrame index/author_id
    current_set = set()
    
    # for each row (article_id) go through authors
    # add 'art_id: author_id' to data_list
    for index, row in join_df.iterrows():
        author_list = _create_author_list(row[0])
        for an_author in author_list:
            current_set.add(an_author)
    
    # first query database to see if exists
    copy_cur_set = current_set.copy()
    show_stmt = "SHOW TABLES LIKE 'author_table'"
    cursor.execute(show_stmt)
    show_result = cursor.fetchone()
    if show_result:
        # just change current set to include those not listed
        for id in copy_cur_set:
            db_check_request = "SELECT * FROM articles_mysql.author_table WHERE author_id = (%s)" %id
            cursor.execute(db_check_request)    
            search_result = cursor.fetchall()
            if search_result:
                current_set.remove(id)
            connection.commit()
        
    if len(current_set) > 0:
        author_df = pd.DataFrame(current_set, columns = ['author_id'])   
    
        # create sqlalchemy engine
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="saul",
                                   pw="Tyxd.eghj3",
                                   db="articles_mysql"))
        
        author_df.to_sql('author_table', con = engine, if_exists = 'append', chunksize = 1000)

    cursor.close()
    
    
    
"""
in this function a table is
created which will serve as a bridge between authors and articles.

this is needed because of the has-many has-many relationship; e.g.
an article has many authors, and an author has many articles.

this table allows to retrieve all articles written by a 
given author.
"""
def _make_join_table_from(join_df):
    """
    Parameters
    ----------
    join_df is a DataFrame with article_id, author_ids
    
    Creates
    -------
    mysql db consisting article_id, (only one) author_id
    
    """
    
    # make new DataFrame article/author
    data_list = []
    
    # for each row (article_id) go through authors
    # add 'art_id: author_id' to data_list
    for index, row in join_df.iterrows():
        article_id = index
        author_list = _create_author_list(row[0])
        for an_author in author_list:
            data_list.append([article_id,an_author])
        
    aa_join_df = pd.DataFrame(data_list, columns = ['article_id', 'author_id']) 

    
    _create_article_db()
    # connect to database to fill in excel data
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3',
                         db='articles_mysql')
    cursor=connection.cursor()
    
    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="saul",
                               pw="Tyxd.eghj3",
                               db="articles_mysql"))
    
    aa_join_df.to_sql('article_author_join_table', con = engine, if_exists = 'append', chunksize = 1000)

    cursor.close()
    

"""
this is a helper function used in the _make_join_table function above
"""    
def _create_author_list(col_input):
    """
    assumes input is of form "id_1, id_2, ..."
    or just one integer: id
    
    Returns
    -------
    a list consisting of the author ids
    [1,2,3]
    
    """
    return_list = []
    if type(col_input) is int:
        return_list.append(col_input)
    else:
        # separate out by comma position
        str_array = col_input.split(", ")
        return_list = list( map(lambda x: int(x), str_array) )
    
    return return_list
    

"""
this illustrates how the join table can be used to 
get all the articles written by a single author
"""   
def get_all_article_ids_of(author_id):
    """ 
    assumes the join_table has been created
    returns the article ids of the author as a list
    """
    author_article_list = []
    article_id_col_number = 1
    
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3',
                         db='articles_mysql')
    cursor=connection.cursor()
    
    db_check_request = "SELECT * FROM articles_mysql.article_author_join_table WHERE author_id = (%s)" %author_id
    cursor.execute(db_check_request)    
    search_result = cursor.fetchall()
    for i in range(0, len(search_result)):
        author_article_list.append(search_result[i][article_id_col_number])
    
    cursor.close()
    
    return author_article_list



"""
this function illustrates how INNER JOIN (or more generally, JOIN)
can be used to add data to a table, in this
case, the article table
"""
def add_data_to_article_table(new_data_df):
    """
    Parameters
    ----------
    new_data_df is of form id| new_data
    
    Modifies
    --------
    the article_table to include the new data
    """
    
    _create_article_db()
    # connect to database to fill in excel data
    connection = pymysql.connect(host='localhost',
                         user='saul',
                         password='Tyxd.eghj3',
                         db='articles_mysql')
    cursor=connection.cursor()
    
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="saul",
                               pw="Tyxd.eghj3",
                               db="articles_mysql"))
    
    # make temporary table from the new_data
    new_data_df.to_sql('temp_table', con = engine, if_exists = 'replace', chunksize = 1000)
    connection.commit()

    # add title column to article_details table
    # first check if exists
    
    
    check_col_request = ("""SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                         WHERE TABLE_SCHEMA='articles_mysql'
                         AND TABLE_NAME='article_details'
                         AND COLUMN_NAME='title'
                         """)
    col_result = cursor.execute(check_col_request)
    if not col_result:     
        add_col_request = ("""ALTER TABLE article_details
                           ADD COLUMN title text""")
        cursor.execute(add_col_request)
    connection.commit()
    
    # join temp table with the article_details table             
    join_request = ("""UPDATE article_details  
                    INNER JOIN temp_table ON article_details.article_id = temp_table.id 
                    SET article_details.title = temp_table.title""")
    
    cursor.execute(join_request)
    connection.commit()
    
    # delete temp_table
    drop_request = "DROP TABLE temp_table"
    cursor.execute(drop_request)
    connection.commit()
    
    cursor.close()
    connection.close()

    
    
    
    
    
    




xl_path = r'/home/saul/python_projects/leibniz_examples/data_files/article_list.xlsx'
articles_df = _excel_read(xl_path)[0]
join_df = _excel_read(xl_path)[1]
_make_article_table_from(articles_df)
_make_author_table_from(join_df)
_make_join_table_from(join_df)

print(get_all_article_ids_of(1))

title_list = [[1, "the dno"], [2,"psdo and bvp"], [3, "dbar"]]
article_title_to_add_df = pd.DataFrame(title_list, columns = ['id', 'title'])
article_title_to_add_df = article_title_to_add_df.set_index('id')
add_data_to_article_table(article_title_to_add_df)



