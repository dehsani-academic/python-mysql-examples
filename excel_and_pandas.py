#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import pandas as pd


def _excel_read(xl_path):
    """
    will read in an excel file with
    article_id, author_id, publish_year
    
    there will be an authors table, an article table, and a 
    table to join the two
    
    article_details (table): id, year
    authors_details (table): id
    article_author_join_table: article_id, author_id

    Returns
    -------
    a DataFrame object representing the excel file
    zeros are placed in cells with no data
    and a DataFrame object for the joining of the tables
    
    """

    article_df = pd.read_excel(xl_path)
    article_df = article_df.loc[:, ~article_df.columns.str.contains('^Unnamed')]
    article_df = article_df.loc[:, ~article_df.columns.str.contains('^author_id')]
    article_df.dropna(axis=0, how='all', inplace=True)
    
    
    article_df['article_id']= article_df['article_id'].astype(int)
    article_df = article_df.set_index('article_id')
    
    # date could also be a datetime object
    article_df['publish_year']= article_df['publish_year'].astype(int)
    
    join_df = pd.read_excel(xl_path)
    join_df = join_df.loc[:, ~join_df.columns.str.contains('^Unnamed')]
    join_df = join_df.loc[:, ~join_df.columns.str.contains('^publish_year')]
    join_df.dropna(axis=0, how='all', inplace=True)
    
    join_df['article_id']= join_df['article_id'].astype(int)
    join_df = join_df.set_index('article_id')
    join_df['author_id']= join_df['author_id'].astype(str)
    
           
    return [article_df, join_df] 



xl_path = r'/home/saul/python_projects/leibniz_examples/data_files/article_list.xlsx'

[articles_df, aa_df] = _excel_read(xl_path)





