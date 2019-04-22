#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 21 22:33:16 2019

@author: domenjemec
@file; wiki_page_analysis
"""
import pandas as pd
import codecs
import os
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime

out_dir = './PROCESSED_DATA'
out_file = '/processed' + str(datetime.today().date()).replace('-', '_') + '.csv'
source = './RAW_DATA/source.html'
sts_list = ['new', 'confirmed', 'updated', 'removed']
library = './RAW_DATA/library.csv'


def retrieve_new_pages(html_file):
    # setup variables
    df = pd.DataFrame()
    soup = BeautifulSoup(codecs.open(html_file, 'r').read(), 'html.parser')

    # retrieve all pages
    for child in soup.nav.ul.find_all('a'):
        df = df.append({'path': child.get('href'),
                        'name_source': child.get_text(),
                        'level_source': child.get('data-level')},
                       ignore_index=True)
    return df


def analyze_pages(new_df):
    # library caching
    ldf = pd.read_csv(library, parse_dates=['dateAdded'])
    ldf['status'] = np.nan

    # merge library and new page df
    join_df = pd.merge(ldf, new_df, how='outer', on='path')

    # STATUS config
    # removed status
    join_df.loc[join_df.name_source.isnull(), 'status'] = sts_list[3]

    # new status
    join_df.loc[join_df.name.isnull(), ['status', 'dateAdded']] = (sts_list[0], datetime.today().date())
    join_df.level.fillna(join_df.level_source, inplace=True)
    join_df.name.fillna(join_df.name_source, inplace=True)

    # compare prep
    join_df.level_source.fillna(join_df.level, inplace=True)
    join_df.level = join_df.level.astype(np.int64)
    join_df.level_source = join_df.level_source.astype(np.int64)

    # confirmed status
    join_df.loc[(join_df.name == join_df.name_source) &
                (join_df.level == join_df.level_source) &
                (join_df.status != sts_list[0]), ['status']] = sts_list[1]

    # clear name & level for update
    join_df.loc[join_df.status.isnull(), ['name', 'level']] = (np.nan, np.nan)
    # updated status
    join_df.loc[join_df.status.isnull(), 'status'] = sts_list[2]
    join_df.level.fillna(join_df.level_source, inplace=True)
    join_df.name.fillna(join_df.name_source, inplace=True)

    # drop unnecessary columns
    join_df = join_df.drop(columns=['level_source', 'name_source'])

    return join_df


def main():
    print('Starting')
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    out_df = retrieve_new_pages(source)
    out_df = analyze_pages(out_df)
    # print(out_df.head())

    out_df.to_csv(out_dir + out_file, sep=',', index=False)
    print('Success')


if __name__ == "__main__":
    main()
