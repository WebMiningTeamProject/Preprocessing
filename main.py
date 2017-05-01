#! /usr/bin/env python3

"""
This module creates a bag of word for the existing news paper articles in table NewsArticles.
The BOW is saved in table NewsArticlesBOW.
A delta mechanism is implemented, i.e., only BOW for new articles are created.
"""
import configparser
import sys
import os
import logging
from DatabaseHandler import DatabaseHandler
from LemmatizationFilePreprocessing import LemmatizationFilePreprocessing
import argparse


def parse_args():
    """
    initiates the argparseres and returns configpath gotten from parser
    """
    parser = argparse.ArgumentParser(
        description='Crawls newspages', prog='crawler')

    parser.add_argument(
        '-c',
        type=str,
        nargs='?',
        dest='config',
        required=True,
        help='path to configfile')
    args = parser.parse_args()
    return args.config


def load_config(config_file):
    """
    Loads config from config file and returns it
    """
    cparser = configparser.ConfigParser()
    try:
        cparser.read(config_file)
        return cparser
    except Exception as exc:
        print(exc)
        sys.exit(1)


def insert_bow_of_new_articles(limit=10000000):
    """
    Selects URIs and texts of articles in NewsArticles that are not yet present in NewsArticlesBOW.
    Inserts URI and BOW into NewsArticlesBOW
    """
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    config_file = parse_args()
    conf = load_config(config_file)

    handler = DatabaseHandler(
        host=conf['DATABASE']['Host'],
        user=conf['DATABASE']['User'],
        password=conf['DATABASE']['Password'],
        db_name=conf['DATABASE']['DB']
    )

    total_number_of_articles = handler.execute(
        """
        SELECT count(*)
        FROM NewsArticles
        """)

    print("There are " + str(total_number_of_articles[0]['count(*)']) + " articles.")


    #find new URIs for which no BOW exists and get the text along with the URI
    new_uris = handler.execute(
        """
        SELECT source_uri as 'uri', text
        FROM NewsArticles
        WHERE source_uri
        NOT IN (SELECT source_uri FROM NewsArticlesBOW);
        """)

    number_of_new_articles = len(new_uris)
    print("There are " + str(number_of_new_articles) + " new articles.")

    counter = 0;
    for entry in new_uris:
        counter = counter + 1
        print("Processing article " + str(counter) + " of " + str(number_of_new_articles) + ".")
        print("Processing: " + entry['uri'])

        #transforming the URI to avoid SQL errors
        entry['uri'] = str.replace(entry['uri'], "'", "''")
        transformed_result = LemmatizationFilePreprocessing.string_transformation(str(entry['text']))
        bow = (' '.join(transformed_result))
        sql_insert_command = "INSERT INTO NewsArticlesBOW (source_uri, bow)" \
                             " VALUES ('" + entry['uri'] + "', '" + bow + "');"
        result = handler.execute(sql_insert_command)
        if counter == limit:
            return


def main():
    logging.basicConfig(level=logging.DEBUG)
    insert_bow_of_new_articles()


# to start, make sure you have a config.ini
# run in console: python main.py -c "config.ini"
if __name__ == "__main__":
    main()

