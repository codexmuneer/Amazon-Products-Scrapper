# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

import sqlite3
class AmazonproductscrapperPipeline(object):

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = sqlite3.connect("productdetails.db")
        self.curr = self.conn.cursor()


    def create_table(self):
        self.curr.execute("""DROP TABLE IF EXISTS productdetails""")
        self.curr.execute("""CREATE TABLE productdetails(
        product_name TEXT,
        product_price REAL,
        product_stars REAL,
        product_ratings INTEGER
        )""")
    def process_item(self, item, spider):
        self.store_db(item)
        print("Pipeline: " + item['product_name'][0] )
        return item

    def store_db(self,item):
        self.curr.execute("INSERT INTO quotes_tb VALUES (?,?,?)",[
            (item['product_name'][0]),
            (item['product_price'][0]),
            (item['product_stars'][0]),
            (item['product_ratings'][0])
        ]
        )
        self.conn.commit()
