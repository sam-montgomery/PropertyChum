import re
import psycopg2
from datetime import datetime
from config import load_config

class DatabaseInterface:
    @staticmethod
    def is_property_tracked(property_key):
        sql = """SELECT COUNT(*) FROM public."tProperty" WHERE "tProperty"."gPropertyKey" = %s;"""
        config = load_config()
        property_key = property_key.replace("'","''")
        try:
            with psycopg2.connect(**config) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (property_key,))
                    count = cur.fetchone()[0]
                    return count
        except (Exception, psycopg2.DatabaseError) as error:
            print("SELECT ERROR: " + str(error))
            return None

    @staticmethod
    def insert_property_tile(price, address, link):
        sql = """INSERT INTO public."tPropertyTile"( "sPrice", "sAddress", "sLink") VALUES (%s,%s,%s);"""
        vendor_id = None
        config = load_config()
        try:
            with  psycopg2.connect(**config) as conn:
                with  conn.cursor() as cur:
                    # execute the INSERT statement
                    cur.execute(sql,(price, address, link))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("INSERT ERROR: " + error)
        finally:
            return vendor_id
    
    @staticmethod
    def insert_price(price, propertyKey):
        sql = """INSERT INTO public."tPrice"("sPrice", "dTimeOfCapture", "gPropertyKey") VALUES (%s,%s,%s);"""
        vendor_id = None
        now = datetime.now()
        time_str = now.strftime("%Y-%m-%d %H:%M:%S")
        config = load_config()
        try:
            with  psycopg2.connect(**config) as conn:
                with  conn.cursor() as cur:
                    # execute the INSERT statement
                    cur.execute(sql,(price, time_str, propertyKey))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("INSERT ERROR: " + error)
        finally:
            return vendor_id  
      
    @staticmethod  
    def insert_property(address, features):
        propertyGuid = ''
        match = re.search(r'\bBT\d{1,2}\s?\d[A-Z]{2}\b', address)
        propertyGuid += address.split(",")[0].strip().replace(" ","")
        if match:
            propertyGuid += match.group(0).replace(" ","")

        sql1 = """INSERT INTO public."tProperty"( "sAddress", "gPropertyKey"""
        sql2 = f"""VALUES ('{address}', '{propertyGuid}'"""

        for feature in features:
            if feature[1] == "":
                break
            else:
                sql1 += f'", "{feature[0]}'
                if feature[0].startswith("s"):
                    sql2 += f", '{feature[1]}'"
                else:
                    sql2 += f", {feature[1]}"

        sql = sql1 + '") ' + sql2 + ");"
            
        vendor_id = None
        config = load_config()
        try:
            with  psycopg2.connect(**config) as conn:
                with  conn.cursor() as cur:
                    # execute the INSERT statement
                    cur.execute(sql)
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("INSERT ERROR: " + error)
        finally:
            return vendor_id
