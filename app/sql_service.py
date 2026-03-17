import mysql.connector
import os

host = os.getenv("HOST","localhost")
user = os.getenv("USER","root")
password = os.getenv("PASSWORD",'root')
database = os.getenv("DATABASE","digital_hunter")


class SqlService():
    def __init__(self):
        self.conn = mysql.connector.connect(host= host,
                                            port= 3306,
                                            user= user,
                                            password= password,
                                            database= database)
        
    def run_query(self,query):
        try:
            cursor = self.conn.cursor(dictionary=True)

            cursor.execute(query)

            res = cursor.fetchall()

            return res
        
        except Exception as e:
            return e

    def find_targets_with_priority_level_1_or_2(self):
        query = """select entity_id, target_name, priority_level ,movement_distance_km from targets
                  where priority_level in (1,2)
                  and movement_distance_km > 5"""
        
        res = self.run_query(query)

        return res
    
    def count_signal_type(self):
        query = """select count(*) as total , signal_type from intel_signals
                  group by signal_type
                   order by count(*) desc"""
        
        return self.run_query(query)
    
    def find_3_unkown_entity_id(self):
        query = """select count(*) as 'total count', entity_id from intel_signals
                  where priority_level = 99
                  group by entity_id
                  order by count(*) desc
                  limit 3"""
        
        return self.run_query(query)
    
    def find_night_birds(self):
        query = """select targets.entity_id from targets
               inner join (select round(min(reported_lat),0) , round(max(reported_lat),0)  , round(min(reported_lon),0),round(max(reported_lon),0),
               entity_id , count(*)
                from intel_signals
               where HOUR(timestamp) between 8 and 20
               group by entity_id 
               having (round(min(reported_lat),0) + round(min(reported_lon),0)) = (round(max(reported_lat),0) + round(max(reported_lon),0))) as temp
                on targets.entity_id = temp.entity_id
               where movement_distance_km > 10

                  """
        
        return self.run_query(query)

    def get_lat_lon(self,entity_id:str):
        query = f"""select reported_lat ,reported_lon 
               from intel_signals
               where entity_id = '{entity_id}' """
        
        return self.run_query(query)

import math

def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(dlambda / 2)**2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def calculate_speed(dist_km, time_seconds):
    if time_seconds <= 0:
        return 0
    hours = time_seconds / 3600
    return dist_km / hours

import datetime


dt1 = datetime.datetime(2026, 3, 15, 11, 10, 55, 674765)
dt2 = datetime.datetime(2026, 3, 15, 10, 54, 39, 606012)


difference = dt1 - dt2

print(f"Difference: {difference}")


total_seconds = difference.total_seconds()
print(f"Total seconds: {total_seconds}")