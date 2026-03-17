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
        query = """select intel_signals.entity_id , sum(intel_signals.distance_from_last)
                 from intel_signals
               inner join (select round(min(reported_lat),0) , round(max(reported_lat),0)  , round(min(reported_lon),0),round(max(reported_lon),0),
               entity_id , count(*)
                from intel_signals
               where HOUR(timestamp) between 8 and 20
               group by entity_id 
               having (round(min(reported_lat),0) + round(min(reported_lon),0)) = (round(max(reported_lat),0) + round(max(reported_lon),0))) as temp
               on intel_signals.entity_id = temp.entity_id
               group by intel_signals.entity_id , day(timestamp)
               having sum(distance_from_last) > 10
                  """
        
        return self.run_query(query)

    def get_lat_lon(self,entity_id:str):
        query = f"""select reported_lat ,reported_lon 
               from intel_signals
               where entity_id = '{entity_id}' """
        return self.run_query(query)

    def analyzing_escape_patterns_after_an_attack(self):
        query = """WITH not_destroyed (attack_id)
               as(
               select attack_id from damage_assessments
               where result != 'destroyed' ),
               
               attacks_not_destroyed
               as(
               select attacks.attack_id , 
               min(attacks.timestamp) as start_attack , attacks.entity_id
               from attacks
               inner join not_destroyed
               on attacks.attack_id = not_destroyed.attack_id
               group by attacks.attack_id),

               3_hour_befor
               as( select intel_signals.entity_id ,
                sum(intel_signals.distance_from_last) / ((max(intel_signals.timestamp) - min(intel_signals.timestamp) ) / 3600) as speed
                from intel_signals
               inner join attacks_not_destroyed
               on intel_signals.entity_id = attacks_not_destroyed.entity_id
               where hour(intel_signals.timestamp) < hour(attacks_not_destroyed.start_attack)
               and hour(intel_signals.timestamp) > hour(attacks_not_destroyed.start_attack) -3
               group by intel_signals.entity_id 
               ),

               3_hour_after
               as(select intel_signals.entity_id ,
                sum(intel_signals.distance_from_last) / ((max(intel_signals.timestamp) - min(intel_signals.timestamp) ) / 3600) as speed
                from intel_signals
               inner join attacks_not_destroyed
               on intel_signals.entity_id = attacks_not_destroyed.entity_id
               where hour(intel_signals.timestamp) > hour(attacks_not_destroyed.start_attack)
               and hour(intel_signals.timestamp) < hour(attacks_not_destroyed.start_attack) + 3
               group by intel_signals.entity_id
               )


               select 3_hour_after.entity_id , 3_hour_after.speed as efter ,
               3_hour_befor.speed as before , (((3_hour_after.speed - 3_hour_befor.speed) * 100) /3_hour_befor.speed) as percent
               from 3_hour_after
               inner join 3_hour_befor
               on 3_hour_after.entity_id = 3_hour_befor.entity_id
               where 3_hour_after.speed > 3_hour_befor.speed * 2
               """
         return self.run_query(query)