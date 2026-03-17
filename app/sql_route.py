from fastapi import APIRouter
from sql_service import SqlService

sqls = SqlService()

route = APIRouter(prefix="/sql")

@route.get("find_targets_with_priority_level_1_or_2")
def find_targets_with_priority_level_1_or_2():
    return sqls.find_targets_with_priority_level_1_or_2()

@route.get("count_signal_type")
def count_signal_type():
    return sqls.count_signal_type()

@route.get("find_3_unkown_entity_id")
def find_3_unkown_entity_id():
    return sqls.find_3_unkown_entity_id()

@route.get("find_night_birds")
def find_night_birds():
    return sqls.find_night_birds()