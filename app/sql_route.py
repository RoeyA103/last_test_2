from fastapi import APIRouter 
from fastapi.responses import StreamingResponse
from sql_service import SqlService
from maps_data.DigitalHunter_map import plot_map_with_geometry

sqls = SqlService()

route = APIRouter(prefix="/sql")

@route.get("/find_targets_with_priority_level_1_or_2")
def find_targets_with_priority_level_1_or_2():
    return sqls.find_targets_with_priority_level_1_or_2()

@route.get("/count_signal_type")
def count_signal_type():
    return sqls.count_signal_type()

@route.get("/find_3_unkown_entity_id")
def find_3_unkown_entity_id():
    return sqls.find_3_unkown_entity_id()

@route.get("/find_night_birds")
def find_night_birds():
    res =sqls.find_night_birds()
    [doc.pop("sum(intel_signals.distance_from_last)") for doc in res]
    return res

@route.post("/plot/{entity_id}")
def get_plot(entity_id:str):
    gopoints = sqls.get_lat_lon(entity_id)
    re_orderd_points = [(point['reported_lat'],point['reported_lon']) for point in gopoints]

    print(re_orderd_points)

    plot_buf = plot_map_with_geometry(coords=re_orderd_points)

    return StreamingResponse(plot_buf,media_type="image/png")



