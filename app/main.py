from fastapi import FastAPI
from sql_route import route as sql_rout
import uvicorn
app = FastAPI()

app.include_router(sql_rout)

@app.get("/")
def root():
    return {"message":"service healthy"}

if __name__=="__main__":
    uvicorn.run("main:app",host="0.0.0.0",port=8080,reload=True)