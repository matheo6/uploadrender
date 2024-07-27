import pandas as pd 
from fastapi import FastAPI
app = FastAPI()

castDf=pd.read_parquet('cleancastDataframe.parquet.gzip')
crewDf=pd.read_parquet('cleanCrewDataframe.parquet.gzip')
movieDf=pd.read_parquet('cleanMovieDataframe.parquet.gzip')


@app.get("/cantidad_filmaciones_mes")
async def cantidad_filmaciones_mes(month):
    monthEs=str(month).strip()
    return (str(movieDf['index'][movieDf['release_month']== month].count()))


@app.get("/cantidad_filmaciones_dia")
async def cantidad_filmaciones_dia(day):
    monthEs=str(day).strip()
    return (str(movieDf['index'][movieDf['release_day']== day].count()))

@app.get("/score_titulo")
async def score_titulo(name):
    return str(name),str(movieDf['release_date'][movieDf['original_title']==name].dt.year.iloc[0]),str(movieDf['popularity'][movieDf['original_title']==name].iloc[0])


@app.get("/votos_titulo")
async def votos_titulo(titulo_de_la_filmación):
    if movieDf['vote_count'][movieDf['original_title']==titulo_de_la_filmación].iloc[0]>2000:
        print(movieDf[['original_title','vote_count','vote_average']][movieDf['original_title']==titulo_de_la_filmación])
        return movieDf[['original_title','vote_count','vote_average']][movieDf['original_title']==titulo_de_la_filmación]


@app.get("/get_actor")
async def get_actor( nombre_actor ):
    retornoTotal=castDf.loc[castDf['name']==nombre_actor,['revenue']].sum().iloc[0]
    cantPeli= castDf.loc[castDf['name']==nombre_actor,['index']].count().iloc[0]
    promedio=castDf.loc[castDf['name']==nombre_actor,['revenue']].mean().iloc[0]
    return str(retornoTotal),str(cantPeli),str(promedio),str(castDf.loc[castDf['name']==nombre_actor,['revenue','name','original_title','index']])


@app.get("/get_director")
async def get_director( nombre_director ):
    exito=crewDf.loc[(crewDf['name']==nombre_director) & (crewDf['job']=='Director') ,['revenue']].sum().iloc[0]
    return str(exito),crewDf.loc[(crewDf['name']==nombre_director) & (crewDf['job']=='Director') ,['original_title','release_date','revenue','budget','job']]