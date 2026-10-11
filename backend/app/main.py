from fastapi import FastAPI, HTTPException
from .core import Pipeline, ValidationError, compile_pipeline, ordered

app = FastAPI(title='OpenSparkUI',version='0.1.0')
TABLES = {'sales':{'orders':{'columns':['order_id','customer_id','amount','order_date'],'size_bytes':858993459200},'customers':{'columns':['id','name','region'],'size_bytes':15728640}},'analytics':{'daily_sales':{'columns':['customer_id','amount'],'size_bytes':None}}}
@app.get('/api/v1/health')
def health(): return {'status':'ok','version':'0.1.0'}
@app.get('/api/v1/catalogs')
def catalogs(): return [{'id':'fixture','type':'fixture','name':'Demo Catalog'}]
@app.get('/api/v1/catalogs/fixture/namespaces')
def namespaces(): return list(TABLES)
@app.get('/api/v1/catalogs/fixture/tables')
def tables(namespace:str='sales'):
    if namespace not in TABLES: raise HTTPException(404,'Namespace not found')
    return [{'namespace':namespace,'name':k,**v} for k,v in TABLES[namespace].items()]
@app.post('/api/v1/pipelines/validate')
def validate(p:Pipeline):
    try: return {'valid':True,'ordered_nodes':[n.id for n in ordered(p)]}
    except (ValidationError,KeyError,TypeError,ValueError) as e: raise HTTPException(422,str(e))
@app.post('/api/v1/pipelines/compile')
def compile_job(p:Pipeline):
    try: return compile_pipeline(p)
    except (ValidationError,KeyError,TypeError,ValueError) as e: raise HTTPException(422,str(e))
