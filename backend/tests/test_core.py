import pytest
from app.core import Pipeline, ordered, compile_pipeline, ValidationError

def sample():
    return Pipeline(nodes=[{'id':'src','type':'source','config':{'namespace':'sales','table':'orders'}},{'id':'flt','type':'filter','inputs':['src'],'config':{'expression':'amount > 0'}},{'id':'out','type':'target','inputs':['flt'],'config':{'namespace':'analytics','table':'daily_sales'}}])

def test_compiles():
    result=compile_pipeline(sample())
    assert 'spark.table' in result['code']
    assert '.filter(' in result['code']
    assert 'saveAsTable' in result['code']

def test_cycle():
    p=sample(); p.nodes[0].inputs=['out']
    with pytest.raises(ValidationError): ordered(p)

def test_no_target():
    p=sample(); p.nodes.pop()
    with pytest.raises(ValidationError): ordered(p)
