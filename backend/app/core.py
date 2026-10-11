from typing import Literal
from pydantic import BaseModel, Field

class Node(BaseModel):
    id: str = Field(pattern=r'^[A-Za-z][A-Za-z0-9_]*$')
    type: Literal['source','filter','select','join','aggregate','sql','target']
    inputs: list[str] = Field(default_factory=list)
    config: dict = Field(default_factory=dict)

class Pipeline(BaseModel):
    ir_version: Literal['0.1'] = '0.1'
    name: str = 'Untitled'
    spark_profile: Literal['spark-3','spark-4'] = 'spark-3'
    nodes: list[Node]

class ValidationError(ValueError):
    pass

def ordered(p: Pipeline) -> list[Node]:
    ids = {n.id: n for n in p.nodes}
    if len(ids) != len(p.nodes): raise ValidationError('Duplicate node IDs')
    seen, visiting, result = set(), set(), []
    def visit(n):
        if n.id in visiting: raise ValidationError('Cycle detected')
        if n.id in seen: return
        visiting.add(n.id)
        for key in n.inputs:
            if key not in ids: raise ValidationError(f'Missing input: {key}')
            visit(ids[key])
        visiting.remove(n.id)
        seen.add(n.id); result.append(n)
    for n in p.nodes: visit(n)
    arity = {'source':0,'filter':1,'select':1,'join':2,'aggregate':1,'sql':1,'target':1}
    for n in result:
        if len(n.inputs) != arity[n.type]: raise ValidationError(f'{n.id}: expected {arity[n.type]} inputs')
    if not any(n.type == 'target' for n in result): raise ValidationError('Target node required')
    return result

def quoted(name: str) -> str:
    import re
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name): raise ValidationError(f'Invalid identifier: {name}')
    return '`'+name+'`'

def compile_pipeline(p: Pipeline) -> dict:
    nodes = ordered(p)
    lines = ['from pyspark.sql import SparkSession, functions as F', '', 'spark = SparkSession.builder.appName('+repr(p.name)+').enableHiveSupport().getOrCreate()']
    decisions = []
    for n in nodes:
        c = n.config; v = 'df_'+n.id
        if n.type == 'source':
            table = '.'.join(quoted(x) for x in [c['namespace'],c['table']])
            lines.append(f'{v} = spark.table({table!r})')
            decisions.append({'node':n.id,'rule':'OPT-008','action':'suggested','reason':'Catalog statistics not available in fixture mode; no size-dependent hints applied'})
        elif n.type == 'filter': lines.append(f'{v} = df_{n.inputs[0]}.filter({c["expression"]!r})')
        elif n.type == 'select':
            cols = c['columns']; lines.append(f'{v} = df_{n.inputs[0]}.select('+', '.join('F.col('+repr(quoted(x))+')' for x in cols)+')')
        elif n.type == 'join':
            keys = c['keys']; cond = ' & '.join(f'(F.col({("l."+quoted(k["left"]))!r}) == F.col({("r."+quoted(k["right"]))!r}))' for k in keys)
            lines.append(f'{v} = df_{n.inputs[0]}.alias("l").join(df_{n.inputs[1]}.alias("r"), {cond}, {c.get("join_type","inner")!r})')
        elif n.type == 'aggregate':
            groups = c['group_by']; aggs = c['aggregates']
            agg_expr = ', '.join(f'getattr(F, {a["function"]!r})(F.col({quoted(a["column"])!r})).alias({quoted(a["alias"])!r})' for a in aggs)
            lines.append(f'{v} = df_{n.inputs[0]}.groupBy('+', '.join('F.col('+repr(quoted(x))+')' for x in groups)+f').agg({agg_expr})')
        elif n.type == 'sql':
            raise ValidationError('SQL nodes require a SQL validation gateway; unsupported in this safe preview')
        elif n.type == 'target':
            table = '.'.join(quoted(x) for x in [c['namespace'],c['table']]); mode=c.get('mode','append')
            if mode not in ('append','overwrite','errorifexists','ignore'): raise ValidationError('Invalid write mode')
            lines.append(f'df_{n.inputs[0]}.write.mode({mode!r}).saveAsTable({table!r})')
    lines.append('spark.stop()')
    return {'code':'\n'.join(lines)+'\n','decisions':decisions,'spark_profile':p.spark_profile,'node_count':len(nodes)}
