# OpenSparkUI — M1 Preview

Self-hosted visual Spark pipeline prototype. **Development preview only:** no authentication, no real catalog connectivity, and no Spark execution. Canvas is illustrative; Compile uses a fixed sample pipeline.

## Run
```sh
cp .env.example .env
docker compose up -d --build
```
Open http://localhost:8088 on the Docker host. For remote use, tunnel `ssh -L 8088:127.0.0.1:8088 user@host`.

## Tests
```sh
docker compose run --rm api pytest -q
```

API endpoints: `GET /api/v1/health`, `GET /api/v1/catalogs`, `GET /api/v1/catalogs/fixture/namespaces`, `GET /api/v1/catalogs/fixture/tables?namespace=sales`, `POST /api/v1/pipelines/validate`, `POST /api/v1/pipelines/compile`.

The code generator supports source, filter, select, join, aggregate, target. SQL nodes deliberately fail closed. Spark 3/4 are compilation profile labels only; runtime compatibility is not yet validated. Generated code is not executed by the service.

**Security:** bound to loopback; no authentication. Do not expose publicly or run generated jobs on production data without review.
