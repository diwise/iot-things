# iot-things

## Development

### Dependencies

Start timescale and pgadmin (keycloak in its own yaml)

```bash
docker compose -f deployments/docker-compose.yaml up
```

### VSCode

Add this to launch.json

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch Package",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/iot-things/main.go",
            "args": [
                "-policies=${workspaceFolder}/assets/config/authz.rego",
                "-things=${workspaceFolder}/assets/data/things.csv"
            ],
            "env": {
                "POSTGRES_USER": "postgres",
                "POSTGRES_PASSWORD": "password",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DBNAME": "postgres",
                "POSTGRES_SSLMODE": "disable"
            }
        }
    ]
}
```

### API

The full route reference is `assets/docs/openapi.yaml` (served as `/openapi.yaml` with Redoc UI at `/docs`).

1: GET http://localhost:8080/api/v0/things?type=WasteContainer

2: GET http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

Response content types

**application/vnd.api+json** (list and get responses)

**application/json** (some responses)

**text/csv** (query export via `Accept: text/csv`)

There is no `application/geo+json` support in the code.

Add an Authorization header with a valid Bearer token. The default policy validates the JWT against its configured issuer and derives allowed tenants from the token claims; an arbitrary token is rejected.

#### Paging

offset - skip n rows

limit - limit response to n rows

_links_ object added to **application/vnd.api+json** response

_Link_ headers added to **application/geo+json** response

### Example response

2: GET http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

```json
{
    "data": {
        "id": "c91149a8-256b-4d65-8ca8-fc00074485c8",
        "type": "WasteContainer",
        "tenant": "default",
        "location": {
            "latitude": 62.390715,
            "longitude": 17.316868
        }
    },
    "included": [
        {
            "id": "ebc1747e-c20e-426d-b1d3-24a01ac85428",
            "type": "Function"
        }
    ]
}
```

3: GET http://localhost:8080/api/v0/things?type=WasteContainer&offset=2&limit=1

```json
{
    "data": [
        {
            "id": "c91149a8-256b-4d65-8ca8-fc00074485c8",
            "type": "WasteContainer",
            "location": {
                "latitude": 17.305868,
                "longitude": 62.390715
            },
            "tenant": "default"
        }
    ],
    "links": {
        "self": "/api/v0/things?type=WasteContainer&offset=2&limit=1",
        "first": "/api/v0/things?type=WasteContainer&offset=1&limit=1",
        "prev": "/api/v0/things?type=WasteContainer&offset=1&limit=1",
        "last": "/api/v0/things?type=WasteContainer&offset=3&limit=1",
        "next": "/api/v0/things?type=WasteContainer&offset=3&limit=1"
    }
}
```

_prev_ & _next_ only visible if valid

### Connect things

4: POST http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

to connect (include) one thing with another. POST a valid "thing" object.

```json
{
    "id": "level:001",
    "type": "Level",
    "location": {
        "latitude": 17.306768,
        "longitude": 62.390715
    },
}
```

### Update 

5: **PUT** http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

**PUT** update/replace a thing 

### Update attribute

5: PATCH http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

**PATCH** update/replace a things attribute

```json
{
    "attr":"value"
}
```

Add or replace _attr_ attribute with _value_

# Configuration

## Faktisk konfiguration (kod ar facit, HARM-002)
Precedens: default < miljovariabel < CLI-flagga. RabbitMQ konfigureras i ovrigt via `messaging.LoadConfiguration`.

| Variabel | Default | Notering |
| --- | --- | --- |
| `LISTEN_ADDRESS` | `0.0.0.0` | Galler bade publik server och kontrollserver |
| `SERVICE_PORT` | `8080` | Publik server (`/api/v0/things/...`) |
| `CONTROL_PORT` | `8000` | Kontrollserver: pprof, liveness, readiness-stubbar (`rabbitmq`, `timescale`) som returnerar OK |
| `POLICIES_FILE` | `/opt/diwise/config/authz.rego` | Kravs vid startup |
| `AUTHZ_ACCESS_OBJECT_ENABLED` | `false` | Switches between the `tenants` and `access` result models |
| `THINGS_FILE` | `/opt/diwise/config/things.csv` | Kravs vid startup, seedar things |
| `CONFIG_FILE` | `/opt/diwise/config/config.yaml` | Kravs vid startup |
| `POSTGRES_HOST` | (tom) |  |
| `POSTGRES_PORT` | `5432` |  |
| `POSTGRES_DBNAME` | `diwise` |  |
| `POSTGRES_USER` | (tom) |  |
| `POSTGRES_PASSWORD` | (tom) |  |
| `POSTGRES_SSLMODE` | `disable` |  |
| `POSTGRES_MAX_CONNS` | `10` |  |
| `POSTGRES_MIN_CONNS` | `2` |  |
| `POSTGRES_MAX_CONN_LIFETIME` | `30m` |  |
| `POSTGRES_MAX_CONN_IDLE_TIME` | `5m` |  |
| `POSTGRES_HEALTH_CHECK_PERIOD` | `30s` |  |
| `RABBITMQ_HOST` | (tom, kravs om inte avstangd) | Se `messaging.LoadConfiguration` |
| `RABBITMQ_PORT` | `5672` |  |
| `RABBITMQ_VHOST` | `/` |  |
| `RABBITMQ_USER` | `user` |  |
| `RABBITMQ_PASS` | `bitnami` |  |
| `RABBITMQ_DISABLED` | `false` |  |
| `RABBITMQ_INIT_TIMEOUT` | `10` | Sekunder |
| `LOG_LEVEL` | `debug` |  |

## CLI flags
 - `policies` - An authorization policy file
 - `authz-access-object` - Enable the access-object authorization policy result model
 - `things` - List of known things (`things.csv`)
 - `config` - A yaml file with configuration (`config.yaml`)
 - `loglevel` - Set the log level

## Configuration files
Alla tre filer kravs vid startup med standardvagarna ovan: `authz.rego`, `things.csv`, `config.yaml`.
Samtliga foljer med imagen under `/opt/diwise/config/` och kan ersattas med externa mounts vid deployment.

Health paths pa kontrollservern (`CONTROL_PORT`): `/health`, `/healthz`, `/livez`, `/readyz`, `/readyz/{check}`.

Externa Kubernetes- och Compose-definitioner finns inte i detta repo och ar darfor inte inventerade har.
