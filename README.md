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
                "-policies=${workspaceFolder}/assets/config/authz.develop.rego",
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

### Api

1: GET http://localhost:8080/api/v0/things?type=WasteContainer

2: GET http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

Accept headers

**application/vnd.api+json** (1) + (2)

**application/geo+json** (1)

**application/json** (1) + (2)


Add Authorization header with **any** Bearer token

#### Paging

page[number] - page number to fetch

page[size] - number of rows per page


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
            "longitude": 17.306868
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

3: GET http://localhost:8080/api/v0/things?type=WasteContainer&page[number]=2&page[size]=1

```json
{
    "data": [
        {
            "id": "c91149a8-256b-4d65-8ca8-fc00074485c8",
            "type": "WasteContainer",
            "location": {
                "latitude": 17.306868,
                "longitude": 62.390715
            },
            "tenant": "default"
        }
    ],
    "links": {
        "self": "/api/v0/things?type=WasteContainer&page[number]=2&page[size]=1",
        "first": "/api/v0/things?type=WasteContainer&page[number]=1&page[size]=1",
        "prev": "/api/v0/things?type=WasteContainer&page[number]=1&page[size]=1",
        "last": "/api/v0/things?type=WasteContainer&page[number]=3&page[size]=1",
        "next": "/api/v0/things?type=WasteContainer&page[number]=3&page[size]=1"
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
        "latitude": 17.306868,
        "longitude": 62.390715
    },
}
```

### Update 

5: PUT http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

PUT update/replace a thing 

### Update attribute

5: PATCH http://localhost:8080/api/v0/things/c91149a8-256b-4d65-8ca8-fc00074485c8

PATCH update/replace a things attribute

```json
{
    "attr":"value"
}
```

Add or replace _attr_ attribute with _value_

