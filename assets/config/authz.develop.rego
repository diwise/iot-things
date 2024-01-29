package example.authz

default allow := false

allow = response {
    #input.method == "GET"
    pathstart := array.slice(input.path, 0, 2)
    pathstart == ["api", "v0"]

    response := {
        "tenants": ["default"]
    }
}