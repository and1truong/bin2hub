bin2hub
====

Feeding events from MySQL binlog to EventHubs.

Check `config.json`, make sure all environments variables are provided.

Run the application:

```bash
CONFIG=./config.json go run ./cmd/main.go
```

##  Event schema

```json
{
    "type": "object",
    "properties": {
        "database": { "type": "string" },
        "table":    { "type": "string" },
        "payload":  { "type": "object" }
    }
}
```
