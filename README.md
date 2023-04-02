Kafka Connect SMT to add current linux [timestamp]https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html)

This SMT supports inserting a timestamp into the record Key or Value
Properties:

| Name                    |Description|Type|Default|Importance|
|-------------------------|---|---|---|---|
| `current_ts.field.name` | Field name for UUID | String | `current_ts` | High |

Example on how to add to your connector:
```
transforms=currentTs
transforms.currentTs.type=ru.rgs.kafka.connect.smt.CurrentTimestamp$Value
transforms.currentTs.current_ts.field.name="current_ts"
```

Lots borrowed from the Apache Kafka® `InsertField` SMT