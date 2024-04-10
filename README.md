## Описание на русском
Изначально форк шаблонного проекта для SMT от [cjmatta](https://github.com/cjmatta/kafka-connect-insert-uuid).

Проект содержит кастомные классы для настройки CDC Debezium.
Сейчас доступны:
* трансформация _CurrentTimestamp_ - позволяет добавлять в сообщение временную метку, для использования в потребителе.
* трансформация _LowerCaseTopic_ - позволяет преобразовать название топика в нижний регистр.
* трансформация _TimestampConverter_ - позволяет преобразовывать date, time или timestamp между разными форматами, например из String в Unix epoch. Основа честно украдена у [howareyouo](https://github.com/howareyouo/kafka-connect-timestamp-converter). Для String изменён тип форматировщика с `java.text.SimpleDateFormat` на `java.time.format.DateTimeFormatter`, что позволяет использовать шаблоны с [необязательными параметрами](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
* трансформация _LowerCaseField_ - позволяет преобразовать названия полей в ключе или значении сообщения в нижний регистр.
* трансформация _StructConverter_ - 
* трансформация _ClearUnicodeNull_ - 

### <ins>Примеры использования</ins>

#### CurrentTimestamp

| Name                    |Description|Type|Default|Importance|
|-------------------------|---|---|---|---|
| `current_ts.field.name` | Field name for UUID | String | `current_ts` | High |

```json lines
"transforms": "currentTs",
"transforms.currentTs.type": "ru.rgs.kafka.connect.transforms.CurrentTimestamp$Value",
"transforms.currentTs.current_ts.field.name": "current_ts",
```
#### LowerCaseTopic
```json lines
"transforms": "lowerCaseTopic",
"transforms.lowerCaseTopic.type": "ru.rgs.kafka.connect.transforms.LowerCaseTopic"
```

#### TimestampConverter
| Name                    | Description                                                                                                                                              |Type| Default | Importance |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---|---------|------------|
| `fields` | The field containing the timestamp, or empty if the entire value is a timestamp| String |         | High       |
| `target.type` | The desired timestamp representation: string, unix, Date, Time, or Timestamp| String |         | High       |
| `format` | A DateTimeFormatter-compatible format for the timestamp. Used to generate the output when type=string or used to parse the input if the input is a string | String |         | Medium     |
| `timezone` | A Timezone name in IANA-compatible format or `system` for server timezone| String | `UTC`   | Low        |

Пример преобразования поля с исходным типом `datetimeoffset` из MS SQL Server с переменным количеством микросекунд.
```json lines
"transforms": "tsConverter",
"transforms.tsConverter.type": "ru.rgs.kafka.connect.transforms.TimestampConverter$Value",
"transforms.tsConverter.fields": "CalcDate",
"transforms.tsConverter.target.type": "Timestamp",
"transforms.tsConverter.format": "yyyy-MM-dd'T'HH:mm:ss[.[SSSSSS][SSSSS][SSSS][SSS][SS][S]]XXX",
"transforms.tsConverter.timezone": "Europe/Moscow",
```
| Значение CalcDate в сообщении | Результат в Postgres |
|-------------------------------|----------------------|
|`"2023-05-23T15:54:56+05:00"`|`2023-05-23 13:54:56.000`|
|`"2023-05-23T13:54:56.8662013+05:00"`|`2023-05-23 13:54:56.866`|
|`"2023-05-23T17:54:56.422+07:00"`|`2023-05-23 13:54:56.422`|

#### LowerCaseField
```json lines
"transforms": "keyToLower,valueToLower",
"transforms.keyToLower.type": "ru.rgs.kafka.connect.transforms.LowerCaseField$Key",
"transforms.valueToLower.type": "ru.rgs.kafka.connect.transforms.LowerCaseField$Value"
```

#### StructConverter
```json lines
"transforms": "keyStructCast,valueStructCast",
"transforms.keyStructCast.type": "ru.rgs.kafka.connect.transforms.StructConverter$Key",
"transforms.valueStructCast.type": "ru.rgs.kafka.connect.transforms.StructConverter$Value"
```

#### ClearUnicodeNull
```json lines
"transforms": "ClearUnicode",
"transforms.ClearUnicode.type": "ru.rgs.kafka.connect.transforms.ClearUnicodeNull$Value",
"transforms.ClearUnicode.fields": "dirtyfield",
"transforms.ClearUnicode.predicate": "IsFoo",

"predicates": "IsFoo",
"predicates.IsFoo.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
"predicates.IsFoo.pattern": "foo",
```