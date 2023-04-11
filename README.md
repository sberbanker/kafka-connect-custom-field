## Описание на русском
Изначально форк шаблонного проекта для SMT от [cjmatta](https://github.com/cjmatta/kafka-connect-insert-uuid).

Проект содержит кастомные классы для настройки CDC Debezium.
Сейчас доступны:
* предикат _HasHeaderKeyValue_ - позволяет применять SMT для сообщений, которые содержат в заголовке заданную пользователем пару "ключ - значение";
* трансформация _CurrentTimestamp_ - позволяет добавлять в сообщение временную метку, для использования в потребителе.

### Примеры использования

<ins>HasHeaderKeyValue</ins>

_Параметры_

| Название       | Описание                            | Тип    | Обязательное | По-умолчанию | Важность |
|----------------|-------------------------------------|--------|--------------|--------------|----------|
| `header_name`  | Название поля в заголовке сообщения | String | Да           |              | High     |
| `header_value` | Значение поля в заголовке сообщения | String | Да           |              | High     |

_Вариант применения в конфигурации источника для Debezium_
```json lines

```

Kafka Connect SMT to add current linux [timestamp](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html).

This SMT supports inserting a timestamp in milliseconds into the record Key or Value
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