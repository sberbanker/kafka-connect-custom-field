## Описание на русском
Изначально форк шаблонного проекта для SMT от [cjmatta](https://github.com/cjmatta/kafka-connect-insert-uuid).

Проект содержит кастомные классы для настройки CDC Debezium.
Сейчас доступны:
* трансформация _CurrentTimestamp_ - позволяет добавлять в сообщение временную метку, для использования в потребителе.
* трансформация _LowerCaseTopic_ - позволяет преобразовать название топика в нижний регистр.

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