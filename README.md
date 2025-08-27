# Retail Processing (Java + Kafka + Flink)

Проект стриминговой обработки событий из розницы: клиенты публикуют транзакции в **Kafka**, `flink-job` читает поток, валидирует, агрегирует и пишет в хранилище (sink).

**Стек**

Java 17 · Apache Kafka · Apache Flink (DataStream API) · Gradle/Maven · Docker

**Что делает пайплайн:**

- Читает события из Kafka (ключевые топики: retail.sales, retail.returns)

- Парсит/валидирует вход (JSON → модель)

- Обрабатывает поздние события (watermarks / allowed lateness)

- Считает агрегаты

- Пишет результат в sink (БД/файлы) — выбор настраивается

## Архитектура
```mermaid
flowchart LR
  subgraph Producers
    A[streaming-clients] -->|JSON events| K[(Kafka topics)]
  end
  K --> F[Flink job]
  F --> S[(Sink DB / DWH)]
  F --> M[(Metrics/Logs)]
