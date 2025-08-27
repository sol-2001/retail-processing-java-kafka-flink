# Retail Processing (Java + Kafka + Flink)

Проект стриминговой обработки событий из розницы: клиенты публикуют транзакции в **Kafka**, `flink-job` читает поток, валидирует, агрегирует и пишет в хранилище (sink).

## Архитектура
```mermaid
flowchart LR
  subgraph Producers
    A[streaming-clients] -->|JSON events| K[(Kafka topics)]
  end
  K --> F[Flink job]
  F --> S[(Sink DB / DWH)]
  F --> M[(Metrics/Logs)]
