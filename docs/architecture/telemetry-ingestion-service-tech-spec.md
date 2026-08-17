# Техническая реализация и Архитектура

**Документ:** `docs/architecture/telemetry-ingestion-service-tech-spec.md`
**Статус:** Approved for Development
**Сервис:** telemetry-ingestion-service (Поддомен: Telemetry Ingestion & Real-Time IoT)

## 1. Технологический Стек и Зависимости

| Компонент | Технология | Назначение / Обоснование |
| :--- | :--- | :--- |
| **Runtime** | Java 21 (Virtual Threads + Netty) | Максимально производительный асинхронный I/O (Non-blocking I/O) для обработки тысяч параллельных TCP-соединений. |
| **Framework** | Spring WebFlux / Netty | Реактивный стек на базе Reactor/Netty вместо стандартного блокирующего Tomcat/Servlet. |
| **Time-Series DB** | TimescaleDB (PostgreSQL Extension) | Высокопроизводительное хранение временных рядов (Hyper-tables), оптимизированное для записи миллионов гео-точек. |
| **Cache & In-Memory** | Redis (Cluster) | Хранение мгновенного маппинга IMEI -> device_id, дедупликация точек по ключу device_id:timestamp. |
| **Messaging** | Apache Kafka (Avro) | Высокоскоростная отправка событий в топики партицированные по device_id. |
| **Binary Parsers** | Netty ByteBuf / Custom Codecs | Ультра-быстрый бинарный парсинг протоколов Teltonika/Wialon без создания лишних объектов в Heap (Zero-Copy). |

## 2. Архитектура Реактивного Конвейера (Reactive Ingestion Pipeline)

Сервис строится на базе паттерна Non-Blocking Reactive Pipeline:

```text
 [TCP/HTTP Connection]
          │
          ▼
 [Netty EventLoop Group] ──► [ByteBuf Binary Decoder] ──► [Redis Reactive Lookup] ──► [TimescaleDB Async Batch Write]
                               (Parse Lat/Lon/Sensors)    (Validate IMEI & Deduplicate)   (R2DBC Reactive Driver)
                                                                                                   │
                                                                                                   ▼
                                                                                      [Kafka Reactive Producer]
                                                                                        (KafkaSender / Avro)
```

*   **Netty Transport Layer:** Принимает соединение на Netty EventLoop. Никакие потоки не блокируются на ожидании сетевых пакетов.
*   **Zero-Copy Parser:** Считывает байты из ByteBuf напрямую без создания строк/промежуточных Java-объектов (минимизация работы Garbage Collector).
*   **Reactive Cache Check:** Используется ReactiveRedisTemplate для проверки права устройства на отправку данных и проверки ключа дедупликации `SETNX device_id:timestamp EX 300`.
*   **Batch Persistence & Streaming:** Запись в TimescaleDB происходит асинхронными батчами (по 500 точек или раз в 100 мс) через R2DBC (Reactive Relational Database Connectivity), параллельно шлется Avro-событие в Kafka.

## 3. Межсервисное Взаимодействие и Интеграции

### 3.1 Схема интеграционных связей

```text
 ┌──────────────────────────┐
 │ GPS-трекеры / Контроллеры│
 └─────────────┬────────────┘
               │ TCP / MQTT / HTTP
               ▼
 ┌──────────────────────────┐      Reactive R2DBC       ┌──────────────────────────┐
 │telemetry-ingestion-service├─────────────────────────►│ TimescaleDB (Time-Series)│
 └─────────────┬────────────┘                           └──────────────────────────┘
               │
               │ Kafka: TelemetryPointReceivedEvent
               │ (Partitioned by device_id)
               ▼
 ┌──────────────────────────┐                           ┌──────────────────────────┐
 │geofencing-alerting-service├─────────────────────────►│  predictive-risk-service │
 └──────────────────────────┘                           └──────────────────────────┘
```

### 3.2 Описание контрактов взаимодействия

**Входящие протоколы связи:**
*   `TCP Port 5001` — бинарный протокол Teltonika (FM1100/FMB920).
*   `TCP Port 5002` — протокол Wialon IPS.
*   `POST /api/v1/telemetry/json` — HTTP REST API для вебхуков от внешних телематических платформ.

**Исходящие события (Kafka Producers):**
*   `TelemetryPointReceivedEvent`:
    *   **Topic:** `logistics.telemetry.points.v1`
    *   **Partition Key:** `device_id` (Гарантирует, что все точки от одной машины попадают в одну партицию Kafka и обрабатываются строго хронологически!).
    *   **Payload (Avro):** `device_id`, `latitude`, `longitude`, `speed`, `altitude`, `timestamp`, `sensors_map`.

## 4. Архитектурные Паттерны и Highload-Оптимизации

### 4.1 Избегание нагрузки на Garbage Collector (GC Pressure)
При нагрузке 50,000+ точек/сек стандартное создание `new Object()` на каждый пакет заставит GC постоянно останавливать JVM (Stop-The-World pauses).
*   **Решение:** Использование Netty PooledByteBufAllocator для переиспользования буферов памяти вне Heap (Off-Heap Memory).

### 4.2 Партицирование и Ключи Kafka
Для исключения эффекта Race Condition при расчете движения машины:
*   Все события от одного `device_id` отправляются в Kafka с Partition Key = `device_id`.
*   Это гарантирует, что Kafka Streams в соседнем сервисе `geofencing-alerting-service` всегда будет читать точки одной машины последовательно.

### 4.3 Backpressure и Защита от Перегрузки (Resilience)
Если TimescaleDB или Kafka начинает тормозить:
*   Сервис использует механизмы Reactive Backpressure (Project Reactor). Входящие сетевые подключения дроппаются или дроппают ACK-ответы, заставляя трекеры снизить интенсивность отправки без падения JVM по OutOfMemoryError.

## 5. Требования к Хранению Данных (TimescaleDB Guidelines)

### 5.1 Структура Hyper-table
TimescaleDB автоматически партицирует таблицы по времени:

```sql
-- Базовая таблица точек
CREATE TABLE telemetry_points (
    time TIMESTAMPTZ NOT NULL,
    device_id UUID NOT NULL,
    location GEOMETRY(Point, 4326) NOT NULL,
    speed REAL,
    altitude REAL,
    sensors JSONB
);

-- Превращение в TimescaleDB Hypertables с интервалом партиции 1 день
SELECT create_hypertable('telemetry_points', 'time', chunk_time_interval => INTERVAL '1 day');

-- Автоматическое сжатие старых партиций (Compression Policy) через 7 дней
ALTER TABLE telemetry_points SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id'
);
SELECT add_compression_policy('telemetry_points', INTERVAL '7 days');
```

## 6. Observability и Эксплуатация

*   **Metrics (Micrometer + Prometheus):**
    *   `telemetry_ingestion_rate_events_total` (counter) — входящий поток точек/сек.
    *   `telemetry_invalid_points_total` (counter с тегами `reason=gps_drift|out_of_bounds|bad_time`) — количества отбракованных точек.
    *   `netty_direct_memory_bytes` (gauge) — использование Off-Heap памяти.
*   **Logging:** Минимальное логирование на уровне INFO/DEBUG в "горячем" пути передачи данных (High-performance logging). Ошибки логируются асинхронно через RingBuffer (LMAX Disruptor Appender).
