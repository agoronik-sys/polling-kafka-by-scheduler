## polling-kafka-by-scheduler

Spring Boot (Java 21) сервис, который читает сообщения из Kafka **не через `@KafkaListener`**, а через `KafkaConsumer.poll(...)`, запуская опрос по расписанию (шедуллер).  
Поддерживаются **приоритетные и обычные сообщения**, а также **ограничение RPS отдельно для каждой из 4 систем**.

### Основная идея

- **Топик:** `processing` (4 партиции).
- Сообщения содержат заголовки:
  - **`X-System-Id`** – для какой системы предназначено сообщение (например, `SYSTEM_A`, `SYSTEM_B`, `SYSTEM_C`, `SYSTEM_D`).
  - **`X-Priority`** – приоритет, значение `HIGH` считается высоким приоритетом, остальные – низким.
- Для каждой системы создаётся **отдельный Kafka consumer** (своё `group.id`), все подписаны на один и тот же топик.
- Каждую секунду (по умолчанию):
  - делается `poll` сообщений;
  - фильтруются сообщения по системе (по заголовку `X-System-Id`);
  - делятся на **high-priority** и **low-priority**;
  - **сначала обрабатываются high-priority**, затем, если ещё есть свободный лимит по RPS, добираются low-priority.

### Технологии

- **Java 21**
- **Spring Boot 3.5.x**
- **Spring Kafka**
- **Spring Scheduling**
- **Docker Compose** для локального Kafka кластера

### Запуск локально

#### 1. Поднять Kafka и создать топик

В корне проекта:

```bash
docker compose up -d
```

Будут подняты:

- `zookeeper` (`confluentinc/cp-zookeeper`)
- `kafka` (`confluentinc/cp-kafka`)
- `kafka-init` – разово создаёт топик `processing` с **4 партициями**.

#### 2. Запустить Spring Boot приложение

```bash
mvn spring-boot:run
```

Приложение подключится к Kafka по адресу `localhost:9092` (см. `application.yml`).

### Конфигурация

Основные настройки находятся в `application.yml`:

- **Kafka:**
  - `spring.kafka.bootstrap-servers: localhost:9092`
  - `spring.kafka.consumer.max-poll-records` – максимальное количество сообщений за один `poll`.
- **Обработка (`processing.*`):**
  - `processing.topic` – имя топика (по умолчанию `processing`).
  - `processing.poll-interval-ms` – период опроса Kafka шедуллером (по умолчанию 1000 мс).
  - `processing.high-priority-value` – значение заголовка `X-Priority`, которое считается высоким приоритетом.
  - `processing.headers.system-id` – имя заголовка для идентификатора системы (по умолчанию `X-System-Id`).
  - `processing.headers.priority` – имя заголовка для приоритета (по умолчанию `X-Priority`).
  - `processing.systems` – список систем, для каждой:
    - `id` – значение заголовка `X-System-Id`;
    - `rps` – максимальное количество сообщений, обрабатываемых за один тик (примерно «сообщений в секунду» при `poll-interval-ms = 1000`).

Пример (по умолчанию):

```yaml
processing:
  topic: processing
  poll-interval-ms: 1000
  high-priority-value: HIGH
  headers:
    system-id: X-System-Id
    priority: X-Priority
  systems:
    - id: SYSTEM_A
      rps: 10
    - id: SYSTEM_B
      rps: 20
    - id: SYSTEM_C
      rps: 5
    - id: SYSTEM_D
      rps: 2
```

### Как работает приоритезация и RPS

Для каждой системы (`SYSTEM_A`, `SYSTEM_B`, и т.д.) в `PriorityPollingService`:

1. Берётся лимит `capacity = rps` данной системы.
2. Делается один `poll` из Kafka.
3. Из полученных сообщений оставляются только те, у которых заголовок `X-System-Id` совпадает с `id` системы.
4. Эти сообщения делятся на:
   - **high-priority** (`X-Priority == HIGH`),
   - **low-priority** (все остальные).
5. Сначала обрабатываются **только high-priority** сообщения, пока не исчерпан `capacity`.
6. Если после high-priority лимит ещё не выбран, **остаток заполняется low-priority** сообщениями.

Таким образом:

- При наличии большого количества high-priority сообщений low-priority могут подождать.
- Когда high-priority исчерпаны, RPS используется для догонки обычных сообщений.

### Структура кода

- `PollingKafkaBySchedulerApplication` – точка входа, включает шедуллер.
- `config/ProcessingProperties` – бин с конфигурацией (`processing.*`).
- `config/KafkaConsumerConfig` – настройка `ConsumerFactory`.
- `service/PriorityPollingService` – логика шедуллерного чтения, приоритезации и RPS.

### Дальнейшее развитие

- Добавить реальную бизнес-логику вместо логирования в `handleRecord(...)`.
- Вынести обработку по системам в отдельные сервисы/стратегии.
- Добавить метрики (Micrometer/Prometheus) для RPS, задержек, очередей и т.д.

