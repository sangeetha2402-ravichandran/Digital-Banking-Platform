# Digital Banking Payment & Transaction Platform

A hands-on **Java / Spring Boot digital banking platform** built to practice production-style microservices, REST APIs, Saga compensation, the Transactional Outbox Pattern, Apache Kafka event-driven processing, PostgreSQL, Docker, and later AWS EKS / IBM MQ integration.



---

## Architecture



![Digital Banking Platform Architecture](./Digital%20Banking%20Platform%20Architecture.png)


### Kafka Event Flow

![Kafka Event Flow](./Digital%20Banking%20Kafka%20Event%20Flow%20Architecture.png)

The Kafka flow currently being implemented is:

```text
Client / Postman
      |
      v
Payment Service (8083)
      |
      +--> paymentdb
      |      +--> payments
      |      +--> outbox_events
      |
      v
Outbox Publisher
      |
      v
payment-created
      |
      v
Fraud Service (8084)
      |
      +-----------------------------+
      |                             |
      | APPROVED                    | REJECTED
      v                             v
payment-approved              payment-rejected
      |                             |
      v                             v
Ledger Service (8085)         Notification Service (next)
      |
      +--> ledgerdb
      |      +--> ledger_transactions
      |
      v
notification-events
      |
      v
Integration Service (next)
      |
      v
IBM MQ (next)
      |
      v
Legacy Core Banking System (next)
```

Failure handling planned in the Kafka architecture:

```text
Consumer failure
      |
      v
payment-retry
      |
      v
Retry exhausted
      |
      v
payment-dlq
```

---

## Repository Structure

```text
digital-banking-platform/
├── account-service/
├── customer-service/
├── fraud-service/
├── ledger-service/
├── payment-service/
├── .gitignore
├── Digital Banking Kafka Event Flow Architecture.png
├── Digital Banking Platform Architecture.png
├── Kafka Digital Banking Event Flow.png
└── README.md
```

---



## Local Service Ports

| Service | Port |
|---|---:|
| Customer Service | `8081` |
| Account Service | `8082` |
| Payment Service | `8083` |
| Fraud Service | `8084` |
| Ledger Service | `8085` |
| Notification Service | `8086`  |
| Integration Service | `8087`  |

---

## Databases Created So Far

PostgreSQL runs locally in Docker.

| Database | Service / Purpose |
|---|---|
| `customerdb` | Customer-related data |
| `paymentdb` | Payment Service |
| `ledgerdb` | Ledger Service |

### `paymentdb`

Main tables currently used:

```text
payments
outbox_events
```

### `ledgerdb`

Main table:

```text
ledger_transactions
```

The Ledger Service has been tested successfully and an approved transaction was persisted into `ledger_transactions`.

---

## Environment Variables

Database credentials are **not hardcoded** in `application.properties`.

Example:

```properties
spring.datasource.password=${DB_PASSWORD}
```

Set the environment variable in PowerShell before starting a database-backed service:

```powershell
$env:DB_PASSWORD="<your-local-db-password>"
```

Example datasource for Payment Service:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5433/paymentdb
spring.datasource.username=banking
spring.datasource.password=${DB_PASSWORD}
```

Example datasource for Ledger Service:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5433/ledgerdb
spring.datasource.username=banking
spring.datasource.password=${DB_PASSWORD}
```

---

## Docker Infrastructure

Current local containers:

```text
banking-postgres
banking-kafka
```

PostgreSQL host mapping:

```text
localhost:5433 -> container:5432
```

Kafka:

```text
localhost:9092
```

Check containers:

```powershell
docker ps
```

---

## Kafka Topics

The architecture topics created are:

```text
payment-created
payment-approved
payment-rejected
notification-events
payment-retry
payment-dlq
```

An older test topic may also still exist locally:

```text
payment-events
```

`payment-events` was used during the first Kafka / Outbox test and is not part of the final target event flow.

### Topic Configuration

For the local environment, each architecture topic was created with:

```text
Partitions:          3
Replication factor:  1
```

Why:

- **3 partitions** allow parallel processing.
- Events with the **same Kafka message key** are routed consistently to the same partition, preserving ordering for that key.
- **Replication factor 1** is used because the local environment currently has only one Kafka broker.
- In production, Kafka would use multiple brokers and a higher replication factor for high availability.

---

## Payment Service

The Payment Service is the main transaction entry point.

### Current responsibilities

```text
Receive payment request
        |
        v
Check idempotency key
        |
        v
Validate source account
        |
        v
Validate target account
        |
        v
Create payment as PENDING
        |
        v
Debit source account
        |
        v
Credit target account
        |
        +--> Success -> COMPLETED
        |
        +--> Credit failure -> compensate source account
                               |
                               +--> COMPENSATED
                               +--> COMPENSATION_FAILED
```

### Idempotency

The service prevents duplicate payments using an idempotency key.

A new request should use a new idempotency key, for example:

```text
REQ-1006
REQ-1007
REQ-1008
```

If the same idempotency key is sent again, the existing payment is returned instead of creating another payment.

---

## Saga Compensation

The project currently uses a synchronous Saga-style compensation flow for fund transfer.

Example:

```text
Debit source account
       |
       v
Credit target account
       |
       +--> success -> payment completed
       |
       +--> failure
              |
              v
        Credit source account back
              |
              +--> success -> COMPENSATED
              |
              +--> failure -> COMPENSATION_FAILED
```

This demonstrates how to handle partial failure in a distributed banking transaction.

---

## Transactional Outbox Pattern

The Payment Service implements an Outbox Pattern to avoid this failure:

```text
Payment saved successfully
but
Kafka publish fails
```

Instead:

```text
Payment DB transaction
        |
        +--> save payment
        |
        +--> save outbox event as PENDING
                    |
                    v
             Outbox Publisher
                    |
                    v
                  Kafka
                    |
                    v
          Mark event PUBLISHED
```

The outbox event contains fields such as:

```text
eventId
aggregateType
aggregateId
eventType
payload
status
createdAt
publishedAt
```

The payload is stored as PostgreSQL `TEXT` and contains JSON.

---

## PaymentEvent DTO

Kafka messages use a dedicated event object rather than publishing the JPA `Payment` entity directly.

Example structure:

```json
{
  "eventId": "unique-event-id",
  "eventType": "PAYMENT_CREATED",
  "paymentId": 8,
  "paymentReference": "PAY-...",
  "sourceAccountNumber": "ACC1001",
  "targetAccountNumber": "ACC1002",
  "amount": 500.00,
  "currency": "NZD",
  "status": "PENDING"
}
```

This keeps the Kafka contract independent from the database entity.

---

## Fraud Service

Fraud Service consumes:

```text
payment-created
```

Current learning rule:

```text
amount <= 10000
    -> APPROVED
    -> payment-approved

amount > 10000
    -> REJECTED
    -> payment-rejected
```

Both branches have been tested successfully.

Example console result:

```text
Fraud Service received payment: PAY-...
Fraud result published to: payment-approved

Fraud Service received payment: PAY-...
Fraud result published to: payment-rejected
```

---

## Ledger Service

Ledger Service consumes:

```text
payment-approved
```

It then creates a ledger record:

```text
payment-approved
       |
       v
Ledger Service
       |
       v
ledgerdb
       |
       v
ledger_transactions
```

The ledger record contains information such as:

```text
paymentId
paymentReference
sourceAccountNumber
targetAccountNumber
amount
currency
status
createdAt
```

The database insert has been tested successfully.

Ledger Service is also configured to publish the next event to:

```text
notification-events
```

Final verification of the complete downstream `notification-events -> Integration Service` flow is still pending.

---

## Rejected Payment Flow

Rejected payments **do not go to Ledger Service**.

Correct flow:

```text
payment-created
      |
      v
Fraud Service
      |
      v
payment-rejected
      |
      v
Notification Service
      |
      +--> Email
      +--> SMS
      +--> In-App notification
```

Notification Service has not yet been implemented.

---

## Approved Payment Flow

The target approved path is:

```text
Payment Service
      |
      v
payment-created
      |
      v
Fraud Service
      |
      v
payment-approved
      |
      v
Ledger Service
      |
      v
ledger_transactions
      |
      v
notification-events
      |
      v
Integration Service
      |
      v
IBM MQ
      |
      v
Legacy Core Banking System
```

Implementation currently reaches the **Ledger Service / ledger database** stage.

---

## Kafka Retry and DLQ

Topics already created:

```text
payment-retry
payment-dlq
```

Planned behavior:

```text
Consumer processing failure
       |
       v
payment-retry
       |
       v
Retry with backoff
       |
       +--> success -> continue normal processing
       |
       +--> max retries reached
                    |
                    v
               payment-dlq
```

Retry/DLQ processing logic is the next reliability enhancement after the remaining services are wired.

---

## Useful Kafka Commands

List topics:

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server localhost:9092
```

Describe a topic:

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-topics.sh `
  --describe `
  --topic payment-created `
  --bootstrap-server localhost:9092
```

Consume `payment-created`:

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic payment-created `
  --from-beginning `
  --property print.key=true `
  --property print.partition=true
```

Consume approved events:

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic payment-approved `
  --from-beginning
```

Consume rejected events:

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic payment-rejected `
  --from-beginning
```

---

## Useful PostgreSQL Commands

Payment database:

```powershell
docker exec -it banking-postgres psql -U banking -d paymentdb
```

Check latest outbox events:

```sql
SELECT
    id,
    aggregate_id,
    event_type,
    status,
    created_at,
    published_at
FROM outbox_events
ORDER BY created_at DESC
LIMIT 10;
```

Ledger database:

```powershell
docker exec -it banking-postgres psql -U banking -d ledgerdb
```

Check ledger transactions:

```sql
SELECT *
FROM ledger_transactions
ORDER BY id DESC;
```

---

## Technology Stack

| Area | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot |
| REST APIs | Spring Web / MVC |
| Persistence | Spring Data JPA / Hibernate |
| Database | PostgreSQL 16 |
| Messaging | Apache Kafka |
| Kafka Integration | Spring for Apache Kafka |
| JSON | Jackson |
| Build | Maven |
| Containers | Docker |
| API testing | Postman |
| Source control | Git / GitHub |
| Future orchestration | Kubernetes / AWS EKS |
| Future legacy integration | IBM MQ / JMS |

---

## Next Development Steps

1. Verify Ledger Service publishes `notification-events`.
2. Build **Notification Service (`8086`)** for rejected-payment notifications.
3. Build **Integration Service (`8087`)** consuming `notification-events`.
4. Add **IBM MQ / JMS** integration.
5. Implement `payment-retry`.
6. Implement `payment-dlq`.
7. Add consumer idempotency.
8. Add safe Outbox Publisher locking / claiming for multiple instances.
9. Add correlation IDs, structured logs, metrics, tracing and monitoring.
10. Containerize all services and deploy to AWS EKS.
11. Add CI/CD using Jenkins + Maven.
12. Add security using Spring Security / JWT and externalized secrets.

---

## Current Milestone

At the current milestone, the following flow has been implemented and tested:

```text
Postman
   |
   v
Payment Service
   |
   v
Payment DB + Transactional Outbox
   |
   v
Kafka payment-created
   |
   v
Fraud Service
   |
   +--> payment-approved
   |        |
   |        v
   |    Ledger Service
   |        |
   |        v
   |    ledger_transactions ✅
   |
   +--> payment-rejected ✅
```

The project will continue from here with **Notification Service, Integration Service, IBM MQ, retry/DLQ, security, observability, CI/CD and AWS EKS deployment**.
