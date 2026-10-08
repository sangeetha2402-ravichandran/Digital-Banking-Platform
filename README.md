# Digital Banking Payment & Transaction Platform

**Java 17 | Spring Boot | Apache Kafka | PostgreSQL | Docker | REST APIs | Transactional Outbox | Saga Compensation**

A digital banking microservices project implementing payment APIs, account transfers, event-driven fraud processing, transaction persistence and failure compensation. Apache Kafka provides asynchronous communication between services. The architecture also includes a future IBM MQ/JMS integration with legacy banking systems.

## Architecture

![Digital Banking Platform Architecture](./Digital%20Banking%20Platform%20Architecture.png)

### Kafka Event Flow

![Kafka Event Flow](./Digital%20Banking%20Event%20Flow%20Architecture.png)

### Implemented Kafka Processing Flow

```text
Client / Postman
       |
       v
Payment Service (8083)
       |-- paymentdb / payments
       |-- paymentdb / outbox_events
       v
Outbox Publisher
       |
       v
Kafka: payment-created
       |
       v
Fraud Service (8084)
       |
       +-- APPROVED --> Kafka: payment-approved --> Ledger Service (8085)
       |                                           |
       |                                           v
       |                                     ledgerdb / ledger_transactions
       |
       +-- REJECTED --> Kafka: payment-rejected
```

The Kafka fraud approval/rejection branches and Ledger Service database insert have been tested. Ledger Service is configured to publish `notification-events`; complete downstream processing is not yet verified.

## Implemented Components

| Component | Technology | Functionality |
|---|---|---|
| Customer Service | Spring Boot, Spring Data JPA | Customer APIs and persistence in `customerdb` |
| Account Service | Spring Boot | Account operations used in fund transfers |
| Payment Service | Spring Boot, PostgreSQL | Payment requests, idempotency key, fund transfer, compensation and payment outbox |
| Fraud Service | Spring for Apache Kafka | Consume payment-created events; publish approved/rejected outcomes |
| Ledger Service | Spring for Apache Kafka, PostgreSQL | Consume approved events; persist ledger transactions |
| Outbox Publisher | Spring Boot, Kafka | Publish pending payment events from the database to Kafka |
| Local Infrastructure | Docker | PostgreSQL and Kafka containers |

## Service Ports

| Service | Port |
|---|---:|
| Customer Service | 8081 |
| Account Service | 8082 |
| Payment Service | 8083 | 
| Fraud Service | 8084 | 
| Ledger Service | 8085 | 
| Notification Service | 8086 |
| Integration Service | 8087 | 

## Payment Service

The Payment Service handles payment requests and fund transfers through REST APIs.

**Current payment processing:**

```text
Receive payment request
    |
    v
Check idempotency key
    |
    v
Validate source and target accounts
    |
    v
Create PENDING payment
    |
    v
Debit source account
    |
    v
Credit target account
    |
    +-- Success --> COMPLETED
    |
    +-- Failure --> Credit source account back
                          |
                          +-- COMPENSATED
                          +-- COMPENSATION_FAILED
```

### Request Idempotency

Payment requests use an idempotency key to prevent repeat submissions from creating duplicate payment requests. Retrying the same request with the same key returns the existing payment.

Example keys:

```text
REQ-1006
REQ-1007
REQ-1008
```

### Saga-Style Compensation

The implemented synchronous transfer flow compensates a successful debit when the following credit operation fails. Compensation outcomes include `COMPENSATED` and `COMPENSATION_FAILED`.

This synchronous account-transfer compensation is separate from the Kafka fraud/ledger event flow.

## Transactional Outbox Pattern

Payment data and a pending outbox event are written in one PostgreSQL transaction, avoiding a payment record being committed without a corresponding event record.

```text
Payment database transaction
    |-- Save payment
    +-- Save outbox event (PENDING)
                |
                v
          Outbox Publisher
                |
                v
             Kafka
                |
                v
      Mark outbox event PUBLISHED
```

Outbox event fields include:

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

The event payload is stored as JSON text in PostgreSQL. The current local flow uses an outbox publisher; multi-instance locking/claiming is a future enhancement.

## Kafka Event Contract

The Kafka payload uses a separate `PaymentEvent` DTO instead of publishing JPA entities directly.

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

### Kafka Topics

| Topic | Purpose | Current status |
|---|---|---|
| `payment-created` | Payment creation events for Fraud Service | Used |
| `payment-approved` | Fraud-approved payment events for Ledger Service | Used |
| `payment-rejected` | Fraud-rejected payment events | Published and tested |
| `notification-events` | Downstream notification events | Created; end-to-end verification pending |
| `payment-retry` | Failed-message retry routing | Created; processing planned |
| `payment-dlq` | Dead-letter messages after retries | Created; processing planned |

`payment-events` was an earlier outbox test topic and is not part of the target architecture.

**Local topic configuration:** 3 partitions; replication factor 1 (single-broker local development). Events that use the same Kafka message key are routed to the same partition, preserving their order within that partition.

## Fraud Service

Consumes `payment-created` and publishes fraud decisions.

**Current test rule:**

```text
amount <= 10000 -> APPROVED -> payment-approved
amount >  10000 -> REJECTED -> payment-rejected
```

Both decisions have been tested. The amount threshold is a development rule, not a production fraud decision engine.

## Ledger Service

Consumes `payment-approved` and inserts an approved payment record into `ledgerdb.ledger_transactions`.

Stored data includes:

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

The approved transaction insert has been tested successfully. The service is configured to publish `notification-events`, but complete downstream handling is not yet verified.

Fraud-rejected events are not posted to the ledger.

## PostgreSQL Databases

| Database | Contents |
|---|---|
| `customerdb` | Customer records |
| `paymentdb` | `payments`, `outbox_events` |
| `ledgerdb` | `ledger_transactions` |

PostgreSQL runs in Docker with host port `5433` mapped to container port `5432`.

### Database Configuration

Database credentials are passed through environment variables rather than committed into application configuration.

```powershell
$env:DB_PASSWORD="<your-local-db-password>"
```

Example Payment Service configuration:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5433/paymentdb
spring.datasource.username=banking
spring.datasource.password=${DB_PASSWORD}
```

Example Ledger Service configuration:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5433/ledgerdb
spring.datasource.username=banking
spring.datasource.password=${DB_PASSWORD}
```

## Docker Infrastructure

| Container | Service | Host connection |
|---|---|---|
| `banking-postgres` | PostgreSQL | `localhost:5433` |
| `banking-kafka` | Apache Kafka | `localhost:9092` |

Check running containers:

```powershell
docker ps
```

## Testing and Verification

### Kafka Topics

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server localhost:9092
```

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-topics.sh `
  --describe `
  --topic payment-created `
  --bootstrap-server localhost:9092
```

### Read Kafka Events

```powershell
docker exec -it banking-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic payment-created `
  --from-beginning `
  --property print.key=true `
  --property print.partition=true
```

For approved or rejected payment messages, replace `payment-created` with `payment-approved` or `payment-rejected`.

### Inspect Payment Outbox

```powershell
docker exec -it banking-postgres psql -U banking -d paymentdb
```

```sql
SELECT id, aggregate_id, event_type, status, created_at, published_at
FROM outbox_events
ORDER BY created_at DESC
LIMIT 10;
```

### Inspect Ledger Records

```powershell
docker exec -it banking-postgres psql -U banking -d ledgerdb
```

```sql
SELECT * FROM ledger_transactions ORDER BY id DESC;
```

### Verified Milestone

- Payment API and synchronous debit/credit compensation flow
- Payment request idempotency key
- Payment and outbox event persistence
- Outbox publishing to Kafka
- Kafka payment creation event consumed by Fraud Service
- Approved and rejected fraud decisions published to Kafka
- Approved payment consumed by Ledger Service
- Approved ledger record inserted into PostgreSQL

## Kafka Runtime Evidence

The following screenshots capture local runtime behaviour on **6 October 2026**. They document Kafka startup, producer and consumer configuration, payment event consumption, fraud event publishing, and outbox polling. They are supplementary evidence, not proof of every end-to-end workflow.

| Evidence | Observed result |
|---|---|
| Payment Service startup | Embedded Tomcat started on HTTP port `8083`; Kafka consumer configuration logged with bootstrap server `localhost:9092`, group `payment-group`, `enable.auto.commit=false`, `auto.offset.reset=earliest`, and `heartbeat.interval.ms=3000`. |
| Fraud Service producer | Kafka producer initialized as an **idempotent producer**. Fraud Service logged receipt of a payment and publication to `payment-approved`. |
| Payment event consumer | Payment Service logged receipt of a JSON payment event, including payment reference, amount `500.00`, currency `NZD`, and `status=COMPLETED`. This establishes consumption of a test event; it does not by itself establish completion of the new end-to-end asynchronous payment workflow. |
| Legacy test topic consumer | Consumer group `payment-group` received partition assignments for `payment-events` partitions `0`, `1`, and `2`, and logged a received payment event. |
| Outbox polling | Hibernate logged a query selecting pending rows from `outbox_events`, ordered by `created_at`. The SQL log proves that the poll query ran, not that a specific message was delivered or acknowledged. |

### Payment Service: Kafka Consumer Configuration

```properties
bootstrap.servers=localhost:9092
group.id=payment-group
auto.offset.reset=earliest
enable.auto.commit=false
heartbeat.interval.ms=3000
```

The service startup log confirms port `8083`. Offset commits and processing guarantees require separate verification; `enable.auto.commit=false` only disables automatic offset commits.

![Payment Service startup and Kafka consumer configuration](./docs/evidence/payment-consumer-config.png)

### Fraud Service: Kafka Approval Event

```text
Fraud Service received payment: PAY-11ab153c-f28b-4172-811d-ab61ce3d34f5
Fraud result published to: payment-approved
```

The logs also show a Kafka **idempotent producer**. Producer idempotence helps avoid duplicate writes from retries within Kafka's supported guarantees; it is not the same as application-level consumer idempotency.

![Fraud Service approval publishing](./docs/evidence/fraud-approval-published.png)

### Payment Service: JSON Payment Event Consumed

The Payment Service logged a payment event with the following representative fields:

```json
{
  "paymentReference": "PAY-c8fe8950-9d8e-4947-aa3c-5a084e8958de",
  "sourceAccountNumber": "ACC1001",
  "targetAccountNumber": "ACC1002",
  "amount": 500.00,
  "currency": "NZD",
  "paymentType": "TRANSFER",
  "status": "COMPLETED",
  "idempotencyKey": "REQ-1006"
}
```

This is a logged test event. Its `COMPLETED` status must not be interpreted as confirmation of IBM MQ or core banking processing.

![Payment JSON event consumed](./docs/evidence/payment-json-event.png)

### Kafka Partition Assignment and Outbox Polling

The consumer log shows `payment-events-0`, `payment-events-1`, and `payment-events-2` assigned to `payment-group`, followed by receipt of a test message. A Hibernate SQL log shows selection of pending `outbox_events` rows ordered by creation time.

`payment-events` is an **earlier test topic**, separate from the target `payment-created` flow.

![Kafka partition assignments and outbox query](./docs/evidence/partition-assignment-outbox-query.png)

---

## Technology Stack

| Area | Technology |
|---|---|
| Language | Java 17 |
| Application Framework | Spring Boot |
| REST | Spring Web / MVC |
| Persistence | Spring Data JPA / Hibernate |
| Database | PostgreSQL 16 |
| Messaging | Apache Kafka, Spring for Apache Kafka |
| Serialization | Jackson / JSON |
| Build | Maven |
| Containers | Docker |
| API Testing | Postman |
| Version Control | Git / GitHub |


```
