# Streaming Data Analytics — Assignment 2
### Live E-commerce Order Management System → Kafka → MongoDB / MySQL

**Industry (Assignment 1):** E-commerce
**Data source (Assignment 1):** Order Management System (OMS)

This repo implements the exact architecture submitted in Assignment 1:
**3 producers → 6 topics → 3 consumers → MySQL + MongoDB → dashboards.**

Instead of replaying a static CSV, `oms_simulator.py` is a **simulated live
e-commerce platform**. It runs an order-lifecycle state machine: orders are created
continuously and each one is then scheduled forward through payment → fulfillment →
delivery (or cancellation / return) over real elapsed time, emitting one event at
every transition. No two runs produce the same data.

---

## Architecture

```
1. ORDER MGMT SYSTEM       2. PRODUCERS            3. KAFKA CLUSTER          4. CONSUMER GROUP            5. STORAGE
─────────────────────      ────────────            ────────────────          ─────────────────            ─────────
Order events         ┐                        ┌── orders-topic ──────┐                                ┌─▶ MySQL
Customer events      ├──▶ CRM Service ────────┤                      ├──▶ Order & Payment ────────────┤   orders, payments,
                     ┘    Producer            └── crm-events-topic ─┐│    Consumer                    │   inventory, customers
                                                                    ││                                │
Payment events ─────────▶ Payment Gateway ────── payments-topic ────┘│                                │
                          Producer                                   │                                │
                                              ┌── fulfillment-topic ─┼──▶ Fulfillment & Inventory ────┤
Inventory events     ┐                        │                      │    Consumer                    │
Cancellation &       ├──▶ WMS / Logistics ────┼── inventory-topic ───┘                                └─▶ MongoDB
Return events        ┘    Producer            │                                                          event logs, returns,
                                              └── returns-topic ─────────▶ Returns & Analytics           tracking, customers
                                                  crm-events-topic ──────▶ Consumer
                                                                                                       6. DASHBOARDS & ALERTS
        ZooKeeper ensemble: cluster metadata, leader election, failure detection
```

### Producer → topic ownership

| Producer | Topics it owns | Source events |
|---|---|---|
| **CRM Service Producer** | `orders-topic`, `crm-events-topic` | Order events, Customer events (created / updated / address) |
| **Payment Gateway Producer** | `payments-topic` | Payment events (initiated / authorised / failed / refund) |
| **WMS / Logistics Producer** | `fulfillment-topic`, `inventory-topic`, `returns-topic` | Fulfillment scans, inventory movements, cancellations & returns |

### Consumer → topic subscription

| Consumer | Subscribes to | Writes to |
|---|---|---|
| **Order & Payment** | `orders-topic`, `payments-topic` | MySQL `orders`, `payments` · Mongo `oms_orders`, `oms_event_log`, `oms_alerts` |
| **Fulfillment & Inventory** | `fulfillment-topic`, `inventory-topic` | MySQL `inventory` · Mongo `oms_tracking`, `oms_orders`, `oms_event_log`, `oms_alerts` |
| **Returns & Analytics** | `returns-topic`, `crm-events-topic` | MySQL `customers` · Mongo `oms_returns`, `oms_customers`, `oms_return_reasons`, `oms_alerts` |

**Message keys:** `order_id` for order-lifecycle events, `sku` for inventory,
`customer_id` for CRM. With the default partitioner (`hash(key) % partitions`),
every event for one entity lands in the same partition and is consumed in the
sequence it occurred — `PLACED` can never be processed after `SHIPPED`.

**A note on consumer groups:** each consumer uses its own `group_id`. In Kafka,
members of one group *share* the partitions of the topics they subscribe to. Because
these three consumers have different subscriptions and different jobs, each needs its
own group so it receives the complete stream for its topics. The "Consumer Group"
box in the diagram is the logical stream-processing layer, not a single Kafka group id.

---

## Files

| File | Purpose |
|---|---|
| `oms_simulator.py` | The simulated live platform — catalog, customers, lifecycle engine, 6 event streams |
| `producer.py` | Three producer threads, each with its own `KafkaProducer`, publishing its own topics |
| `consumer.py` | Three role-based consumers with validation, enrichment, aggregation and alert rules |
| `sample_data/oms_sample_events.jsonl` | 2,036 generated events across all 6 streams |
| `sample_data/oms_orders_sample.csv` | 260 order-placed events flattened to CSV |
| `create_topics.sh` / `.bat` | Creates the six topics |
| `docker-compose.yml` | Kafka + ZooKeeper stack |

---

## How to run

**1. Start Kafka and create topics**
```bash
docker-compose up -d
./create_topics.sh sda-kafka-1        # Linux/macOS — use your container name
create_topics.bat sda-kafka-1         # Windows
```

**2. Python environment**
```bash
python -m venv venv
venv\Scripts\activate                 # Windows
# source venv/bin/activate            # macOS / Linux
pip install -r requirements.txt
```

**3. Producer** (terminal 1) — starts all three producers
```bash
python producer.py --rate 2 --speed 5
```

**4. Consumer** (terminal 2) — starts all three consumers
```bash
set MONGO_URI=mongodb+srv://...       # Windows;  export MONGO_URI=... on mac/linux
python consumer.py
```

Run consumers in separate terminals instead, if you want three clean screenshots:
```bash
python consumer.py --role order_payment
python consumer.py --role fulfillment_inventory
python consumer.py --role returns_analytics
```

Add `--mysql` to also populate the MySQL tables from the diagram (optional —
MongoDB alone satisfies the assignment).

**Verify from the Kafka CLI:**
```bash
docker exec -it sda-kafka-1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders-topic --from-beginning
```

### Producer flags

| Flag | Meaning |
|---|---|
| `--rate 5` | new orders created per tick |
| `--interval 0.5` | seconds between ticks |
| `--speed 5` | lifecycle multiplier — orders progress 5× faster (good for demos) |
| `--limit 200` | stop after N events |
| `--dry-run` | print without Kafka (useful for testing) |
| `--export f.jsonl` | also write every event to a file |
| `--seed 42` | reproducible run |

Regenerate the sample dataset:
```bash
python producer.py --dry-run --limit 2000 --speed 14 --seed 42 \
       --export sample_data/oms_sample_events.jsonl
```

---

## Alert rules (box 4: "flag anomalies")

| Alert | Raised by | Trigger | Severity |
|---|---|---|---|
| `PAYMENT_FRAUD_RISK` | Order & Payment | 3+ `PAYMENT_FAILED` on the same order | HIGH |
| `HIGH_VALUE_ORDER` | Order & Payment | order value above ₹5,000 | LOW |
| `OUT_OF_STOCK` | Fulfillment & Inventory | `stock_after_movement` reaches 0 | HIGH |
| `LOW_STOCK` | Fulfillment & Inventory | stock falls to ≤ 10 on a deduction | MEDIUM |
| `DEFECT_CLUSTER` | Returns & Analytics | same SKU returned 3+ times for the same reason | HIGH |

These map directly to the real-time decisions identified in Assignment 1 — pulling a
SKU from the storefront the moment stock hits zero, holding a repeated-failure payment
before settlement, and halting sales of a defective SKU while the return trail is fresh.
