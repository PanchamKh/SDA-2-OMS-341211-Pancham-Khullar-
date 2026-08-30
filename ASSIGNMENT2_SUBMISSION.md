# Assignment 2 — Submission Answers
**Issue title:** `[A2][SDA-?] Pancham — E-commerce OMS Live Producer` (replace `SDA-?` with your section)

---

## Section
`SDA-?`  ← select your actual section (SDA-1 / SDA-2 / SDA-G)

## Full Name
Pancham

## Student ID / Roll Number
_(fill in)_

## Link to Your Assignment 1 Submission
_(paste your A1 GitHub issue URL)_

## GitHub Repository Link
_(paste your public repo URL)_

---

## Kafka Topic Name Used

Six topics, exactly as in the Assignment 1 architecture diagram — one per event stream:

```
orders-topic        payments-topic       fulfillment-topic
inventory-topic     returns-topic        crm-events-topic
```

Each created with 3 partitions (replication factor 1 on the single-broker local
setup; the production design in A1 specifies replication factor 3).

Primary topic for the demo screenshot: **`orders-topic`**.

---

## Sample Data Description

**Format:** JSON (JSON Lines for the static sample file; live JSON messages over Kafka)

**How it was generated:** Python — a purpose-built simulator (`oms_simulator.py`) using
`Faker` (locale `en_IN`) for customer names, emails, phones and addresses, plus `random`
for the order lifecycle. It is **not** a static CSV replay: the simulator runs an
order-lifecycle state machine, so every run produces different, genuinely live data.

**How the simulated platform works:**
- A catalog of **15 SKUs** across 5 categories with real opening stock levels, a pool of
  **200 customers** across 10 Indian cities, and **4 warehouses**.
- On each tick, new orders are created and existing orders advance:
  `PAYMENT_INITIATED → PAYMENT_AUTHORISED/FAILED → PROCESSING → PACKED → SHIPPED →
  DELIVERED`, with ~6% cancelled before packing and ~10% returned after delivery.
- The CRM stream runs alongside it — customer signups, segment changes and address
  updates fire independently of the order flow.
- Stock is a **live number**: decremented on every order, restored on every cancellation
  or return. Stock-out events are therefore real, not random flags.
- ~12% of payments fail once; ~4% of orders are seeded as fraud patterns that fail 3–5
  times in a row, which is what the Order & Payment consumer's fraud rule detects.

**Six event streams, mapped to the A1 data-source table:**

| A1 category | Topic | Event types |
|---|---|---|
| Order Information, Order Details, Product Information | `orders-topic` | `ORDER_PLACED` |
| Payment Information | `payments-topic` | `PAYMENT_INITIATED`, `PAYMENT_AUTHORISED`, `PAYMENT_FAILED` |
| Order Fulfilment, Shipping Information | `fulfillment-topic` | `ORDER_PROCESSING`, `ORDER_PACKED`, `ORDER_SHIPPED`, `ORDER_DELIVERED` |
| Inventory Information | `inventory-topic` | `STOCK_DEDUCTED`, `STOCK_RESTOCKED_CANCEL`, `STOCK_RESTOCKED_RETURN` |
| Cancellation/Return | `returns-topic` | `ORDER_CANCELLED`, `RETURN_RAISED` |
| Customer Information | `crm-events-topic` | `CUSTOMER_CREATED`, `CUSTOMER_UPDATED`, `ADDRESS_UPDATED` |

Time/Event Information (`event_timestamp`, IST, ISO-8601) is present on every event.

**Volume in the repo:** `sample_data/oms_sample_events.jsonl` — **2,036 events**
covering 260 complete order journeys:

| Event | Count | | Event | Count |
|---|---|---|---|---|
| ORDER_PLACED | 260 | | ORDER_DELIVERED | 197 |
| PAYMENT_INITIATED | 260 | | PAYMENT_FAILED | 50 |
| PAYMENT_AUTHORISED | 254 | | RETURN_RAISED | 18 |
| ORDER_PROCESSING | 229 | | ORDER_CANCELLED | 14 |
| ORDER_PACKED | 223 | | ADDRESS_UPDATED | 11 |
| ORDER_SHIPPED | 213 | | CUSTOMER_UPDATED | 9 |
| STOCK_DEDUCTED | 260 | | CUSTOMER_CREATED | 6 |
| STOCK_RESTOCKED (cancel + return) | 32 | | | |

By producer: WMS/Logistics 1,186 · Payment Gateway 564 · CRM Service 286.
`sample_data/oms_orders_sample.csv` holds the 260 order-placed events flattened to CSV.

**Sample record:**

```json
{
  "event_id": "3f1c8b2a-...",
  "event_type": "ORDER_PLACED",
  "event_timestamp": "2026-08-26T17:42:07+05:30",
  "order_id": "ORD-2026-600001",
  "customer_id": "CUST-10042",
  "customer_location": "Delhi NCR",
  "customer_segment": "Repeat",
  "product_id": "PRD-2001", "sku": "SKU-2001-WHT-STD",
  "product_name": "Wireless Earbuds", "category": "Electronics",
  "quantity": 2, "unit_price": 3499.0, "discount": 699.8,
  "subtotal": 6998.0, "shipping_charges": 0.0, "total_order_value": 6298.2,
  "payment_method": "UPI", "delivery_method": "Express",
  "warehouse_id": "WH-DEL-01", "stock_at_order": 45,
  "_source_producer": "crm-service"
}
```

---

## Producer Code Description

`producer.py` implements **three independent producers**, matching box 2 of the A1
architecture. Each runs in its own thread with its own `KafkaProducer` instance,
its own `client_id`, and its own set of topics:

| Producer | Topics |
|---|---|
| CRM Service Producer | `orders-topic`, `crm-events-topic` |
| Payment Gateway Producer | `payments-topic` |
| WMS / Logistics Producer | `fulfillment-topic`, `inventory-topic`, `returns-topic` |

Each is configured with `value_serializer=json.dumps(...).encode()`,
`key_serializer`, `acks="all"` (wait for all in-sync replicas), `retries=5` and
`linger_ms=10`, and each verifies its topics exist via `partitions_for()` before
starting, so a missing topic fails fast with a clear message.

**How it reads data:** it does not read a file. The simulated platform runs in the
main thread; `sim.tick()` returns a list of `(producer, topic, key, event)` tuples —
the new orders created in that tick, plus every existing order whose next lifecycle
step has come due, plus any CRM activity. Each event is pushed onto the queue of the
producer that owns it, and that producer's thread publishes it. This is why the stream
is live: one tick can emit an `ORDER_PLACED` for a new order and an `ORDER_SHIPPED`
for an order placed thirty seconds earlier.

**Message format:** UTF-8 JSON, one event per message, 0.4–1.2 KB each.

**Keying / routing:** order-lifecycle events are keyed on `order_id`, inventory events
on `sku`, CRM events on `customer_id`. With the default partitioner
(`hash(key) % number_of_partitions`), all events for one entity go to the same
partition and are consumed in the order they occurred.

**Frequency:** default 2 new orders per second. Because each order emits 8–10 events
across its lifecycle, steady-state throughput settles at roughly **10–20 events/second**.
`--rate` and `--interval` control the load; `--speed` compresses the lifecycle so a full
order journey completes in seconds rather than minutes for the demo.

**Consumers.** `consumer.py` implements the three consumers from box 4, each
subscribing only to its own topics, each with its own `group_id`, each running
validation → enrichment → aggregation → anomaly flagging:

| Consumer | Topics | Writes |
|---|---|---|
| Order & Payment | `orders-topic`, `payments-topic` | MySQL `orders`, `payments`; Mongo `oms_orders`, `oms_event_log`, `oms_alerts` |
| Fulfillment & Inventory | `fulfillment-topic`, `inventory-topic` | MySQL `inventory`; Mongo `oms_tracking`, `oms_event_log`, `oms_alerts` |
| Returns & Analytics | `returns-topic`, `crm-events-topic` | MySQL `customers`; Mongo `oms_returns`, `oms_customers`, `oms_return_reasons`, `oms_alerts` |

Alert rules: `PAYMENT_FRAUD_RISK` (3+ failed payments on one order), `OUT_OF_STOCK`,
`LOW_STOCK` (≤10 units), `HIGH_VALUE_ORDER` (>₹5,000) and `DEFECT_CLUSTER` (same SKU
returned 3+ times for the same reason). MySQL is optional via `--mysql`; MongoDB alone
satisfies the assignment's minimum-one-collection requirement.

---

## Terminal Output

> Example run below — **replace this with the output of your own run and attach your
> screenshot.** Take the screenshot after ~70 events so fulfillment, cancellation and
> CRM events all appear.

```
Connecting three producers to Kafka at localhost:9092 ...
✅ CRM Service Producer      connected → orders-topic, crm-events-topic
✅ Payment Gateway Producer  connected → payments-topic
✅ WMS / Logistics Producer  connected → fulfillment-topic, inventory-topic, returns-topic


🏬 LIVE e-commerce OMS event stream — Ctrl+C to stop
   2 new order(s) every 0.5s | 3 producers | 6 topics | 15 SKUs | 4 warehouses

──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
[    1] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600001 | SKU-1003-GRY-9 x1 | ₹2239.20 | Pune
[    2] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600002 | SKU-1001-BLK-M x1 | ₹858.10 | Kolkata
[    3] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-1003-GRY-9 | -1 | stock=59 | WH-BLR-03
[    4] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-1001-BLK-M | -1 | stock=119 | WH-DEL-01
[    5] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600001 | NET_BANKING | ₹2239.20 | PENDING
[    6] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600002 | UPI | ₹858.10 | PENDING
[    7] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600003 | CREDIT_CARD | ₹903.05 | PENDING
[    8] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-3003-WHT-SET | -1 | stock=109 | WH-HYD-04
[    9] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600003 | SKU-3003-WHT-SET x1 | ₹903.05 | Kolkata
[   10] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600004 | CREDIT_CARD | ₹1618.20 | PENDING
[   11] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600004 | SKU-1001-BLK-M x2 | ₹1618.20 | Kolkata
[   12] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-1001-BLK-M | -2 | stock=117 | WH-DEL-01
[   13] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2004-BLU-STD | -1 | stock=24 | WH-MUM-02
[   14] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600005 | UPI | ₹1899.00 | PENDING
[   15] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600005 | SKU-2004-BLU-STD x1 | ₹1899.00 | Mumbai
[   16] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-1002-BLU-32 | -2 | stock=78 | WH-DEL-01
[   17] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600006 | WALLET | ₹3998.00 | PENDING
[   18] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600006 | SKU-1002-BLU-32 x2 | ₹3998.00 | Kolkata
[   19] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_FAILED         | ORD-2026-600001 | NET_BANKING | ₹2239.20 | FAILED (Bank declined)
[   20] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600002 | UPI | ₹858.10 | AUTHORISED
[   21] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600003 | CREDIT_CARD | ₹903.05 | AUTHORISED
[   22] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600007 | SKU-2001-WHT-STD x3 | ₹8397.60 | Ahmedabad
[   23] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600007 | DEBIT_CARD | ₹8397.60 | PENDING
[   24] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600008 | UPI | ₹1519.20 | PENDING
[   25] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2001-WHT-STD | -3 | stock=42 | WH-HYD-04
[   26] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2004-BLU-STD | -1 | stock=23 | WH-BLR-03
[   27] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600008 | SKU-2004-BLU-STD x1 | ₹1519.20 | Hyderabad
[   28] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_FAILED         | ORD-2026-600004 | CREDIT_CARD | ₹1618.20 | FAILED (Gateway timeout)
[   29] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600005 | UPI | ₹1899.00 | AUTHORISED
[   30] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600006 | WALLET | ₹3998.00 | AUTHORISED
[   31] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600009 | SKU-2001-WHT-STD x1 | ₹3149.10 | Jaipur
[   32] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600009 | DEBIT_CARD | ₹3149.10 | PENDING
[   33] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600010 | UPI | ₹948.00 | PENDING
[   34] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2001-WHT-STD | -1 | stock=41 | WH-MUM-02
[   35] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-3003-WHT-SET | -1 | stock=108 | WH-HYD-04
[   36] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600001 | NET_BANKING | ₹2239.20 | AUTHORISED
[   37] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600010 | SKU-3003-WHT-SET x1 | ₹948.00 | Pune
[   38] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600008 | UPI | ₹1519.20 | AUTHORISED
[   39] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600011 | CREDIT_CARD | ₹2549.15 | PENDING
[   40] 👤 CRM Service Producer     → crm-events-topic   | CUSTOMER_CREATED       | CUST-20200 | new signup via WEB | Chennai
[   41] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-5002-BLK-10 | -1 | stock=19 | WH-MUM-02
[   42] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600012 | CREDIT_CARD | ₹2158.20 | PENDING
[   43] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600011 | SKU-5002-BLK-10 x1 | ₹2549.15 | Mumbai
[   44] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-5001-PUR-6MM | -2 | stock=53 | WH-HYD-04
[   45] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600002 | PROCESSING | WH-DEL-01
[   46] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600006 | PROCESSING | WH-DEL-01
[   47] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600007 | DEBIT_CARD | ₹8397.60 | AUTHORISED
[   48] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600004 | CREDIT_CARD | ₹1618.20 | AUTHORISED
[   49] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600009 | DEBIT_CARD | ₹3149.10 | AUTHORISED
[   50] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600010 | UPI | ₹948.00 | AUTHORISED
[   51] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600012 | SKU-5001-PUR-6MM x2 | ₹2158.20 | Kolkata
[   52] 👤 CRM Service Producer     → crm-events-topic   | ADDRESS_UPDATED        | CUST-10014 | address → Chennai (OFFICE)
[   53] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600013 | SKU-2003-BLK-STD x2 | ₹4998.00 | Delhi NCR
[   54] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600013 | WALLET | ₹4998.00 | PENDING
[   55] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2003-BLK-STD | -2 | stock=28 | WH-BLR-03
[   56] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-4002-STD-500 | -1 | stock=139 | WH-MUM-02
[   57] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600003 | PROCESSING | WH-HYD-04
[   58] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600014 | CREDIT_CARD | ₹848.00 | PENDING
[   59] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600014 | SKU-4002-STD-500 x1 | ₹848.00 | Kolkata
[   60] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600005 | PROCESSING | WH-MUM-02
[   61] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600012 | CREDIT_CARD | ₹2158.20 | AUTHORISED
[   62] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-1002-BLU-32 | -2 | stock=76 | WH-DEL-01
[   63] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600015 | UPI | ₹3998.00 | PENDING
[   64] 👤 CRM Service Producer     → crm-events-topic   | CUSTOMER_UPDATED       | CUST-10001 | segment Repeat → Repeat
[   65] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600015 | SKU-1002-BLU-32 x2 | ₹3998.00 | Chennai
[   66] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2003-BLK-STD | -2 | stock=26 | WH-BLR-03
[   67] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600016 | COD | ₹3998.40 | PENDING
[   68] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600016 | SKU-2003-BLK-STD x2 | ₹3998.40 | Lucknow
[   69] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600008 | PROCESSING | WH-BLR-03
[   70] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600011 | CREDIT_CARD | ₹2549.15 | AUTHORISED
[   71] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PACKED           | ORD-2026-600002 | PACKED | WH-DEL-01
[   72] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PACKED           | ORD-2026-600006 | PACKED | WH-DEL-01
[   73] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600017 | SKU-2002-BLK-STD x1 | ₹1299.00 | Delhi NCR
[   74] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-2002-BLK-STD | -1 | stock=89 | WH-MUM-02
[   75] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600017 | UPI | ₹1299.00 | PENDING
[   76] 🛒 CRM Service Producer     → orders-topic       | ORDER_PLACED           | ORD-2026-600018 | SKU-3001-SLV-1L x1 | ₹698.00 | Ahmedabad
[   77] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_INITIATED      | ORD-2026-600018 | CREDIT_CARD | ₹698.00 | PENDING
[   78] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600013 | WALLET | ₹4998.00 | AUTHORISED
[   79] 🏬 WMS / Logistics Producer → inventory-topic    | STOCK_DEDUCTED         | SKU-3001-SLV-1L | -1 | stock=199 | WH-MUM-02
[   80] 💳 Payment Gateway Producer → payments-topic     | PAYMENT_AUTHORISED     | ORD-2026-600014 | CREDIT_CARD | ₹848.00 | AUTHORISED
[   81] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600001 | PROCESSING | WH-BLR-03
[   82] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600007 | PROCESSING | WH-HYD-04
[   83] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600009 | PROCESSING | WH-MUM-02
[   84] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600010 | PROCESSING | WH-HYD-04
[   85] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PACKED           | ORD-2026-600005 | PACKED | WH-MUM-02
[   86] 📦 WMS / Logistics Producer → fulfillment-topic  | ORDER_PROCESSING       | ORD-2026-600012 | PROCESSING | WH-HYD-04
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

📊 Produced 86 events in 4.0s (21.5 events/sec)

   By producer:
     CRM Service Producer          21
     Payment Gateway Producer      34
     WMS / Logistics Producer      31

   By topic:
     🛒 orders-topic           18
     💳 payments-topic         34
     📦 fulfillment-topic      13
     🏬 inventory-topic        18
     ↩️  returns-topic           0
     👤 crm-events-topic        3

✅ All three producers flushed and closed.
```

---

## Challenges & Learnings

**Hardest part.** Getting the producer to emit a genuinely *live* stream rather than a
CSV replay. A CSV loop finishes and stops; a real OMS has hundreds of orders at
different stages simultaneously. I restructured the generator as a scheduler — each
order stores its next stage and a due time, and every tick drains whatever has come
due — before the stream behaved like an actual platform.

**Three producers, not one.** My first version used a single producer for all topics.
That does not reflect the architecture: in production the payment gateway and the
warehouse system are separate systems publishing independently. Splitting them into
three threads with three `KafkaProducer` instances exposed a real concurrency bug — all
three were writing to the same export file and interleaving mid-line, corrupting the
JSON. A shared lock around the write fixed it.

**Consumer groups.** I assumed all three consumers could share one `group_id` because
the diagram draws them in one box. They cannot — members of a group *share* partitions,
so with different subscriptions the assignment becomes unpredictable and each consumer
sees only part of its stream. Each functional consumer needs its own group. That
distinction between a *logical* processing layer and a *Kafka* consumer group was the
biggest conceptual correction of this assignment.

**Ordering.** Sending without a key meant Kafka round-robined messages across
partitions, so a `PAYMENT_AUTHORISED` could be consumed before its `ORDER_PLACED` and
the order-state document ended up wrong. Keying on `order_id` fixed it and made the
partitioning theory from Lecture 3 concrete.

**Broker connectivity and silent loss.** `NoBrokersAvailable` on the first attempt,
because the container was `sda-kafka-1` not `kafka`, and because
`KAFKA_CFG_ADVERTISED_LISTENERS` must be `localhost:9092` for a Python client outside
Docker. Separately, messages appeared to send but were missing from the console
consumer because I exited without `producer.flush()` — sends are asynchronous and
buffered, so the flush in the `finally` block is not optional.

**What I learnt.** Kafka's value here is decoupling. The three producers, the six
topics and the three consumers all run at their own pace; I can kill the returns
consumer, restart it, and it resumes from its committed offset with nothing lost, while
orders and payments keep flowing untouched. Splitting one data source into six topics
also meant I could write and test the CRM logic without touching the order flow at all.
