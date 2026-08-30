"""
consumer.py
-----------
Box 4 of the architecture: THREE consumers, each subscribing only to its own
topics, each validating / enriching / aggregating / flagging anomalies, and each
writing to the storage layer (box 5).

    Order & Payment Consumer        orders-topic, payments-topic
                                    -> MySQL: orders, payments
                                    -> MongoDB: oms_event_log, oms_alerts

    Fulfillment & Inventory Consumer  fulfillment-topic, inventory-topic
                                    -> MySQL: inventory
                                    -> MongoDB: oms_tracking, oms_event_log, oms_alerts

    Returns & Analytics Consumer    returns-topic, crm-events-topic
                                    -> MySQL: customers
                                    -> MongoDB: oms_returns, oms_customers,
                                                oms_event_log, oms_alerts

MongoDB is the default sink and is all you need for the assignment. MySQL is
optional — add --mysql to also write the structured tables from the diagram.

Each consumer uses its OWN group_id. In Kafka, members of one group share the
partitions of the topics they subscribe to; because these three consumers have
different subscriptions and different jobs, each needs its own group so that it
receives the complete stream for its topics. The "Consumer Group" box in the
diagram is the logical stream-processing layer, not a single Kafka group id.

Usage
-----
    python consumer.py                       # runs all three in one process
    python consumer.py --role order_payment  # run just one (separate terminal)
    python consumer.py --from-beginning
    python consumer.py --mysql               # also write MySQL tables
"""

import argparse
import json
import os
import sys
import threading
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BROKER = "localhost:9092"

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/"
)
DB_NAME = "sda_ecommerce"

# Optional MySQL (only used with --mysql)
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DB", "sda_ecommerce"),
}

# Consumer role -> (topics, group_id, label, icon)
ROLES = {
    "order_payment": (
        ["orders-topic", "payments-topic"],
        "oms-order-payment-group",
        "Order & Payment Consumer      ", "🛒"),
    "fulfillment_inventory": (
        ["fulfillment-topic", "inventory-topic"],
        "oms-fulfillment-inventory-group",
        "Fulfillment & Inventory Consumer", "📦"),
    "returns_analytics": (
        ["returns-topic", "crm-events-topic"],
        "oms-returns-analytics-group",
        "Returns & Analytics Consumer  ", "↩️ "),
}

LOW_STOCK_THRESHOLD = 10
FAILED_PAYMENT_THRESHOLD = 3
HIGH_VALUE_THRESHOLD = 5000.0

print_lock = threading.Lock()


def utcnow():
    return datetime.now(timezone.utc)


def say(*args):
    with print_lock:
        print(*args)


# --------------------------------------------------------------------------
# Storage layer
# --------------------------------------------------------------------------
def connect_mongo():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        print(f"✅ MongoDB connected → {DB_NAME}")
        return client[DB_NAME]
    except Exception as exc:
        print("❌ MongoDB connection failed:", exc)
        sys.exit(1)


class MySQLSink:
    """Optional structured sink. Creates its tables on first use."""

    DDL = [
        """CREATE TABLE IF NOT EXISTS orders (
             order_id VARCHAR(32) PRIMARY KEY, customer_id VARCHAR(32),
             sku VARCHAR(32), category VARCHAR(40), quantity INT,
             total_order_value DECIMAL(12,2), payment_method VARCHAR(20),
             delivery_method VARCHAR(20), warehouse_id VARCHAR(20),
             order_status VARCHAR(24), placed_at VARCHAR(32),
             last_updated DATETIME)""",
        """CREATE TABLE IF NOT EXISTS payments (
             transaction_id VARCHAR(40) PRIMARY KEY, order_id VARCHAR(32),
             payment_method VARCHAR(20), payment_status VARCHAR(20),
             amount DECIMAL(12,2), failure_reason VARCHAR(64),
             attempts INT, updated_at DATETIME)""",
        """CREATE TABLE IF NOT EXISTS inventory (
             sku VARCHAR(32), warehouse_id VARCHAR(20), category VARCHAR(40),
             stock_on_hand INT, last_movement VARCHAR(32), updated_at DATETIME,
             PRIMARY KEY (sku, warehouse_id))""",
        """CREATE TABLE IF NOT EXISTS customers (
             customer_id VARCHAR(32) PRIMARY KEY, customer_name VARCHAR(80),
             customer_location VARCHAR(40), pincode VARCHAR(10),
             customer_segment VARCHAR(20), updated_at DATETIME)""",
    ]

    def __init__(self):
        import mysql.connector
        self.conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = self.conn.cursor()
        for ddl in self.DDL:
            cur.execute(ddl)
        self.conn.commit()
        cur.close()
        self.lock = threading.Lock()
        print(f"✅ MySQL connected → {MYSQL_CONFIG['database']}")

    def run(self, sql, params):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self.conn.commit()
            cur.close()


# --------------------------------------------------------------------------
# One consumer per role
# --------------------------------------------------------------------------
class RoleConsumer(threading.Thread):
    def __init__(self, role, db, mysql, from_beginning):
        super().__init__(daemon=True, name=role)
        self.role = role
        self.topics, self.group_id, self.label, self.icon = ROLES[role]
        self.db = db
        self.mysql = mysql
        self.events = db["oms_event_log"]
        self.alerts = db["oms_alerts"]
        self.orders = db["oms_orders"]
        self.tracking = db["oms_tracking"]
        self.returns = db["oms_returns"]
        self.customers = db["oms_customers"]
        self.failed_payments = defaultdict(int)
        self.counts = defaultdict(int)
        self.alert_count = 0
        self.saved = 0
        self.running = True
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest" if from_beginning else "latest",
            enable_auto_commit=True,
            group_id=self.group_id,
            consumer_timeout_ms=1000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        print(f"✅ {self.label} subscribed → {', '.join(self.topics)} "
              f"[group: {self.group_id}]")

    # ---- shared helpers -------------------------------------------------
    def alert(self, alert_type, severity, event, detail):
        self.alerts.insert_one({
            "alert_type": alert_type, "severity": severity,
            "raised_by": self.role,
            "order_id": event.get("order_id"), "sku": event.get("sku"),
            "detail": detail, "source_event": event,
            "alert_at": utcnow(), "status": "open",
        })
        self.alert_count += 1
        say(f"   {'🚨' if severity == 'HIGH' else '⚠️ '} ALERT "
            f"[{alert_type}] {detail}")

    def log_raw(self, topic, event):
        self.events.insert_one({**event, "_topic": topic,
                                "_consumer": self.role, "_saved_at": utcnow()})

    # ---- role 1: orders + payments --------------------------------------
    def handle_order_payment(self, event):
        et, oid = event["event_type"], event.get("order_id")
        update = {"last_event": et, "last_updated": utcnow()}

        if et == "ORDER_PLACED":
            update.update({
                "order_status": "PLACED", "customer_id": event["customer_id"],
                "customer_location": event["customer_location"],
                "customer_segment": event["customer_segment"],
                "sku": event["sku"], "product_name": event["product_name"],
                "category": event["category"], "quantity": event["quantity"],
                "total_order_value": event["total_order_value"],
                "payment_method": event["payment_method"],
                "delivery_method": event["delivery_method"],
                "warehouse_id": event["warehouse_id"],
                "placed_at": event["event_timestamp"],
            })
            if event["total_order_value"] > HIGH_VALUE_THRESHOLD:
                self.alert("HIGH_VALUE_ORDER", "LOW", event,
                           f"{oid} value ₹{event['total_order_value']:.2f} "
                           f"exceeds manual-review threshold")
            if self.mysql:
                self.mysql.run(
                    """INSERT INTO orders (order_id, customer_id, sku, category,
                         quantity, total_order_value, payment_method,
                         delivery_method, warehouse_id, order_status, placed_at,
                         last_updated)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                       ON DUPLICATE KEY UPDATE order_status=VALUES(order_status),
                         last_updated=NOW()""",
                    (oid, event["customer_id"], event["sku"], event["category"],
                     event["quantity"], event["total_order_value"],
                     event["payment_method"], event["delivery_method"],
                     event["warehouse_id"], "PLACED", event["event_timestamp"]))

        elif et == "PAYMENT_FAILED":
            self.failed_payments[oid] += 1
            update.update({"payment_status": "FAILED",
                           "payment_attempts": self.failed_payments[oid]})
            if self.failed_payments[oid] >= FAILED_PAYMENT_THRESHOLD:
                self.alert("PAYMENT_FRAUD_RISK", "HIGH", event,
                           f"{oid} has {self.failed_payments[oid]} failed attempts "
                           f"({event.get('failure_reason')}) — hold before fulfillment")

        elif et == "PAYMENT_AUTHORISED":
            update.update({"payment_status": "AUTHORISED",
                           "transaction_id": event["transaction_id"],
                           "paid_at": event["event_timestamp"]})

        if et.startswith("PAYMENT") and self.mysql:
            self.mysql.run(
                """INSERT INTO payments (transaction_id, order_id, payment_method,
                     payment_status, amount, failure_reason, attempts, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                   ON DUPLICATE KEY UPDATE payment_status=VALUES(payment_status),
                     failure_reason=VALUES(failure_reason),
                     attempts=VALUES(attempts), updated_at=NOW()""",
                (event.get("transaction_id"), oid, event.get("payment_method"),
                 event.get("payment_status"), event.get("amount"),
                 event.get("failure_reason"), self.failed_payments[oid]))

        if oid:
            self.orders.update_one({"_id": oid}, {"$set": update}, upsert=True)

    # ---- role 2: fulfillment + inventory --------------------------------
    def handle_fulfillment_inventory(self, event):
        et, oid = event["event_type"], event.get("order_id")

        if et.startswith("STOCK"):
            qty = event["stock_after_movement"]
            if qty == 0:
                self.alert("OUT_OF_STOCK", "HIGH", event,
                           f"{event['sku']} hit zero at {event['warehouse_id']} "
                           f"— pull listing from storefront")
            elif qty <= LOW_STOCK_THRESHOLD and event["quantity_change"] < 0:
                self.alert("LOW_STOCK", "MEDIUM", event,
                           f"{event['sku']} down to {qty} units — trigger reorder")
            if self.mysql:
                self.mysql.run(
                    """INSERT INTO inventory (sku, warehouse_id, category,
                         stock_on_hand, last_movement, updated_at)
                       VALUES (%s,%s,%s,%s,%s,NOW())
                       ON DUPLICATE KEY UPDATE stock_on_hand=VALUES(stock_on_hand),
                         last_movement=VALUES(last_movement), updated_at=NOW()""",
                    (event["sku"], event["warehouse_id"], event["category"],
                     qty, et))
            return

        # fulfillment events
        update = {"order_status": event["fulfillment_status"],
                  "last_event": et, "last_updated": utcnow()}
        if et == "ORDER_SHIPPED":
            update.update({"shipped_at": event["event_timestamp"],
                           "tracking_id": event["tracking_id"],
                           "courier": event["courier"]})
            self.tracking.insert_one({
                "order_id": oid, "tracking_id": event["tracking_id"],
                "courier": event["courier"], "status": "SHIPPED",
                "warehouse_id": event["warehouse_id"],
                "destination": event["customer_location"],
                "shipped_at": event["event_timestamp"], "_saved_at": utcnow()})
        if et == "ORDER_DELIVERED":
            update["delivered_at"] = event["event_timestamp"]
            self.tracking.update_one(
                {"order_id": oid},
                {"$set": {"status": "DELIVERED",
                          "delivered_at": event["event_timestamp"]}})
        if oid:
            self.orders.update_one({"_id": oid}, {"$set": update}, upsert=True)
        if self.mysql and oid:
            self.mysql.run(
                """UPDATE orders SET order_status=%s, last_updated=NOW()
                   WHERE order_id=%s""", (event["fulfillment_status"], oid))

    # ---- role 3: returns + CRM ------------------------------------------
    def handle_returns_analytics(self, event):
        et, oid = event["event_type"], event.get("order_id")

        if et in ("ORDER_CANCELLED", "RETURN_RAISED"):
            self.returns.insert_one({**event, "_saved_at": utcnow()})
            reason = event.get("cancellation_reason") or event.get("return_reason")
            self.orders.update_one(
                {"_id": oid},
                {"$set": {"order_status": "CANCELLED" if et == "ORDER_CANCELLED"
                          else "RETURN_RAISED",
                          "reason": reason, "last_event": et,
                          "last_updated": utcnow()}}, upsert=True)
            # rolling aggregate: reasons per SKU
            self.db["oms_return_reasons"].update_one(
                {"_id": f"{event['sku']}|{reason}"},
                {"$inc": {"count": 1},
                 "$set": {"sku": event["sku"], "reason": reason,
                          "last_seen": utcnow()}}, upsert=True)
            agg = self.db["oms_return_reasons"].find_one(
                {"_id": f"{event['sku']}|{reason}"})
            if agg and agg.get("count", 0) >= 3:
                self.alert("DEFECT_CLUSTER", "HIGH", event,
                           f"{event['sku']} returned {agg['count']}x for "
                           f"'{reason}' — halt sales pending QC check")
            return

        # CRM events
        cid = event["customer_id"]
        doc = {"customer_id": cid, "last_event": et, "last_updated": utcnow()}
        for f in ("customer_name", "customer_location", "pincode",
                  "customer_segment", "email", "phone", "shipping_address",
                  "signup_channel"):
            if f in event:
                doc[f] = event[f]
        self.customers.update_one({"_id": cid}, {"$set": doc}, upsert=True)
        if self.mysql:
            self.mysql.run(
                """INSERT INTO customers (customer_id, customer_name,
                     customer_location, pincode, customer_segment, updated_at)
                   VALUES (%s,%s,%s,%s,%s,NOW())
                   ON DUPLICATE KEY UPDATE
                     customer_location=VALUES(customer_location),
                     pincode=VALUES(pincode),
                     customer_segment=VALUES(customer_segment),
                     updated_at=NOW()""",
                (cid, event.get("customer_name"), event.get("customer_location"),
                 event.get("pincode"), event.get("customer_segment")))

    # ---- loop -----------------------------------------------------------
    def run(self):
        handler = {
            "order_payment": self.handle_order_payment,
            "fulfillment_inventory": self.handle_fulfillment_inventory,
            "returns_analytics": self.handle_returns_analytics,
        }[self.role]

        while self.running:
            for msg in self.consumer:          # consumer_timeout_ms makes this yield
                event = msg.value
                self.log_raw(msg.topic, event)
                handler(event)
                self.counts[msg.topic] += 1
                self.saved += 1
                say(f"💾 {self.icon} {self.label} | {msg.topic:<18} "
                    f"p{msg.partition} off={msg.offset:<6} | "
                    f"{event['event_type']:<22} | key={msg.key}")
                if not self.running:
                    break

    def shutdown(self):
        self.running = False
        self.join(timeout=5)
        self.consumer.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=list(ROLES) + ["all"], default="all",
                    help="run one consumer, or all three in this process")
    ap.add_argument("--from-beginning", action="store_true",
                    help="replay the topics from offset 0")
    ap.add_argument("--mysql", action="store_true",
                    help="also write the structured MySQL tables")
    args = ap.parse_args()

    db = connect_mongo()
    mysql = MySQLSink() if args.mysql else None
    roles = list(ROLES) if args.role == "all" else [args.role]

    consumers = [RoleConsumer(r, db, mysql, args.from_beginning) for r in roles]
    for c in consumers:
        c.start()

    print("\n📡 Listening for OMS events — Ctrl+C to stop\n" + "─" * 112)
    try:
        while True:
            for c in consumers:
                c.join(timeout=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for c in consumers:
            c.shutdown()
        print("─" * 112)
        total = sum(c.saved for c in consumers)
        alerts = sum(c.alert_count for c in consumers)
        print(f"\n📊 Consumed {total} events, raised {alerts} alerts\n")
        for c in consumers:
            print(f"   {c.icon} {c.label} {c.saved:>6} events, "
                  f"{c.alert_count} alerts")
        print("\n   MongoDB collections:")
        for name in ("oms_event_log", "oms_orders", "oms_tracking",
                     "oms_returns", "oms_customers", "oms_return_reasons",
                     "oms_alerts"):
            print(f"     {name:<20} {db[name].count_documents({}):>6} documents")
        print("\n✅ Consumers closed.")


if __name__ == "__main__":
    main()
