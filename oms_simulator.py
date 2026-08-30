"""
oms_simulator.py
----------------
A fake e-commerce platform that generates LIVE Order Management System events.

This is the "1. ORDER MANAGEMENT SYSTEM" box of the architecture diagram. It
emits the five source event families shown there:

    Order Events                 (order / update / status)
    Payment Events               (authorised / failed / refund)
    Customer Events              (created / updated / address)
    Inventory Events             (stock allocated / deducted)
    Cancellation & Return Events

Instead of replaying a static CSV, it runs an order-lifecycle state machine:
orders are created continuously and each one is then scheduled forward through
payment -> fulfillment -> delivery (or cancellation / return) over real elapsed
time, emitting one event at every transition.

Nothing here knows about Kafka. Each event is tagged with the SOURCE PRODUCER
that owns it, and producer.py routes it to the right producer thread and topic.
"""

import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker("en_IN")
IST = timezone(timedelta(hours=5, minutes=30))


# --------------------------------------------------------------------------
# Topics (box 3 of the diagram)  and  producers (box 2)
# --------------------------------------------------------------------------
TOPICS = [
    "orders-topic",
    "payments-topic",
    "fulfillment-topic",
    "inventory-topic",
    "returns-topic",
    "crm-events-topic",
]

# Which producer owns which topic
PRODUCER_TOPICS = {
    "crm-service":     ["orders-topic", "crm-events-topic"],
    "payment-gateway": ["payments-topic"],
    "wms-logistics":   ["fulfillment-topic", "inventory-topic", "returns-topic"],
}
TOPIC_OWNER = {t: p for p, ts in PRODUCER_TOPICS.items() for t in ts}


# --------------------------------------------------------------------------
# Product catalog — the master data our events refer to
# --------------------------------------------------------------------------
CATALOG = [
    # product_id, name,                       category,      sku,                 price, opening_stock
    ("PRD-1001", "Cotton Crew Neck T-Shirt",  "Apparel",     "SKU-1001-BLK-M",    899,   120),
    ("PRD-1002", "Slim Fit Denim Jeans",      "Apparel",     "SKU-1002-BLU-32",  1999,    80),
    ("PRD-1003", "Running Shoes",             "Footwear",    "SKU-1003-GRY-9",   2799,    60),
    ("PRD-1004", "Leather Formal Belt",       "Accessories", "SKU-1004-BRN-FS",   749,   150),
    ("PRD-2001", "Wireless Earbuds",          "Electronics", "SKU-2001-WHT-STD", 3499,    45),
    ("PRD-2002", "65W Fast Charger",          "Electronics", "SKU-2002-BLK-STD", 1299,    90),
    ("PRD-2003", "Smart Fitness Band",        "Electronics", "SKU-2003-BLK-STD", 2499,    30),
    ("PRD-2004", "Bluetooth Speaker",         "Electronics", "SKU-2004-BLU-STD", 1899,    25),
    ("PRD-3001", "Stainless Steel Bottle 1L", "Home",        "SKU-3001-SLV-1L",   649,   200),
    ("PRD-3002", "Cotton Bedsheet Set",       "Home",        "SKU-3002-GRN-DBL", 1499,    70),
    ("PRD-3003", "Ceramic Coffee Mug Set",    "Home",        "SKU-3003-WHT-SET",  899,   110),
    ("PRD-4001", "Organic Green Tea 250g",    "Grocery",     "SKU-4001-STD-250",  499,   180),
    ("PRD-4002", "Roasted Almonds 500g",      "Grocery",     "SKU-4002-STD-500",  799,   140),
    ("PRD-5001", "Yoga Mat 6mm",              "Sports",      "SKU-5001-PUR-6MM", 1199,    55),
    ("PRD-5002", "Adjustable Dumbbell 10kg",  "Sports",      "SKU-5002-BLK-10",  2999,    20),
]

CITIES = [
    ("Delhi NCR", "110001"), ("Mumbai", "400001"), ("Bengaluru", "560001"),
    ("Hyderabad", "500001"), ("Chennai", "600001"), ("Pune", "411001"),
    ("Kolkata", "700001"), ("Ahmedabad", "380001"), ("Jaipur", "302001"),
    ("Lucknow", "226001"),
]

WAREHOUSES = ["WH-DEL-01", "WH-MUM-02", "WH-BLR-03", "WH-HYD-04"]
PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "COD", "WALLET"]
DELIVERY_METHODS = ["Standard", "Express", "Same-Day"]
CUSTOMER_SEGMENTS = ["New", "Repeat", "Prime", "Corporate"]

CANCEL_REASONS = [
    "Changed mind", "Found cheaper elsewhere", "Ordered by mistake",
    "Delivery date too late", "Payment issue",
]
RETURN_REASONS = [
    "Size did not fit", "Product damaged in transit", "Wrong item delivered",
    "Quality not as described", "Missing accessories", "Defective unit",
]
PAYMENT_FAILURES = [
    "Insufficient funds", "Bank declined", "Gateway timeout",
    "3DS authentication failed", "Card expired",
]


def _now():
    return datetime.now(IST).isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Customer pool — reused so repeat customers and fraud patterns are real
# --------------------------------------------------------------------------
class CustomerPool:
    def __init__(self, size=200):
        self.customers = []
        for i in range(size):
            city, pin = random.choice(CITIES)
            self.customers.append({
                "customer_id": f"CUST-{10000 + i}",
                "customer_name": fake.name(),
                "customer_location": city,
                "pincode": pin,
                "customer_segment": random.choices(
                    CUSTOMER_SEGMENTS, weights=[35, 40, 20, 5])[0],
                "shipping_address": f"{fake.building_number()}, {fake.street_name()}, {city} - {pin}",
                "email": fake.email(),
                "phone": f"+91-{random.randint(6000000000, 9999999999)}",
            })

    def pick(self):
        return random.choice(self.customers)

    def new_customer(self):
        """A brand-new signup — produces a CUSTOMER_CREATED CRM event."""
        city, pin = random.choice(CITIES)
        cust = {
            "customer_id": f"CUST-{20000 + len(self.customers)}",
            "customer_name": fake.name(),
            "customer_location": city,
            "pincode": pin,
            "customer_segment": "New",
            "shipping_address": f"{fake.building_number()}, {fake.street_name()}, {city} - {pin}",
            "email": fake.email(),
            "phone": f"+91-{random.randint(6000000000, 9999999999)}",
        }
        self.customers.append(cust)
        return cust


# --------------------------------------------------------------------------
# The simulator
# --------------------------------------------------------------------------
class OMSSimulator:
    """Emits (producer, topic, key, event) tuples."""

    TOPICS = TOPICS

    def __init__(self, seed=None, fraud_rate=0.04, fail_rate=0.12, speed=1.0):
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        self.customers = CustomerPool()
        self.stock = {p[3]: p[5] for p in CATALOG}      # sku -> live quantity
        self.pending = []                               # orders awaiting next transition
        self.speed = max(speed, 0.01)
        self.fraud_rate = fraud_rate
        self.fail_rate = fail_rate
        self.counter = 0
        self.stats = defaultdict(int)

    # -- helpers ----------------------------------------------------------
    def _new_order_id(self):
        self.counter += 1
        return f"ORD-2026-{600000 + self.counter}"

    def _base(self, event_type, order=None, customer_id=None):
        ev = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": _now(),
        }
        if order:
            ev["order_id"] = order["order_id"]
            ev["customer_id"] = order["customer_id"]
        elif customer_id:
            ev["customer_id"] = customer_id
        return ev

    def _schedule(self, order, stage, seconds):
        order["_next_stage"] = stage
        order["_due_at"] = datetime.now(IST) + timedelta(seconds=seconds / self.speed)
        self.pending.append(order)

    @staticmethod
    def _pack(topic, key, event):
        """Attach the owning producer so producer.py can route it."""
        event["_source_producer"] = TOPIC_OWNER[topic]
        return (TOPIC_OWNER[topic], topic, key, event)

    # -- CRM / customer events (crm-events-topic) --------------------------
    def _emit_customer_created(self):
        cust = self.customers.new_customer()
        ev = self._base("CUSTOMER_CREATED", customer_id=cust["customer_id"])
        ev.update({
            "customer_name": cust["customer_name"],
            "customer_location": cust["customer_location"],
            "pincode": cust["pincode"],
            "customer_segment": cust["customer_segment"],
            "email": cust["email"],
            "phone": cust["phone"],
            "signup_channel": random.choice(["APP", "WEB", "REFERRAL"]),
        })
        return [self._pack("crm-events-topic", cust["customer_id"], ev)]

    def _emit_customer_updated(self):
        cust = self.customers.pick()
        kind = random.choices(
            ["ADDRESS_UPDATED", "CUSTOMER_UPDATED"], weights=[60, 40])[0]
        ev = self._base(kind, customer_id=cust["customer_id"])
        if kind == "ADDRESS_UPDATED":
            city, pin = random.choice(CITIES)
            cust["customer_location"], cust["pincode"] = city, pin
            cust["shipping_address"] = f"{fake.building_number()}, {fake.street_name()}, {city} - {pin}"
            ev.update({
                "customer_location": city,
                "pincode": pin,
                "shipping_address": cust["shipping_address"],
                "address_type": random.choice(["HOME", "OFFICE", "OTHER"]),
            })
        else:
            old = cust["customer_segment"]
            cust["customer_segment"] = random.choice(CUSTOMER_SEGMENTS)
            ev.update({
                "customer_location": cust["customer_location"],
                "previous_segment": old,
                "customer_segment": cust["customer_segment"],
                "updated_field": "customer_segment",
            })
        return [self._pack("crm-events-topic", cust["customer_id"], ev)]

    # -- order creation ---------------------------------------------------
    def _create_order(self):
        cust = self.customers.pick()
        pid, name, category, sku, price, _ = random.choice(CATALOG)

        qty = random.choices([1, 2, 3, 4], weights=[65, 22, 9, 4])[0]
        discount = round(price * qty * random.choice([0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
        subtotal = round(price * qty, 2)
        shipping = 0 if subtotal > 999 else 49
        total = round(subtotal - discount + shipping, 2)

        return {
            "order_id": self._new_order_id(),
            "customer_id": cust["customer_id"],
            "customer_location": cust["customer_location"],
            "customer_segment": cust["customer_segment"],
            "shipping_address": cust["shipping_address"],
            "product_id": pid, "product_name": name,
            "category": category, "sku": sku,
            "quantity": qty, "unit_price": float(price),
            "discount": discount, "subtotal": subtotal,
            "shipping_charges": float(shipping), "total_order_value": total,
            "payment_method": random.choice(PAYMENT_METHODS),
            "delivery_method": random.choices(DELIVERY_METHODS, weights=[65, 28, 7])[0],
            "warehouse_id": random.choice(WAREHOUSES),
            "order_type": random.choices(["ONLINE", "APP", "MARKETPLACE"],
                                         weights=[35, 55, 10])[0],
            "is_fraud_pattern": random.random() < self.fraud_rate,
        }

    def _emit_placed(self, order):
        """Placing an order fans out to three producers at once."""
        events = []

        # -- CRM Service Producer -> orders-topic
        placed = self._base("ORDER_PLACED", order)
        placed.update({
            "order_status": "PLACED", "order_type": order["order_type"],
            "customer_location": order["customer_location"],
            "customer_segment": order["customer_segment"],
            "product_id": order["product_id"], "product_name": order["product_name"],
            "category": order["category"], "sku": order["sku"],
            "quantity": order["quantity"], "unit_price": order["unit_price"],
            "discount": order["discount"], "subtotal": order["subtotal"],
            "shipping_charges": order["shipping_charges"],
            "total_order_value": order["total_order_value"],
            "payment_method": order["payment_method"],
            "delivery_method": order["delivery_method"],
            "shipping_address": order["shipping_address"],
            "warehouse_id": order["warehouse_id"],
            "stock_at_order": self.stock[order["sku"]],
        })
        events.append(self._pack("orders-topic", order["order_id"], placed))

        # -- WMS / Logistics Producer -> inventory-topic (stock allocated)
        self.stock[order["sku"]] = max(0, self.stock[order["sku"]] - order["quantity"])
        inv = self._base("STOCK_DEDUCTED", order)
        inv.update({
            "product_id": order["product_id"], "sku": order["sku"],
            "category": order["category"], "warehouse_id": order["warehouse_id"],
            "quantity_change": -order["quantity"],
            "stock_after_movement": self.stock[order["sku"]],
            "product_availability": "IN_STOCK" if self.stock[order["sku"]] > 0 else "OUT_OF_STOCK",
        })
        events.append(self._pack("inventory-topic", order["sku"], inv))

        # -- Payment Gateway Producer -> payments-topic
        pay = self._base("PAYMENT_INITIATED", order)
        pay.update({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "payment_method": order["payment_method"],
            "payment_status": "PENDING",
            "amount": order["total_order_value"],
        })
        order["transaction_id"] = pay["transaction_id"]
        events.append(self._pack("payments-topic", order["order_id"], pay))

        order["_fail_budget"] = random.randint(3, 5) if order["is_fraud_pattern"] else (
            1 if random.random() < self.fail_rate else 0)
        self._schedule(order, "PAYMENT_RESULT", random.uniform(2, 6))
        return events

    def _emit_payment_result(self, order):
        if order["_fail_budget"] > 0:
            order["_fail_budget"] -= 1
            ev = self._base("PAYMENT_FAILED", order)
            ev.update({
                "transaction_id": order["transaction_id"],
                "payment_method": order["payment_method"],
                "payment_status": "FAILED",
                "amount": order["total_order_value"],
                "failure_reason": random.choice(PAYMENT_FAILURES),
            })
            self._schedule(order, "PAYMENT_RESULT", random.uniform(3, 8))
            return [self._pack("payments-topic", order["order_id"], ev)]

        ev = self._base("PAYMENT_AUTHORISED", order)
        ev.update({
            "transaction_id": order["transaction_id"],
            "payment_method": order["payment_method"],
            "payment_status": "AUTHORISED",
            "amount": order["total_order_value"],
        })
        self._schedule(order, "PROCESSING", random.uniform(4, 10))
        return [self._pack("payments-topic", order["order_id"], ev)]

    def _emit_fulfillment(self, order, event_type, status, next_stage, delay):
        ev = self._base(event_type, order)
        ev.update({
            "fulfillment_status": status,
            "warehouse_id": order["warehouse_id"], "sku": order["sku"],
            "quantity": order["quantity"],
            "delivery_method": order["delivery_method"],
            "customer_location": order["customer_location"],
        })
        if event_type == "ORDER_SHIPPED":
            ev["tracking_id"] = f"TRK-{uuid.uuid4().hex[:10].upper()}"
            ev["courier"] = random.choice(["Delhivery", "Blue Dart", "Ekart", "XpressBees"])
            order["tracking_id"] = ev["tracking_id"]
        if next_stage:
            self._schedule(order, next_stage, delay)
        return [self._pack("fulfillment-topic", order["order_id"], ev)]

    def _emit_cancellation(self, order):
        ev = self._base("ORDER_CANCELLED", order)
        ev.update({
            "cancellation_status": "CANCELLED",
            "cancellation_reason": random.choice(CANCEL_REASONS),
            "refund_status": "INITIATED",
            "refund_amount": order["total_order_value"],
            "sku": order["sku"], "category": order["category"],
        })
        return [self._pack("returns-topic", order["order_id"], ev),
                self._restock(order, "STOCK_RESTOCKED_CANCEL")]

    def _emit_return(self, order):
        ev = self._base("RETURN_RAISED", order)
        ev.update({
            "return_status": "RETURN_RAISED",
            "return_reason": random.choice(RETURN_REASONS),
            "refund_status": "PENDING",
            "refund_amount": order["total_order_value"],
            "sku": order["sku"], "product_name": order["product_name"],
            "category": order["category"],
        })
        return [self._pack("returns-topic", order["order_id"], ev),
                self._restock(order, "STOCK_RESTOCKED_RETURN")]

    def _restock(self, order, event_type):
        self.stock[order["sku"]] += order["quantity"]
        inv = self._base(event_type, order)
        inv.update({
            "product_id": order["product_id"], "sku": order["sku"],
            "category": order["category"], "warehouse_id": order["warehouse_id"],
            "quantity_change": order["quantity"],
            "stock_after_movement": self.stock[order["sku"]],
            "product_availability": "IN_STOCK",
        })
        return self._pack("inventory-topic", order["sku"], inv)

    # -- the tick ---------------------------------------------------------
    def tick(self, new_orders=1):
        """Advance the platform one step -> list of (producer, topic, key, event)."""
        events = []

        # CRM activity: occasional signups and profile/address updates
        if random.random() < 0.12:
            events.extend(self._emit_customer_created())
        if random.random() < 0.18:
            events.extend(self._emit_customer_updated())

        # brand-new orders
        for _ in range(new_orders):
            events.extend(self._emit_placed(self._create_order()))

        # orders whose next lifecycle step has come due
        now = datetime.now(IST)
        still_pending, due = [], []
        for o in self.pending:
            (due if o["_due_at"] <= now else still_pending).append(o)
        self.pending = still_pending

        for order in due:
            stage = order["_next_stage"]
            if stage == "PAYMENT_RESULT":
                events.extend(self._emit_payment_result(order))
            elif stage == "PROCESSING":
                if random.random() < 0.06:
                    events.extend(self._emit_cancellation(order))
                else:
                    events.extend(self._emit_fulfillment(
                        order, "ORDER_PROCESSING", "PROCESSING", "PACKED",
                        random.uniform(4, 9)))
            elif stage == "PACKED":
                events.extend(self._emit_fulfillment(
                    order, "ORDER_PACKED", "PACKED", "SHIPPED", random.uniform(5, 12)))
            elif stage == "SHIPPED":
                events.extend(self._emit_fulfillment(
                    order, "ORDER_SHIPPED", "SHIPPED", "DELIVERED", random.uniform(8, 20)))
            elif stage == "DELIVERED":
                events.extend(self._emit_fulfillment(
                    order, "ORDER_DELIVERED", "DELIVERED", None, 0))
                if random.random() < 0.10:
                    self._schedule(order, "RETURN", random.uniform(6, 15))
            elif stage == "RETURN":
                events.extend(self._emit_return(order))

        for producer, topic, _, _ in events:
            self.stats[topic] += 1
            self.stats[producer] += 1
        self.stats["total"] += len(events)
        return events

    def low_stock_skus(self, threshold=10):
        return {s: q for s, q in self.stock.items() if q <= threshold}
