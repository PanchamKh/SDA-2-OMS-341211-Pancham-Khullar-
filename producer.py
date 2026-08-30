"""
producer.py
-----------
Box 2 of the architecture: THREE independent Kafka producers, each owning its
own topics, each running in its own thread with its own KafkaProducer instance.

    CRM Service Producer      -> orders-topic,       crm-events-topic
    Payment Gateway Producer  -> payments-topic
    WMS / Logistics Producer  -> fulfillment-topic,  inventory-topic,  returns-topic

The simulated OMS platform (oms_simulator.py) runs in the main thread and pushes
each event onto the queue of the producer that owns it, exactly as three separate
source systems would publish independently in production.

Messages are keyed on order_id (sku for inventory, customer_id for CRM) so that
all events for one entity land in the same partition and stay in sequence.

Usage
-----
    python producer.py                      # stream forever, 2 new orders/sec
    python producer.py --rate 5 --speed 5   # heavier load, faster lifecycle
    python producer.py --limit 200          # stop after 200 events
    python producer.py --dry-run            # print only, no Kafka needed
    python producer.py --dry-run --limit 1500 --export sample_data/oms_sample_events.jsonl
"""

import argparse
import json
import queue
import sys
import threading
import time
from datetime import datetime

from oms_simulator import OMSSimulator, PRODUCER_TOPICS, TOPICS

KAFKA_BROKER = "localhost:9092"

LABEL = {
    "crm-service":     "CRM Service Producer     ",
    "payment-gateway": "Payment Gateway Producer ",
    "wms-logistics":   "WMS / Logistics Producer ",
}
ICON = {
    "orders-topic":      "🛒",
    "payments-topic":    "💳",
    "fulfillment-topic": "📦",
    "inventory-topic":   "🏬",
    "returns-topic":     "↩️ ",
    "crm-events-topic":  "👤",
}


# --------------------------------------------------------------------------
# One producer thread per source system
# --------------------------------------------------------------------------
class SourceProducer(threading.Thread):
    """Owns one KafkaProducer and publishes only the topics assigned to it."""

    def __init__(self, name, topics, dry_run=False, export_fh=None, lock=None,
                 counter=None):
        super().__init__(daemon=True, name=name)
        self.pname = name
        self.topics = topics
        self.dry_run = dry_run
        self.export_fh = export_fh
        self.lock = lock
        self.counter = counter
        self.q = queue.Queue()
        self.sent = 0
        self.running = True
        self.producer = None if dry_run else self._connect()

    def _connect(self):
        from kafka import KafkaProducer, errors
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                retries=5,
                acks="all",          # wait for all in-sync replicas
                linger_ms=10,
                client_id=self.pname,
            )
            for t in self.topics:
                p.partitions_for(t)   # fails fast if the topic does not exist
            print(f"✅ {LABEL[self.pname]} connected → {', '.join(self.topics)}")
            return p
        except errors.NoBrokersAvailable:
            print(f"❌ Kafka not running at {KAFKA_BROKER} — run: docker-compose up -d")
            sys.exit(1)
        except Exception as exc:
            print(f"❌ {self.pname} failed to connect:", exc)
            sys.exit(1)

    def publish(self, topic, key, event):
        self.q.put((topic, key, event))

    def run(self):
        while self.running or not self.q.empty():
            try:
                topic, key, event = self.q.get(timeout=0.2)
            except queue.Empty:
                continue
            if self.producer:
                self.producer.send(topic, key=key, value=event)
            self.sent += 1
            with self.lock:
                if self.export_fh:
                    self.export_fh.write(json.dumps(event) + "\n")
                self.counter[0] += 1
                print(f"[{self.counter[0]:>5}] {ICON[topic]} {LABEL[self.pname]}→ "
                      f"{topic:<18} | {event['event_type']:<22} | {summarise(event)}")

    def shutdown(self):
        self.running = False
        self.join(timeout=5)
        if self.producer:
            self.producer.flush()
            self.producer.close()


# --------------------------------------------------------------------------
def summarise(event):
    """One readable line per event for the terminal."""
    et = event["event_type"]
    oid = event.get("order_id", event.get("customer_id", "-"))
    if et == "ORDER_PLACED":
        return (f"{oid} | {event['sku']} x{event['quantity']} "
                f"| ₹{event['total_order_value']:.2f} | {event['customer_location']}")
    if et.startswith("PAYMENT"):
        extra = f" ({event.get('failure_reason')})" if event.get("failure_reason") else ""
        return (f"{oid} | {event['payment_method']} | ₹{event['amount']:.2f} "
                f"| {event['payment_status']}{extra}")
    if et.startswith("STOCK"):
        return (f"{event['sku']} | {event['quantity_change']:+d} "
                f"| stock={event['stock_after_movement']} | {event['warehouse_id']}")
    if "fulfillment_status" in event:
        trk = f" | {event['courier']} {event['tracking_id']}" if event.get("tracking_id") else ""
        return f"{oid} | {event['fulfillment_status']} | {event['warehouse_id']}{trk}"
    if et == "ORDER_CANCELLED":
        return f"{oid} | CANCELLED | {event['cancellation_reason']} | refund ₹{event['refund_amount']:.2f}"
    if et == "RETURN_RAISED":
        return f"{oid} | RETURN | {event['return_reason']} | {event['sku']}"
    if et == "CUSTOMER_CREATED":
        return f"{oid} | new signup via {event['signup_channel']} | {event['customer_location']}"
    if et == "ADDRESS_UPDATED":
        return f"{oid} | address → {event['customer_location']} ({event['address_type']})"
    if et == "CUSTOMER_UPDATED":
        return f"{oid} | segment {event['previous_segment']} → {event['customer_segment']}"
    return oid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=2,
                    help="new orders created per tick (default 2)")
    ap.add_argument("--interval", type=float, default=1.0,
                    help="seconds between ticks (default 1.0)")
    ap.add_argument("--speed", type=float, default=1.0,
                    help="lifecycle multiplier; 5 = orders progress 5x faster")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after N events (0 = run forever)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print events without sending to Kafka")
    ap.add_argument("--export", type=str, default=None,
                    help="also write every event to this JSON Lines file")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    sim = OMSSimulator(seed=args.seed, speed=args.speed)
    export_fh = open(args.export, "w", encoding="utf-8") if args.export else None
    lock = threading.Lock()
    counter = [0]

    if args.dry_run:
        print("🧪 DRY RUN — events are printed, nothing is sent to Kafka\n")
    else:
        print(f"Connecting three producers to Kafka at {KAFKA_BROKER} ...")

    producers = {
        name: SourceProducer(name, topics, args.dry_run, export_fh, lock, counter)
        for name, topics in PRODUCER_TOPICS.items()
    }
    for p in producers.values():
        p.start()

    print("\n🏬 LIVE e-commerce OMS event stream — Ctrl+C to stop")
    print(f"   {args.rate} new order(s) every {args.interval}s | 3 producers | "
          f"6 topics | 15 SKUs | 4 warehouses\n")
    print("─" * 122)

    started = datetime.now()
    try:
        while True:
            for producer_name, topic, key, event in sim.tick(new_orders=args.rate):
                producers[producer_name].publish(topic, key, event)
            if args.limit and counter[0] >= args.limit:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        for p in producers.values():
            p.shutdown()
        elapsed = (datetime.now() - started).total_seconds()
        print("─" * 122)
        print(f"\n📊 Produced {counter[0]} events in {elapsed:.1f}s "
              f"({counter[0] / max(elapsed, 1):.1f} events/sec)\n")
        print("   By producer:")
        for name, p in producers.items():
            print(f"     {LABEL[name]} {p.sent:>6}")
        print("\n   By topic:")
        for t in TOPICS:
            print(f"     {ICON[t]} {t:<18} {sim.stats[t]:>6}")
        low = sim.low_stock_skus()
        if low:
            print(f"\n⚠️  Low stock at shutdown: "
                  f"{', '.join(f'{k}={v}' for k, v in low.items())}")
        if export_fh:
            export_fh.close()
            print(f"\n✅ Sample data written to {args.export}")
        if not args.dry_run:
            print("\n✅ All three producers flushed and closed.")


if __name__ == "__main__":
    main()
