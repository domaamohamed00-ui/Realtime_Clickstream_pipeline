"""
Domain-Specific Clickstream Event Generator with Controlled Anomalies.
Big Data Engineering - SIC 8 / 2026 Project 05.
"""

import asyncio
import json
import logging
import random
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("ClickstreamGenerator")

fake = Faker()
Faker.seed(42)
random.seed(42)

# ==========================================
# DOMAIN CATALOG & MARKETING COST ATTRIBUTION
# ==========================================
CATEGORIES = {
    "Electronics": [
        ("PROD_ELEC_01", "Wireless Noise-Cancelling Headphones", 249.99),
        ("PROD_ELEC_02", "Smart Fitness Watch Ultra", 199.50),
        ("PROD_ELEC_03", "4K Ultra-HD Monitor 27-inch", 389.00),
    ],
    "Apparel": [
        ("PROD_APP_01", "Performance Waterproof Jacket", 129.00),
        ("PROD_APP_02", "Merino Wool Thermal Crew", 68.00),
        ("PROD_APP_03", "Everyday Stretch Chino Pant", 54.00),
    ],
    "Home & Living": [
        ("PROD_HOME_01", "Espresso Machine Compact", 179.99),
        ("PROD_HOME_02", "Ceramic Non-Stick Cookware Set", 145.00),
    ]
}

MARKETING_CHANNELS = ["organic", "cpc", "social", "email", "affiliate", "direct"]
PLATFORMS = ["web", "ios", "android"]

@dataclass
class ClickstreamEvent:
    event_id: str
    session_id: str
    user_id: Optional[str]
    anonymous_id: str
    event_type: str
    event_timestamp: str
    platform: str
    marketing_channel: str
    campaign: Optional[str]
    device_info: Dict[str, Any]
    page_context: Dict[str, Any]
    product_data: Optional[Dict[str, Any]] = None
    order_data: Optional[Dict[str, Any]] = None
    anomaly_label: Optional[str] = None

class ClickstreamSessionSimulator:
    """Simulates single-user session journeys with realistic transitions and injected anomalies."""

    def __init__(self, session_id: str, anomaly_type: Optional[str] = None):
        self.session_id = session_id
        self.anomaly_type = anomaly_type
        self.user_id = f"USR_{random.randint(1000, 9999)}" if random.random() > 0.4 else None
        self.anonymous_id = str(uuid.uuid4())
        self.platform = random.choice(PLATFORMS)
        self.marketing_channel = random.choice(MARKETING_CHANNELS)
        self.campaign = f"CAMPAIGN_{random.choice(['SUMMER26', 'BLACK_FRIDAY', 'INFLUENCER_TECH'])}" if self.marketing_channel in ["cpc", "social", "email"] else None
        self.ip_address = fake.ipv4()
        self.user_agent = fake.user_agent()
        self.cart: List[Dict[str, Any]] = []

    def _create_base_event(self, event_type: str, dwell_ms: int, page_url: str, category: str) -> ClickstreamEvent:
        return ClickstreamEvent(
            event_id=str(uuid.uuid4()),
            session_id=self.session_id,
            user_id=self.user_id,
            anonymous_id=self.anonymous_id,
            event_type=event_type,
            event_timestamp=datetime.now(timezone.utc).isoformat(),
            platform=self.platform,
            marketing_channel=self.marketing_channel,
            campaign=self.campaign,
            device_info={
                "user_agent": self.user_agent,
                "ip_address": self.ip_address,
                "geo_country": "US",
                "screen_resolution": "1920x1080" if self.platform == "web" else "390x844"
            },
            page_context={
                "page_url": page_url,
                "page_category": category,
                "dwell_time_ms": dwell_ms
            },
            anomaly_label=self.anomaly_type
        )

    # ----------------------------------------------------
    # ANOMALY 1: Bot-like Crawling Loop
    # ----------------------------------------------------
    async def run_bot_crawler(self) -> List[ClickstreamEvent]:
        events = []
        self.user_agent = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        for i in range(random.randint(15, 30)):
            category = random.choice(list(CATEGORIES.keys()))
            url = f"/category/{category.lower().replace(' ', '-')}/page_{i}.html"
            event = self._create_base_event(
                event_type="page_view",
                dwell_ms=random.randint(20, 80), # Unnaturally low dwell time
                page_url=url,
                category=category
            )
            events.append(event)
        return events

    # ----------------------------------------------------
    # ANOMALY 2: High-Value Cart Abandonment
    # ----------------------------------------------------
    async def run_cart_abandonment(self) -> List[ClickstreamEvent]:
        events = []
        events.append(self._create_base_event("page_view", random.randint(3000, 8000), "/", "Home"))

        # Add multiple high-value products to cart
        for _ in range(random.randint(2, 4)):
            cat = "Electronics"
            prod_id, prod_name, price = random.choice(CATEGORIES[cat])
            self.cart.append({"product_id": prod_id, "price": price})

            # View product
            ev_view = self._create_base_event("product_view", random.randint(5000, 15000), f"/product/{prod_id}", cat)
            ev_view.product_data = {"product_id": prod_id, "product_name": prod_name, "category": cat, "unit_price": price, "quantity": 1}
            events.append(ev_view)

            # Add to cart
            ev_add = self._create_base_event("add_to_cart", random.randint(1000, 3000), f"/product/{prod_id}", cat)
            ev_add.product_data = ev_view.product_data
            events.append(ev_add)

        # Abrupt session exit without checkout
        events.append(self._create_base_event("session_exit", 500, "/cart", "Cart"))
        return events

    # ----------------------------------------------------
    # ANOMALY 3: Promotional Purchase Surge (Flash Sale)
    # ----------------------------------------------------
    async def run_promo_surge(self) -> List[ClickstreamEvent]:
        events = []
        self.campaign = "CAMPAIGN_SUMMER_FLASH_SALE"
        self.marketing_channel = "social"
        cat = "Electronics"
        prod_id, prod_name, price = CATEGORIES[cat][0]
        discounted_price = round(price * 0.6, 2)  # 40% promotional discount

        events.append(self._create_base_event("page_view", 800, "/promotions/flash-sale", "Promo"))

        ev_cart = self._create_base_event("add_to_cart", 1200, f"/product/{prod_id}", cat)
        ev_cart.product_data = {"product_id": prod_id, "product_name": prod_name, "category": cat, "unit_price": discounted_price, "quantity": 1}
        events.append(ev_cart)

        events.append(self._create_base_event("checkout_start", 2500, "/checkout", "Checkout"))

        order_ev = self._create_base_event("purchase", 2000, "/order-confirmation", "Checkout")
        order_ev.order_data = {
            "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
            "cart_total": discounted_price,
            "item_count": 1,
            "discount_code": "FLASH40"
        }
        events.append(order_ev)
        return events

    # ----------------------------------------------------
    # STANDARD HUMAN JOURNEY (Markov Transition)
    # ----------------------------------------------------
    async def run_standard_journey(self) -> List[ClickstreamEvent]:
        events = []
        events.append(self._create_base_event("page_view", random.randint(2000, 10000), "/", "Home"))

        # 70% proceed to search/browse
        if random.random() < 0.70:
            cat = random.choice(list(CATEGORIES.keys()))
            events.append(self._create_base_event("search_filter", random.randint(4000, 12000), f"/category/{cat}", cat))

            # 50% view a product
            if random.random() < 0.50:
                prod_id, prod_name, price = random.choice(CATEGORIES[cat])
                ev_view = self._create_base_event("product_view", random.randint(5000, 20000), f"/product/{prod_id}", cat)
                ev_view.product_data = {"product_id": prod_id, "product_name": prod_name, "category": cat, "unit_price": price, "quantity": 1}
                events.append(ev_view)

                # 35% add to cart
                if random.random() < 0.35:
                    ev_cart = self._create_base_event("add_to_cart", random.randint(2000, 6000), f"/product/{prod_id}", cat)
                    ev_cart.product_data = ev_view.product_data
                    events.append(ev_cart)

                    # 60% start checkout
                    if random.random() < 0.60:
                        events.append(self._create_base_event("checkout_start", random.randint(5000, 15000), "/checkout", "Checkout"))

                        # 75% complete purchase
                        if random.random() < 0.75:
                            ev_pur = self._create_base_event("purchase", random.randint(3000, 8000), "/checkout/success", "Checkout")
                            ev_pur.order_data = {
                                "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
                                "cart_total": price,
                                "item_count": 1,
                                "discount_code": None
                            }
                            events.append(ev_pur)

        events.append(self._create_base_event("session_exit", 100, "/exit", "Exit"))
        return events

    async def generate_events(self) -> List[ClickstreamEvent]:
        if self.anomaly_type == "bot_crawler":
            return await self.run_bot_crawler()
        elif self.anomaly_type == "cart_abandonment":
            return await self.run_cart_abandonment()
        elif self.anomaly_type == "promo_surge":
            return await self.run_promo_surge()
        else:
            return await self.run_standard_journey()


class ClickstreamStreamEngine:
    """Orchestrates concurrent generation and dispatches to Kafka broker or batch JSON."""

    def __init__(self, kafka_bootstrap: Optional[str] = None, topic: str = "clickstream-raw"):
        self.kafka_bootstrap = kafka_bootstrap
        self.topic = topic
        self.producer = None

    async def start(self):
        if self.kafka_bootstrap:
            try:
                from aiokafka import AIOKafkaProducer
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                await self.producer.start()
                logger.info("Connected to Kafka broker at %s", self.kafka_bootstrap)
            except Exception as e:
                logger.warning("Could not connect to Kafka: %s. Falling back to local logging.", e)
                self.producer = None

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def emit_event(self, event: ClickstreamEvent):
        payload = asdict(event)
        if self.producer:
            await self.producer.send_and_wait(self.topic, payload)
        else:
            # Fallback to stdout log
            logger.info("EMIT [%s] %s session=%s user=%s anomaly=%s",
                        event.event_type, event.page_context.get("page_url"),
                        event.session_id[:8], event.user_id, event.anomaly_label)

    async def run_session_worker(self, worker_id: int, total_sessions: int):
        for _ in range(total_sessions):
            session_id = str(uuid.uuid4())

            # Inject controlled anomalies based on fixed probability dials
            rand_roll = random.random()
            if rand_roll < 0.10:
                anomaly = "bot_crawler"
            elif rand_roll < 0.25:
                anomaly = "cart_abandonment"
            elif rand_roll < 0.35:
                anomaly = "promo_surge"
            else:
                anomaly = None

            simulator = ClickstreamSessionSimulator(session_id=session_id, anomaly_type=anomaly)
            events = await simulator.generate_events()

            for event in events:
                await self.emit_event(event)
                await asyncio.sleep(0.05) # Throttle event emission rate


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Real-Time Clickstream Generator")
    parser.add_argument("--mode", choices=["kafka", "batch"], default="batch", help="Output destination")
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap server")
    parser.add_argument("--sessions", type=int, default=10, help="Number of sessions to generate per worker")
    parser.add_argument("--workers", type=int, default=3, help="Concurrent session workers")
    parser.add_argument("--output-file", default="sample_clickstream.json", help="File for batch mode")
    args = parser.parse_args()

    engine = ClickstreamStreamEngine(
        kafka_bootstrap=args.bootstrap if args.mode == "kafka" else None
    )

    await engine.start()
    logger.info("Starting generator: %d workers, %d sessions each. Mode=%s", args.workers, args.sessions, args.mode)

    tasks = [engine.run_session_worker(w, args.sessions) for w in range(args.workers)]
    await asyncio.gather(*tasks)

    await engine.stop()
    logger.info("Generation finished successfully.")

if __name__ == "__main__":
    asyncio.run(main())
