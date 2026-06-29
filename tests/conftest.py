import json
import pytest


SAMPLE_CDC_INSERT = {
    "before": None,
    "after": {
        "customer_id": "42",
        "name": "Jean Dupont",
        "email": "jean@example.com",
        "phone": "+33-6-11-22-33-44",
        "loyalty_tier": "gold",
    },
    "source": {"table": "customers"},
    "op": "c",
    "ts_ms": 1782736500000,
}

SAMPLE_CDC_UPDATE = {
    "before": {"loyalty_tier": "gold", "customer_id": "42"},
    "after": {
        "customer_id": "42",
        "name": "Jean Dupont",
        "email": "jean@example.com",
        "phone": "+33-6-11-22-33-44",
        "loyalty_tier": "platinum",
    },
    "source": {"table": "customers"},
    "op": "u",
    "ts_ms": 1782736501000,
}

SAMPLE_CDC_DELETE = {
    "before": {"customer_id": "42", "name": "Jean Dupont"},
    "after": None,
    "source": {"table": "customers"},
    "op": "d",
    "ts_ms": 1782736502000,
}

SAMPLE_CDC_ORDER = {
    "before": None,
    "after": {
        "order_id": "101",
        "customer_id": "42",
        "total_amount": "299.99",
        "status": "pending",
        "payment_method": "card",
    },
    "source": {"table": "orders"},
    "op": "c",
    "ts_ms": 1782736503000,
}

SAMPLE_CDC_INVENTORY = {
    "before": {"stock_quantity": "50", "sku": "SKU-LAP-001"},
    "after": {
        "product_id": "1",
        "sku": "SKU-LAP-001",
        "product_name": "Laptop Pro 16\"",
        "category": "Électronique",
        "stock_quantity": "47",
        "unit_price": "1499.99",
    },
    "source": {"table": "inventory"},
    "op": "u",
    "ts_ms": 1782736504000,
}


@pytest.fixture
def cdc_insert():
    return json.dumps(SAMPLE_CDC_INSERT)


@pytest.fixture
def cdc_update():
    return json.dumps(SAMPLE_CDC_UPDATE)


@pytest.fixture
def cdc_delete():
    return json.dumps(SAMPLE_CDC_DELETE)


@pytest.fixture
def cdc_order():
    return json.dumps(SAMPLE_CDC_ORDER)


@pytest.fixture
def cdc_inventory():
    return json.dumps(SAMPLE_CDC_INVENTORY)
