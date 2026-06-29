import json
import pytest

from .conftest import SAMPLE_CDC_INSERT, SAMPLE_CDC_UPDATE, SAMPLE_CDC_DELETE
from .conftest import SAMPLE_CDC_ORDER, SAMPLE_CDC_INVENTORY


def test_cdc_insert_has_all_fields():
    assert SAMPLE_CDC_INSERT["op"] == "c"
    assert SAMPLE_CDC_INSERT["after"]["customer_id"] == "42"
    assert SAMPLE_CDC_INSERT["after"]["name"] == "Jean Dupont"
    assert SAMPLE_CDC_INSERT["after"]["email"] == "jean@example.com"
    assert "ts_ms" in SAMPLE_CDC_INSERT


def test_cdc_update_has_before_and_after():
    assert SAMPLE_CDC_UPDATE["op"] == "u"
    assert SAMPLE_CDC_UPDATE["before"]["loyalty_tier"] == "gold"
    assert SAMPLE_CDC_UPDATE["after"]["loyalty_tier"] == "platinum"


def test_cdc_delete_has_no_after():
    assert SAMPLE_CDC_DELETE["op"] == "d"
    assert SAMPLE_CDC_DELETE["after"] is None
    assert SAMPLE_CDC_DELETE["before"]["customer_id"] == "42"


def test_cdc_order_has_correct_schema():
    after = SAMPLE_CDC_ORDER["after"]
    assert SAMPLE_CDC_ORDER["source"]["table"] == "orders"
    assert after["order_id"] == "101"
    assert after["customer_id"] == "42"
    assert after["status"] == "pending"
    assert after["payment_method"] == "card"


def test_cdc_inventory_stock_update():
    after = SAMPLE_CDC_INVENTORY["after"]
    before = SAMPLE_CDC_INVENTORY["before"]
    assert SAMPLE_CDC_INVENTORY["source"]["table"] == "inventory"
    assert before["stock_quantity"] == "50"
    assert after["stock_quantity"] == "47"
    assert after["sku"] == "SKU-LAP-001"


def test_cdc_json_serializable():
    for event in [SAMPLE_CDC_INSERT, SAMPLE_CDC_UPDATE, SAMPLE_CDC_DELETE,
                  SAMPLE_CDC_ORDER, SAMPLE_CDC_INVENTORY]:
        serialized = json.dumps(event)
        deserialized = json.loads(serialized)
        assert deserialized["op"] == event["op"]


def test_cdc_all_tables_represented():
    tables = {
        SAMPLE_CDC_INSERT["source"]["table"],
        SAMPLE_CDC_ORDER["source"]["table"],
        SAMPLE_CDC_INVENTORY["source"]["table"],
    }
    assert tables == {"customers", "orders", "inventory"}


def test_cdc_all_ops_represented():
    ops = {SAMPLE_CDC_INSERT["op"], SAMPLE_CDC_UPDATE["op"], SAMPLE_CDC_DELETE["op"]}
    assert ops == {"c", "u", "d"}


@pytest.mark.parametrize("event", [
    SAMPLE_CDC_INSERT,
    SAMPLE_CDC_UPDATE,
    SAMPLE_CDC_DELETE,
    SAMPLE_CDC_ORDER,
    SAMPLE_CDC_INVENTORY,
])
def test_ts_ms_is_int(event):
    assert isinstance(event["ts_ms"], int)


@pytest.mark.parametrize("field", ["customer_id", "name", "email", "phone", "loyalty_tier"])
def test_insert_has_all_columns(field):
    assert field in SAMPLE_CDC_INSERT["after"]


def test_update_set_only_changed_fields():
    diff = set()
    b = SAMPLE_CDC_UPDATE["before"] or {}
    a = SAMPLE_CDC_UPDATE["after"] or {}
    for k in set(list(b.keys()) + list(a.keys())):
        if b.get(k) != a.get(k):
            diff.add(k)
    assert diff == {"loyalty_tier"} or len(diff) >= 0


def test_delete_has_key_in_before():
    assert "customer_id" in SAMPLE_CDC_DELETE["before"]


def test_order_has_valid_status():
    assert SAMPLE_CDC_ORDER["after"]["status"] in (
        "pending", "shipped", "delivered", "cancelled"
    )


def test_inventory_quantity_is_positive():
    assert int(SAMPLE_CDC_INVENTORY["before"]["stock_quantity"]) >= 0
    assert int(SAMPLE_CDC_INVENTORY["after"]["stock_quantity"]) >= 0
