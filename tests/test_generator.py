import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir, "src", "scripts"))

from generate_changes import TABLE_ACTIONS, PRODUCTS, FIRST_NAMES, LAST_NAMES


def test_table_actions_defined():
    assert "customers" in TABLE_ACTIONS
    assert "orders" in TABLE_ACTIONS
    assert "order_items" in TABLE_ACTIONS
    assert "inventory" in TABLE_ACTIONS


def test_table_actions_have_at_least_one_action():
    for table, actions in TABLE_ACTIONS.items():
        assert len(actions) >= 1, f"{table} has no actions"


def test_products_are_valid():
    for product in PRODUCTS:
        sku, name, category, price = product
        assert isinstance(sku, str) and sku.startswith("SKU-")
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(category, str) and len(category) > 0
        assert isinstance(price, float) and price > 0


def test_first_names_non_empty():
    assert all(isinstance(n, str) and len(n) > 0 for n in FIRST_NAMES)


def test_last_names_non_empty():
    assert all(isinstance(n, str) and len(n) > 0 for n in LAST_NAMES)


def test_actions_include_insert():
    all_actions = []
    for actions in TABLE_ACTIONS.values():
        all_actions.extend(actions)
    assert "insert" in all_actions
    assert "update" in all_actions


def test_actions_include_delete_for_orders():
    assert "delete" in TABLE_ACTIONS["orders"]


def test_products_have_diverse_categories():
    categories = {p[2] for p in PRODUCTS}
    assert len(categories) >= 2


@pytest.mark.parametrize("sku_prefix", ["SKU-LAP", "SKU-PHO", "SKU-BOO", "SKU-HOM", "SKU-SPO"])
def test_sku_prefixes(sku_prefix):
    assert any(p[0].startswith(sku_prefix) for p in PRODUCTS)


def test_no_duplicate_skus():
    skus = [p[0] for p in PRODUCTS]
    assert len(skus) == len(set(skus))
