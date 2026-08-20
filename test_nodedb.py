"""
Pruebas para NodeDB.
"""
import pytest
from pathlib import Path
from nodedb import NodeDB, format_node_id

def test_format_node_id():
    assert format_node_id(123456789) == "!075bcd15"
    assert format_node_id("075bcd15") == "!075bcd15"
    assert format_node_id("!075bcd15") == "!075bcd15"
    assert format_node_id("^all") == "^all"
    assert format_node_id("broadcast") == "broadcast"

def test_nodedb_update_and_get(tmp_path: Path):
    db_file = tmp_path / "nodedb.json"
    db = NodeDB(persist_path=db_file)

    db.update_node(
        node_id="!a1b2c3d4",
        long_name="Nodo Cima Montaña",
        short_name="CIMA",
        hw_model="HELTEC_V3",
        battery_level=88
    )

    node = db.get_node("!a1b2c3d4")
    assert node is not None
    assert node.long_name == "Nodo Cima Montaña"
    assert node.short_name == "CIMA"
    assert node.hw_model == "HELTEC_V3"
    assert node.battery_level == 88

    # Partial update without overwriting
    db.update_node(
        node_id="!a1b2c3d4",
        battery_level=85,
        voltage=4.12
    )

    node2 = db.get_node("!a1b2c3d4")
    assert node2.long_name == "Nodo Cima Montaña"
    assert node2.battery_level == 85
    assert node2.voltage == 4.12

    # Save to disk and reload
    db.save_to_disk()
    assert db_file.exists()

    db2 = NodeDB(persist_path=db_file)
    reloaded_node = db2.get_node("!a1b2c3d4")
    assert reloaded_node is not None
    assert reloaded_node.long_name == "Nodo Cima Montaña"
    assert reloaded_node.voltage == 4.12
