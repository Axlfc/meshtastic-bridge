"""
Pruebas para PacketDecoder.
"""
import pytest
from nodedb import NodeDB
from decoder import PacketDecoder
from planner_import import PlannedSite
from schemas import TextEvent, TelemetryEvent, PositionEvent, NodeInfoEvent

@pytest.fixture
def nodedb():
    return NodeDB(persist_path=None)

@pytest.fixture
def planned_sites():
    return [
        PlannedSite(
            site_id="CIMA_TARRAGONA_01",
            lat=41.1189,
            lon=1.2445,
            elev_m=150.0,
            vecinos_visibles=5,
            added_for_redundancy=False
        )
    ]

def test_decode_nodeinfo(nodedb):
    decoder = PacketDecoder(nodedb=nodedb)
    packet = {
        "fromId": "!a1b2c3d4",
        "id": 12345,
        "decoded": {
            "portnum": "NODEINFO_APP",
            "user": {
                "longName": "Nodo Cima Montaña",
                "shortName": "CIMA",
                "hwModel": "HELTEC_V3"
            }
        }
    }
    event = decoder.decode_packet(packet)
    assert isinstance(event, NodeInfoEvent)
    assert event.from_node == "!a1b2c3d4"
    assert event.payload.long_name == "Nodo Cima Montaña"
    assert event.payload.short_name == "CIMA"

    # Verify NodeDB was updated
    sender = nodedb.get_sender_info("!a1b2c3d4")
    assert sender.long_name == "Nodo Cima Montaña"
    assert sender.short_name == "CIMA"

def test_decode_text_message(nodedb):
    nodedb.update_node("!a1b2c3d4", long_name="Nodo Cima Montaña", short_name="CIMA", hw_model="HELTEC_V3")
    decoder = PacketDecoder(nodedb=nodedb)
    packet = {
        "fromId": "!a1b2c3d4",
        "toId": "^all",
        "id": 12345678,
        "rxRssi": -95,
        "rxSnr": 6.25,
        "hopLimit": 3,
        "decoded": {
            "portnum": "TEXT_MESSAGE_APP",
            "text": "Prueba de conectividad en la red mesh"
        }
    }
    event = decoder.decode_packet(packet)
    assert isinstance(event, TextEvent)
    assert event.from_node == "!a1b2c3d4"
    assert event.sender.long_name == "Nodo Cima Montaña"
    assert event.sender.short_name == "CIMA"
    assert event.signal.rssi == -95
    assert event.signal.snr == 6.25
    assert event.payload.text == "Prueba de conectividad en la red mesh"

def test_decode_position_with_planned_site(nodedb, planned_sites):
    decoder = PacketDecoder(nodedb=nodedb, planned_sites=planned_sites, match_radius_m=500.0)
    packet = {
        "fromId": "!a1b2c3d4",
        "id": 9999,
        "decoded": {
            "portnum": "POSITION_APP",
            "position": {
                "latitude": 41.1190,
                "longitude": 1.2446,
                "altitude": 150
            }
        }
    }
    event = decoder.decode_packet(packet)
    assert isinstance(event, PositionEvent)
    assert event.payload.latitude == 41.1190
    assert event.planned_site is not None
    assert event.planned_site.site_id == "CIMA_TARRAGONA_01"
