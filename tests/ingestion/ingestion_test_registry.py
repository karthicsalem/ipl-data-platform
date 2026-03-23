import json

import pytest
from ipl.ingestion.bronze.registry import extract_player_registry


@pytest.fixture
def sample_match_file(tmp_path):
    """Create a minimal match JSON for testing"""
    match_data = {
        "meta": {"data_version": "1.1.0", "created": "2025-01-01", "revision": 1},
        "info": {
            "registry": {
                "people": {
                    "Player One": "id001",
                    "Player Two": "id002",
                    "Umpire Name": "id003",  # should be excluded
                }
            },
            "players": {
                "Team A": ["Player One"],
                "Team B": ["Player Two"],
                # Umpire Name intentionally not in players
            },
        },
        "innings": [],
    }
    match_file = tmp_path / "test_match.json"
    match_file.write_text(json.dumps(match_data))
    return str(match_file)


def test_extract_player_registry_excludes_officials(sample_match_file):
    """Officials in registry but not in players should be excluded"""
    result = extract_player_registry([sample_match_file])
    player_ids = [r[0] for r in result]
    assert "id003" not in player_ids  # umpire excluded
    assert "id001" in player_ids
    assert "id002" in player_ids


def test_extract_player_registry_returns_correct_count(sample_match_file):
    """Should return exactly the number of players across both teams"""
    result = extract_player_registry([sample_match_file])
    assert len(result) == 2
