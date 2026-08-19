import pytest

from radar.contracts.sources import OLIST_CONTRACTS, SourceContract, get_source_contract
from radar.ingestion.olist import REQUIRED_OLIST_FILES


def test_all_olist_files_have_a_contract() -> None:
    assert {contract.filename for contract in OLIST_CONTRACTS.values()} == REQUIRED_OLIST_FILES
    assert len(OLIST_CONTRACTS) == 9


@pytest.mark.parametrize("contract", OLIST_CONTRACTS.values(), ids=OLIST_CONTRACTS)
def test_primary_keys_exist_and_are_required(contract: SourceContract) -> None:
    assert set(contract.primary_key) <= set(contract.column_names)
    assert set(contract.primary_key) <= set(contract.required_columns)


def test_header_validation_is_order_sensitive() -> None:
    contract = get_source_contract("orders")
    contract.validate_header(list(contract.column_names))

    with pytest.raises(ValueError, match="Header incompatível"):
        contract.validate_header(list(reversed(contract.column_names)))


def test_unknown_source_fails_fast() -> None:
    with pytest.raises(ValueError, match="desconhecida"):
        get_source_contract("unknown")
