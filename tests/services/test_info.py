INFO_ENDPOINT = "/info"


def test_info_assays(app_no_auth):
    """Check that the /info/assays endpoint returns a list of assays"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/assays")
    assert type(res.json) == list
    assert "wes" in res.json


def test_info_manifests(app_no_auth):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/manifests")
    assert type(res.json) == list
    assert "pbmc" in res.json


def test_info_extra_types(app_no_auth):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/extra_data_types")
    assert type(res.json) == list
    assert "participants info" in res.json
