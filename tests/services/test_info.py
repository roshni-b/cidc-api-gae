INFO_ENDPOINT = "/info"


def test_info_assays(app_no_auth):
    """Check that the /info/assays endpoints returns a list of assays"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/assays")
    assert type(res.json) == list
    assert "wes" in res.json
