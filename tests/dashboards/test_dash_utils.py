import pytest
import dash
import dash_html_components as html
from dash.testing.composite import DashComposite
from dash.dependencies import Output, Input
from flask import request

from cidc_api.dashboards.dash_utils import create_new_dashboard


@pytest.mark.skip()
def test_create_new_dashboard():
    """Test that `create_new_dashboard` builds a `Dash` instance with the expected base URL."""
    dashboard = create_new_dashboard("test_dashboard")

    assert isinstance(dashboard, dash.Dash)
    assert dashboard.config.url_base_pathname == "/dashboards/test_dashboard/"


@pytest.mark.skip()
def test_param_injection(dash_duo: DashComposite):
    """Test that URL parameters get injected into the body of dash callback requests."""
    dashboard_id = "test-db"
    param_one = "foo"
    param_two = "bar"
    callback_complete = "done"
    test_input = "test-input"
    test_output = "test-output"

    dashboard = create_new_dashboard(dashboard_id)

    dashboard.layout = html.Div([html.Div(id=test_input), html.Div(id=test_output)])

    @dashboard.callback(Output(test_output, "children"), Input(test_input, "children"))
    def assert_url_params(_):
        # (see the URL we navigate to below)
        assert request.json["param_one"] == param_one
        assert request.json["param_two"] == param_two
        return callback_complete

    dash_duo.server(dashboard)
    dash_duo.wait_for_page(
        f"{dash_duo.server.url}/dashboards/{dashboard_id}?param_one={param_one}&param_two={param_two}"
    )

    dash_duo.wait_for_text_to_equal(f"#{test_output}", callback_complete)
