import dash

# This HTML template overrides Dash's default to enable passing values
# (e.g., a user's identity token) in the JSON body of all Dash ajax requests
# via URL query params. For example, loading a Dash dashboard like so:
#
#   <iframe src="https://api.cimac-network.org/dashboard-url?id_token=foo&some_param=bar" />
#
# will automatically insert {"id_token": "foo", "some_param": "bar"} into the
# JSON body of every ajax request the dashboard makes after loading into the iframe.
# This is useful for a) performing user authentication and authorization and
# b) parameterizing dashboards (i.e., having a generic trial dashboard that takes
# a protocol identifier as a parameter to determine what to render).
#
# The operative code here is in the <script id="_dash-renderer">...</script> tag.
index_string = """
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%css%}
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                <script id="_dash-renderer">
                    // Inject parameters from the current URL's query string into
                    // the body of every data update request that Dash sends.
                    const urlParams = new URLSearchParams(window.location.search);
                    const renderer = new DashRenderer({
                        request_pre: (req) => {
                            for (const [param, value] of urlParams.entries()) {
                                req[param] = value;
                            }
                        },
                        request_post: (req, res) => {}
                    })
                </script>
            </footer>
        </body>
    </html>
"""


def create_new_dashboard(dashboard_name: str) -> dash.Dash:
    """Initialize a new Dash dashboard with CIDC configuration defaults."""
    return dash.Dash(
        url_base_pathname=f"/dashboards/{dashboard_name}/", index_string=index_string
    )
