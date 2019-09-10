"""Endpoints for downloadable file operations"""

from multiprocessing.pool import ThreadPool

from eve import Eve

import gcloud_client


def register_files_hooks(app: Eve):
    #     app.on_fetched_resource_downloadable_files += insert_download_urls
    app.on_fetched_item_downloadable_files += insert_download_url


def insert_download_url(payload: dict):
    """
    Get a signed GCS download URL for the requested file.

    TODO: evaluate ways of caching signed URLs for files that haven't yet
    expired, so that we aren't re-generating them on every request.
    """
    object_url = payload["object_url"]
    download_url = gcloud_client.get_signed_url(object_url)
    payload["download_link"] = download_url


def insert_download_urls(payload: dict):
    """
    Get a signed GCS download URL for each of the requested files.

    NOTE: this hook is currently disabled.
    """
    # Each call to insert_download_url generates a request
    # to the GCS API, so we get a speed-up from multithreading:
    # although Python dispatches the requests synchronously,
    # the OS handles the network requests in parallel.
    with ThreadPool() as pool:
        pool.map(insert_download_url, payload["_items"])
