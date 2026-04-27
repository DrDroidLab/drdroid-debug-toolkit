import ssl
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager


class TLSMinV1_2Adapter(HTTPAdapter):
    """HTTPAdapter that pins outbound HTTPS to TLS 1.2 or higher.

    Matches the system OpenSSL default on the runtime image; declared
    explicitly so static analysers see the minimum-version requirement. The
    adapter also carries the verification posture so the SSLContext built
    here is internally consistent — relying on `session.verify=False` to
    override a CERT_REQUIRED context after the fact has historically been
    fragile across urllib3 versions.
    """

    def __init__(self, ssl_verify=True, **kwargs):
        self._ssl_verify = ssl_verify
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        if not self._ssl_verify:
            # Order matters: check_hostname must be cleared before
            # verify_mode can be set to CERT_NONE.
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        pool_kwargs["ssl_context"] = ctx
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )


def make_secure_session(ssl_verify=True):
    """Build a `requests.Session` with TLS 1.2+ enforcement.

    Use in place of module-level `requests.request(...)` so static analysers
    can see both the minimum TLS version and the verification posture.
    Pass `ssl_verify=False` only when the caller knowingly targets
    self-signed-cert endpoints; the SSLContext on the mounted adapter will
    be set to CERT_NONE in that case so it stays internally consistent.
    """
    session = requests.Session()
    session.mount("https://", TLSMinV1_2Adapter(ssl_verify=ssl_verify))
    session.verify = ssl_verify
    return session


def make_request_with_retry(method, url, headers=None, payload=None, max_retries=3, default_resend_delay=1):
    retries = 0
    while retries < max_retries:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, data=payload)
        else:
            raise ValueError(f"make_request_with_retry:: Unsupported method: {method}")

        # Check if we hit the rate limit
        if response.status_code == 429:  # Rate limit exceeded
            rate_limit_reset = int(response.headers.get("x-ratelimit-reset", default_resend_delay))
            print(f"Rate limit exceeded. Retrying in {rate_limit_reset} seconds...")
            time.sleep(rate_limit_reset)  # Wait until reset time
            retries += 1
        else:
            return response  # Return successful response

    raise Exception("make_request_with_retry:: Max retries reached")
