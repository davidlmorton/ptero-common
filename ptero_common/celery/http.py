import celery
from ptero_common import nicer_logging
from ptero_common.nicer_logging import logged_request
import json
from requests.exceptions import ConnectionError

__all__ = ['HTTP', 'HTTPWithResult']


LOG = nicer_logging.getLogger(__name__)

MIN = 60
DELAYS = [1, 5, 10, 30, 60, 2 * MIN, 4 * MIN, 10 * MIN, 30 * MIN, 60 * MIN]
DELAYS.extend([60 * MIN for i in range(72)])

CODES_TO_RETRY = set([
    408,  # Request Timeout
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
])


class HTTP(celery.Task):
    ignore_result = True
    max_retries = len(DELAYS)

    def run(self, method, url, **kwargs):
        try:
            response = getattr(logged_request, method.lower())(
                url, data=self.body(kwargs),
                headers={'Content-Type': 'application/json'},
                timeout=10, logger=LOG)
        except ConnectionError:
            delay = DELAYS[self.request.retries]
            LOG.exception(
                "A ConnectionError occured for while attempting to send "
                "%s  %s, retrying in %s seconds. Attempt %d of %d.",
                method.upper(), url, delay, self.request.retries + 1,
                self.max_retries + 1, extra={"method": method.upper(),
                    "url": url})
            self.retry(throw=False, countdown=delay)

        if response.status_code in CODES_TO_RETRY:
            delay = DELAYS[self.request.retries]
            LOG.warning(
                "Got response (%s), retrying in %s seconds.  Attempt %d of %d.",
                response.status_code, delay, self.request.retries + 1,
                self.max_retries + 1, extra={"method": method.upper(),
                    "status_code": respsonse.status_code, "url": url})
            self.retry(throw=False, countdown=delay)

        response_info = {
            "method": method,
            "url": url,
            "data": kwargs,
            "status_code": response.status_code,
            "text": response.text,
            "headers": lowercase_dict(response.headers),
        }

        if response.status_code < 200 or response.status_code >= 300:
            LOG.warning("Got response (%s), returning response info.",
                    response.status_code, extra={"method": method.upper(),
                    "status_code": respsonse.status_code, "url": url})
            return response_info
        elif not self.ignore_result:
            response_info["json"] = response.json()
            return response_info
        else:
            return

    def body(self, kwargs):
        return json.dumps(kwargs)


def lowercase_dict(dict_like):
    return {key.lower(): value for key, value in dict_like.iteritems()}


class HTTPWithResult(HTTP):
    ignore_result = False
