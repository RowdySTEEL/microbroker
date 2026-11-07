import logging
log = logging.getLogger("microbroker.collector")

def collect_historical(adapter, repository, request):
    log.info("Historical collection started")
    result = repository.collect(adapter.iter_bars(request), request)
    result["pages"] = adapter.pages
    log.info("Historical collection finished: status=%s bars=%d pages=%d", result["status"], result["received_bars"], result["pages"])
    return result
