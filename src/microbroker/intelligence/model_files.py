"""Explicit public model download pinned to a commit and independently checked bytes."""
from datetime import datetime, timezone
import hashlib
import json
import logging
from http.client import IncompleteRead
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen
from .sentiment import AnalysisError

MODEL_ID = 'ProsusAI/finbert'
MODEL_REVISION = '4556d13015211d73dccd3fdd39d39232506f3e43'
EXPECTED_FILES = {
    'config.json': (758, 'f6449ddda85eb726207a40be59c0cd3bd4b142ccb27298d5e45f9ae3396b1abe'),
    'special_tokens_map.json': (112, '303df45a03609e4ead04bc3dc1536d0ab19b5358db685b6f3da123d05ec200e3'),
    'tokenizer_config.json': (252, 'ffc7913f6084b138dad68de5a0c2f0ee25ac983709260901e01f53be5796b9d9'),
    'vocab.txt': (231508, '07eced375cec144d27c900241f3e339478dec958f92fddbc551f295c992038a3'),
    'pytorch_model.bin': (437992753, 'e15a7b5738df7f17553399b6d94c6e2ff69c89245d066e8e5d183f5803a554e3'),
}
log = logging.getLogger('microbroker.model')
RANGE_BYTES = 8*1024*1024
READ_BYTES = 64*1024

class CorruptDownload(AnalysisError):
    pass

def file_sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()

def valid_file(path, expected):
    return path.is_file() and not path.is_symlink() and path.stat().st_size == expected[0] and file_sha256(path) == expected[1]

def verify_model_files(model_dir):
    folder = Path(model_dir).expanduser().resolve()
    if any((folder/(name+'.part')).exists() for name in EXPECTED_FILES):
        raise AnalysisError('FinBERT download incomplete; run prepare-finbert to resume partial bytes')
    if folder.is_dir() and any(not item.name.startswith('.') and item.name not in
                              {*EXPECTED_FILES, 'microbroker-model.json'} for item in folder.iterdir()):
        raise AnalysisError('FinBERT folder contains unexpected files; use a dedicated model directory')
    for name, expected in EXPECTED_FILES.items():
        if not valid_file(folder/name, expected):
            raise AnalysisError('FinBERT files missing or invalid; run prepare-finbert --model-dir models/finbert')
    return {'model': MODEL_ID, 'revision': MODEL_REVISION,
            'files_sha256': {name: value[1] for name, value in EXPECTED_FILES.items()}}

def prepare_finbert(model_dir='models/finbert', *, opener=urlopen, sleep=time.sleep):
    folder = Path(model_dir).expanduser().resolve(); folder.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    for name, expected in EXPECTED_FILES.items():
        dest = folder/name
        if valid_file(dest, expected): continue
        part = folder/(name+'.part')
        if part.is_symlink():
            raise AnalysisError('Model partial file must be a regular file')
        if part.exists() and part.stat().st_size > expected[0]:
            part.unlink()
        for attempt in range(5):
            try:
                offset = part.stat().st_size if part.exists() else 0
                log.info('Downloading pinned FinBERT file: %s attempt=%d resume_bytes=%d', name, attempt+1, offset)
                while offset < expected[0]:
                    end = min(offset+RANGE_BYTES, expected[0])-1
                    request = Request(f'https://huggingface.co/{MODEL_ID}/resolve/{MODEL_REVISION}/{name}',
                                      headers={'User-Agent': 'MicroBroker-R5.1', 'Accept-Encoding': 'identity',
                                               'Range': f'bytes={offset}-{end}', 'Cache-Control': 'no-cache'})
                    with opener(request, timeout=120) as response:
                        headers = getattr(response, 'headers', {})
                        status = getattr(response, 'status', 200)
                        if headers.get('Content-Encoding', 'identity').lower() != 'identity':
                            raise AnalysisError('Compressed model response cannot be resumed')
                        if status == 206:
                            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', headers.get('Content-Range', ''))
                            if (not match or int(match[1]) != offset or not offset <= int(match[2]) <= end or
                                    int(match[3]) != expected[0]):
                                raise AnalysisError('Model range response did not match requested offset')
                            response_end = int(match[2])+1
                            mode = 'ab'
                        elif status == 200:
                            # A server may ignore Range; never append its full body to a partial file.
                            if offset: log.warning('Model server ignored Range: %s; restarting this file', name)
                            offset = 0; response_end = expected[0]; mode = 'wb'
                        else:
                            raise AnalysisError('Unexpected model download status')
                        next_report = time.monotonic()+5
                        with part.open(mode) as output:
                            while True:
                                interrupted = None
                                try:
                                    block = response.read(READ_BYTES)
                                except IncompleteRead as exc:
                                    block = exc.partial; interrupted = exc
                                if offset+len(block) > response_end:
                                    raise AnalysisError('Model response exceeded declared range')
                                if block:
                                    output.write(block); offset += len(block)
                                if time.monotonic() >= next_report:
                                    output.flush()
                                    log.info('FinBERT file progress: %s bytes=%d/%d', name, offset, expected[0])
                                    next_report = time.monotonic()+5
                                if interrupted is not None: raise interrupted
                                if not block: break
                        if offset != response_end:
                            raise AnalysisError('Model connection ended before requested range completed')
                    log.info('FinBERT file progress: %s bytes=%d/%d', name, offset, expected[0])
                if not valid_file(part, expected):
                    raise CorruptDownload('Model file checksum did not match pinned release')
                part.replace(dest); downloaded += 1
                break
            except Exception as exc:
                if isinstance(exc, CorruptDownload): part.unlink(missing_ok=True)
                saved = part.stat().st_size if part.exists() else 0
                log.warning('FinBERT transfer interrupted: file=%s saved_bytes=%d/%d error_type=%s',
                            name, saved, expected[0], type(exc).__name__)
                if attempt == 4:
                    raise AnalysisError('FinBERT download paused; partial bytes preserved. Check internet and run prepare-finbert again') from None
                sleep(min(2**attempt, 10))
    manifest = verify_model_files(folder)
    manifest_path = folder/'microbroker-model.json'
    if not manifest_path.exists():
        temporary = folder/'microbroker-model.json.part'
        temporary.write_text(json.dumps({**manifest, 'prepared_at': datetime.now(timezone.utc).isoformat()}, indent=2)+'\n')
        temporary.replace(manifest_path)
    return {'status': 'ready', 'model': MODEL_ID, 'revision': MODEL_REVISION,
            'verified_files': len(EXPECTED_FILES), 'downloaded_files': downloaded,
            'download_strategy': 'validated_http_ranges', 'partial_files_preserved': True,
            'inference_network_required': False}
