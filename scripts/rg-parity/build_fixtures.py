import gzip, zipfile, tarfile, io, os, shutil
from pathlib import Path

root = Path('/tmp/parity')
shutil.rmtree(root, ignore_errors=True)
src = root / 'bucket'      # what seagrep indexes
dec = root / 'decoded'     # what rg searches (decoded equivalents)
src.mkdir(parents=True); dec.mkdir(parents=True)

def both(name, data: bytes, decoded_name=None, decoded=None):
    (src / name).write_bytes(data)
    (dec / (decoded_name or name)).write_bytes(decoded if decoded is not None else data)

body = (
    "plain needle line one\n"
    "second line without it\n"
    "needleworks at line start\n"
    "ends with a needle\n"
    "unicode naïve café needle über\n"
)
both('ascii.txt', body.encode())
both('utf8.txt', ("héllo wörld needle Ω≈ç\nsecond needle line\n").encode())
# invalid UTF-8 around a matchable token
both('invalid_utf8.txt', b"before \xff\xfe\x92 needle after\nplain needle line\n")
# UTF-16 with BOMs (rg transcodes these by default)
u16 = "windows export needle line\nsecond needle here\n"
both('utf16le.txt', '﻿'.encode('utf-16-le') + u16.encode('utf-16-le'))
both('utf16be.txt', '﻿'.encode('utf-16-be') + u16.encode('utf-16-be'))
# line terminators
both('crlf.txt', b"crlf needle line\r\nsecond crlf needle\r\n")
both('cr_only.txt', b"old mac needle line\rsecond needle\r")
both('no_newline.txt', b"final needle without newline")
both('empty.txt', b"")
both('huge_line.txt', b"x" * 500_000 + b" needle in a huge line " + b"y" * 500_000 + b"\n")
# binary: NUL before and after the match
both('binary_nul.dat', b"\x00\x01\x02 binary needle match \x00 tail\n")
both('nul_after.dat', b"early needle match\nthen\x00binary tail\n")

# containers: seagrep decodes; rg sees pre-decoded twins
inner = body.encode()
both('compressed.txt.gz', gzip.compress(inner), 'compressed.txt', inner)

zbuf = io.BytesIO()
with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as z:
    z.writestr('member_a.txt', body)
    z.writestr('sub/member_b.txt', "zip nested needle\n")
both('archive.zip', zbuf.getvalue(), 'archive_member_a.txt', body.encode())
(dec / 'archive_member_b.txt').write_bytes(b"zip nested needle\n")

tbuf = io.BytesIO()
with tarfile.open(fileobj=tbuf, mode='w:gz') as t:
    data = b"tarred needle content\nsecond tar needle\n"
    info = tarfile.TarInfo('logs/app.log'); info.size = len(data)
    t.addfile(info, io.BytesIO(data))
both('bundle.tar.gz', tbuf.getvalue(), 'bundle_app.log', b"tarred needle content\nsecond tar needle\n")

print("fixtures:", len(list(src.iterdir())), "src /", len(list(dec.iterdir())), "decoded")
