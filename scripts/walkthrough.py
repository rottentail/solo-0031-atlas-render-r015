#!/usr/bin/env python3
"""Run bounded production flows against a temporary Atlas Render server."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zlib

ROOT = Path(__file__).resolve().parents[1]


def sample():
    return {"width": 320, "height": 240, "background": "#F5F1E8", "layers": [
        {"id": "plate", "kind": "rectangle", "width": 220, "height": 150,
         "transform": {"x": 45, "y": 35, "scaleX": 1, "scaleY": 1, "degrees": 5},
         "opacity": 1, "fill": {"kind": "linear", "color": "#145A70", "endColor": "#32A5A0", "from": {"x": 0, "y": 0}, "to": {"x": 220, "y": 150}}},
        {"id": "accent", "kind": "ellipse", "width": 130, "height": 130,
         "transform": {"x": 150, "y": 95, "scaleX": 1, "scaleY": 1, "degrees": 0},
         "opacity": .85, "fill": {"kind": "solid", "color": "#E87E4F"}},
        {"id": "inset", "kind": "group", "transform": {"x": 150, "y": 100, "scaleX": 1, "scaleY": 1, "degrees": -8}, "opacity": .9,
         "clip": {"x": 0, "y": 0, "width": 90, "height": 75}, "children": [
             {"id": "facet", "kind": "polygon", "width": 100, "height": 100,
              "transform": {"scaleX": 1, "scaleY": 1}, "opacity": .8,
              "fill": {"kind": "solid", "color": "#FFF4D5"}, "points": [{"x": 10, "y": 90}, {"x": 50, "y": 0}, {"x": 100, "y": 90}]}]}]}


def tiny_png():
    def chunk(tag, data):
        return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', zlib.crc32(tag + data))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', 2, 2, 8, 6, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(b'\0' + bytes([240, 40, 80, 255] * 2) + b'\0' + bytes([40, 80, 240, 128] * 2))) + chunk(b'IEND', b'')


def heavy_document():
    doc = sample()
    doc['width'] = 1500
    doc['height'] = 1500
    doc['layers'] = [dict(doc['layers'][0], id=f'sheet-{i}', width=1500, height=1500, transform={'scaleX': 1, 'scaleY': 1}) for i in range(16)]
    return doc


# 任务生命周期协议的外部镜像：事件种类 → (允许的前置状态, 提交后状态)。
# 与 internal/model/jobs.go 中的协议表一一对应；重放事件流必须落回已提交状态。
JOB_PROTOCOL = {
    'job.queued': (frozenset([None]), 'queued'),
    'job.running': (frozenset(['queued']), 'running'),
    'job.cancelled': (frozenset(['queued', 'running']), 'cancelled'),
    'job.retried': (frozenset(['failed', 'cancelled']), 'queued'),
    'job.recovered': (frozenset(['running']), 'queued'),
    'job.succeeded': (frozenset(['running']), 'succeeded'),
    'job.failed': (frozenset(['running']), 'failed'),
}


class Demo:
    def __init__(self, binary, directory):
        self.binary = binary
        self.directory = directory
        self.proc = None
        self.log = open(directory / 'server.log', 'ab')
        self.start()

    def start(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}'
        env = dict(os.environ, ATLAS_ADDR=f'127.0.0.1:{port}', ATLAS_STATE=str(self.directory / 'state.json'))
        self.proc = subprocess.Popen([str(self.binary)], env=env, stdout=self.log, stderr=self.log)
        for _ in range(100):
            try:
                self.call('GET', '/healthz')
                return
            except (urllib.error.URLError, ConnectionError):
                if self.proc.poll() is not None:
                    raise RuntimeError((self.directory / 'server.log').read_text())
                time.sleep(.03)
        raise RuntimeError('server startup deadline exceeded')

    def stop(self, abrupt=False):
        if self.proc and self.proc.poll() is None:
            (self.proc.kill if abrupt else self.proc.terminate)()
            self.proc.wait(timeout=15)

    def call(self, method, path, body=None, status=200, headers=None):
        hdr = dict(headers or {})
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            hdr['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.url + path, data=body, method=method, headers=hdr)
        try:
            response = urllib.request.urlopen(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read()
            if response.status != status:
                raise RuntimeError(f'{method} {path}: expected {status}, received {response.status}: {data[:500]!r}')
            if response.headers.get('Content-Type', '').startswith('application/json'):
                return json.loads(data)
            return data

    def wait(self, job_id, wanted=('succeeded',)):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            job = self.call('GET', '/v1/jobs/' + job_id)
            if job['state'] in wanted:
                return job
            if job['state'] in ('failed', 'cancelled'):
                raise RuntimeError(f'unexpected job outcome: {job}')
            time.sleep(.025)
        raise RuntimeError('render deadline exceeded')

    def submit(self, snapshot, key='render-one', scale=1):
        return self.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': scale}, 202, {'Idempotency-Key': key})


def job_protocol(demo):
    doc = sample()
    demo.call('PUT', '/v1/scenes/emblem', {'name': '协议场景', 'expectedRevision': 0, 'document': doc})
    small = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 1}, 201)
    demo.call('PUT', '/v1/scenes/emblem', {'name': '协议场景', 'expectedRevision': 1, 'document': heavy_document()})
    big = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 201)

    # 队列容量：两个重任务占住执行器期间，提交与重试共享同一容量规则。
    blockers = [demo.submit(big, f'proto-block-{i}') for i in range(2)]
    fills = [demo.submit(small, f'proto-fill-{i}') for i in range(62)]
    demo.call('POST', '/v1/jobs', {'snapshotId': small['id'], 'scale': 1}, 429, {'Idempotency-Key': 'proto-overflow'})
    evicted = fills[0]['id']
    demo.call('POST', f'/v1/jobs/{evicted}/cancel')
    fills.append(demo.submit(small, 'proto-fill-62'))
    demo.call('POST', f'/v1/jobs/{evicted}/retry', status=429)
    for f in fills[1:]:
        demo.call('POST', f"/v1/jobs/{f['id']}/cancel")
    for b in blockers:
        assert demo.wait(b['id'])['state'] == 'succeeded'
    for f in fills:
        j = demo.call('GET', f"/v1/jobs/{f['id']}")
        assert j['state'] == 'cancelled' and not j.get('artifactId'), j

    # 排队中取消是唯一已提交事实；重复取消幂等，执行器稍候也不会再领取它。
    busy = demo.submit(big, 'proto-busy')
    demo.wait(busy['id'], wanted=('running',))
    queued = demo.submit(small, 'proto-queued')
    first = demo.call('POST', f"/v1/jobs/{queued['id']}/cancel")
    again = demo.call('POST', f"/v1/jobs/{queued['id']}/cancel")
    assert first['state'] == again['state'] == 'cancelled'
    demo.wait(busy['id'])
    time.sleep(.2)
    q = demo.call('GET', f"/v1/jobs/{queued['id']}")
    assert q['state'] == 'cancelled' and not q.get('artifactId'), q

    # 运行中取消与立即重试真实交错：旧尝试的完成事实被围栏丢弃，
    # 取消与成功各自只提交一次，终态后取消和重试都被拒绝。
    racer = demo.submit(big, 'proto-racer')
    demo.wait(racer['id'], wanted=('running',))
    stopped = demo.call('POST', f"/v1/jobs/{racer['id']}/cancel")
    assert stopped['state'] == 'cancelled'
    retried = demo.call('POST', f"/v1/jobs/{racer['id']}/retry", status=202)
    assert retried['attempt'] == 2 and retried['state'] == 'queued'
    result = demo.wait(racer['id'])
    assert result['attempt'] == 2 and result['artifactId']
    demo.call('POST', f"/v1/jobs/{racer['id']}/cancel", status=409)
    demo.call('POST', f"/v1/jobs/{racer['id']}/retry", status=409)
    assert demo.call('GET', f"/v1/jobs/{racer['id']}")['state'] == 'succeeded'

    # 同键重放穿过取消、重试和成功仍返回原任务；改换语义返回409。
    assert demo.submit(big, 'proto-racer')['id'] == racer['id']
    demo.call('POST', '/v1/jobs', {'snapshotId': big['id'], 'scale': 2}, 409, {'Idempotency-Key': 'proto-racer'})

    # 清理结果字节保留成功事实。
    demo.call('DELETE', f"/v1/artifacts/{result['artifactId']}")
    demo.call('GET', f"/v1/artifacts/{result['artifactId']}", status=410)
    kept = demo.call('GET', f"/v1/jobs/{racer['id']}")
    assert kept['state'] == 'succeeded' and kept['artifactId'] == result['artifactId']

    # SIGKILL 重启：运行中任务重新入队，attempt 不变，最终由同一固定输入完成。
    victim = demo.submit(big, 'proto-victim')
    demo.wait(victim['id'], wanted=('running',))
    demo.stop(abrupt=True)
    demo.start()
    recovered = demo.wait(victim['id'])
    assert recovered['attempt'] == 1 and recovered['state'] == 'succeeded'
    png = demo.call('GET', f"/v1/artifacts/{recovered['artifactId']}")
    assert png[:8] == b'\x89PNG\r\n\x1a\n'

    # 重启后：旧幂等键仍重放原任务，成功任务不可取消，排队取消仍生效。
    assert demo.submit(big, 'proto-victim')['id'] == victim['id']
    demo.call('POST', f"/v1/jobs/{victim['id']}/cancel", status=409)
    busy2 = demo.submit(big, 'proto-busy-2')
    demo.wait(busy2['id'], wanted=('running',))
    queued2 = demo.submit(small, 'proto-queued-2')
    assert demo.call('POST', f"/v1/jobs/{queued2['id']}/cancel")['state'] == 'cancelled'
    demo.wait(busy2['id'])

    # 审计：每个任务的事件流按协议表重放，必须落回 GET 返回的已提交状态。
    events = demo.call('GET', '/v1/events')
    by_job = {}
    for e in events:
        if e['kind'].startswith('job.'):
            by_job.setdefault(e['subject'], []).append(e['kind'])
    for job_id, stream in by_job.items():
        state = None
        for kind in stream:
            allowed, target = JOB_PROTOCOL[kind]
            assert state in allowed, f'{job_id}: {kind} committed from {state}'
            state = target
        committed = demo.call('GET', f'/v1/jobs/{job_id}')['state']
        assert committed == state, f'{job_id}: replay reached {state}, committed is {committed}'
    racer_stream = by_job[racer['id']]
    assert racer_stream.count('job.cancelled') == 1 and racer_stream.count('job.succeeded') == 1
    assert racer_stream.index('job.cancelled') < racer_stream.index('job.retried') < racer_stream.index('job.succeeded')
    assert by_job[queued['id']] == ['job.queued', 'job.cancelled']
    assert by_job[victim['id']].count('job.recovered') == 1
    return {'workflow': 'job-protocol', 'jobsAudited': len(by_job),
            'cancelledFills': len(fills), 'racerAttempt': result['attempt'],
            'recoveredAttempt': recovered['attempt'], 'recoveredPngBytes': len(png)}


def run(demo, workflow, out):
    if workflow == 'job-protocol':
        return job_protocol(demo)
    d = sample()
    put = lambda doc, rev=0: demo.call('PUT', '/v1/scenes/emblem', {'name': '双色印记', 'expectedRevision': rev, 'document': doc})
    scene = put(d)
    if workflow == 'create-scene':
        assert demo.call('GET', '/v1/scenes/emblem')['document'] == scene['document']
        bad = sample(); bad['layers'][0]['transform']['scaleX'] = 0
        demo.call('PUT', '/v1/scenes/invalid', {'name': '无效几何', 'expectedRevision': 0, 'document': bad}, 400)
    elif workflow == 'revise-scene':
        d['background'] = '#F0F0F0'; assert put(d, 1)['revision'] == 2
        demo.call('PUT', '/v1/scenes/emblem', {'name': '旧版本', 'expectedRevision': 1, 'document': d}, 409)
    elif workflow == 'archive-scene':
        archived = demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': 1, 'state': 'archived'})
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': archived['revision']}, 409)
        assert demo.call('POST', '/v1/scenes/emblem/state', {'expectedRevision': archived['revision'], 'state': 'active'})['revision'] == 3
    if workflow in ('import-asset', 'retire-asset'):
        asset = demo.call('PUT', '/v1/assets/ribbon', tiny_png(), 201)
        assert asset['width'] == 2 and asset['digest'] == hashlib.sha256(tiny_png()).hexdigest()
        demo.call('PUT', '/v1/assets/broken', b'not png', 400)
        layer = {'id': 'ribbon', 'kind': 'image', 'width': 60, 'height': 60, 'assetId': 'ribbon', 'opacity': 1, 'transform': {'x': 20, 'y': 30, 'scaleX': 1, 'scaleY': 1}}
        d['layers'].append(layer); put(d, 1)
        pinned = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 201)
        demo.call('POST', '/v1/assets/ribbon/retire')
        demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': 2}, 409)
        result = demo.wait(demo.submit(pinned)['id']); assert result['state'] == 'succeeded'
        return {'workflow': workflow, 'assetDigest': asset['digest'], 'pinnedRender': result['state']}
    current = demo.call('GET', '/v1/scenes/emblem')
    snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision']}, 201)
    if workflow == 'freeze-scene':
        original_digest = snapshot['digest']; d['background'] = '#000000'; put(d, current['revision'])
        assert demo.call('GET', '/v1/snapshots/' + snapshot['id'])['digest'] == original_digest
    if workflow in ('export-package', 'import-package'):
        package = demo.call('GET', '/v1/snapshots/' + snapshot['id'] + '/package')
        restored = demo.call('POST', '/v1/imports/restored', {'name': '恢复的印记', 'package': package}, 201)
        assert restored['document'] == snapshot['document']
        corrupt = json.loads(json.dumps(package)); corrupt['snapshot']['document']['width'] += 1
        demo.call('POST', '/v1/imports/corrupt', {'name': '损坏包', 'package': corrupt}, 400)
        demo.call('GET', '/v1/scenes/corrupt', status=404)
    if workflow in ('cancel-render', 'retry-render', 'recover-render'):
        put(heavy_document(), current['revision'])
        snapshot = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
    job = demo.submit(snapshot)
    if workflow == 'submit-render':
        assert demo.submit(snapshot)['id'] == job['id']
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 2}, 409, {'Idempotency-Key': 'render-one'})
    if workflow == 'revoke-snapshot':
        demo.call('POST', '/v1/snapshots/' + snapshot['id'] + '/revoke')
        demo.call('POST', '/v1/jobs', {'snapshotId': snapshot['id'], 'scale': 1}, 409, {'Idempotency-Key': 'blocked-after-revoke'})
    if workflow in ('cancel-render', 'retry-render'):
        stopped = demo.call('POST', '/v1/jobs/' + job['id'] + '/cancel')
        assert stopped['state'] == 'cancelled'
        if workflow == 'cancel-render':
            time.sleep(.1); assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'cancelled'
            return {'workflow': workflow, 'state': 'cancelled'}
        job = demo.call('POST', '/v1/jobs/' + job['id'] + '/retry', status=202)
        assert job['attempt'] == 2
    if workflow == 'recover-render':
        demo.wait(job['id'], wanted=('running',)); demo.stop(abrupt=True); demo.start()
    result = demo.wait(job['id'])
    png = demo.call('GET', '/v1/artifacts/' + result['artifactId'])
    assert png[:8] == b'\x89PNG\r\n\x1a\n'
    assert struct.unpack('>II', png[16:24]) == (snapshot['document']['width'], snapshot['document']['height'])
    if out:
        Path(out).mkdir(parents=True, exist_ok=True); (Path(out) / 'atlas-sample.png').write_bytes(png)
    if workflow == 'compare-renders':
        other = demo.wait(demo.submit(snapshot, 'render-two')['id'])
        comparison = demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': other['artifactId']})
        assert comparison['changedPixels'] == 0
        d['background'] = '#000000'; put(d, current['revision'])
        altered = demo.call('POST', '/v1/scenes/emblem/snapshots', {'expectedRevision': current['revision'] + 1}, 201)
        changed = demo.wait(demo.submit(altered, 'render-three')['id'])
        assert demo.call('POST', '/v1/compare', {'left': result['artifactId'], 'right': changed['artifactId']})['changedPixels'] > 0
    if workflow == 'purge-artifact':
        demo.call('DELETE', '/v1/artifacts/' + result['artifactId'])
        demo.call('GET', '/v1/artifacts/' + result['artifactId'], status=410)
        assert demo.call('GET', '/v1/jobs/' + job['id'])['state'] == 'succeeded'
    if workflow == 'observe-service':
        summary = demo.call('GET', '/v1/status'); assert summary['jobs']['succeeded'] >= 1
        events = demo.call('GET', '/v1/events'); assert any(e['kind'] == 'job.succeeded' for e in events)
    return {'workflow': workflow, 'state': result['state'], 'pngBytes': len(png), 'pngDigest': hashlib.sha256(png).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workflow', default='submit-render')
    parser.add_argument('--out')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='atlas-walkthrough-') as temp:
        directory = Path(temp); binary = directory / 'atlas'
        subprocess.run(['go', 'build', '-o', str(binary), './cmd/atlas'], cwd=ROOT, check=True, timeout=60)
        demo = Demo(binary, directory)
        try:
            print(json.dumps(run(demo, args.workflow, args.out), ensure_ascii=False))
        finally:
            demo.stop(); demo.log.close()


if __name__ == '__main__':
    main()
