var fs = require('fs')
var path = require('path')
var http = require('http')
var tape = require('tape')
var ddrive = require('@ddrive/core')
var DPack = require('@dpack/core')
var dwsChain = require('@dwcore/dws-chain')
var dwrem = require('@dwcore/rem')
var tmp = require('temporary-directory')
var ecstatic = require('ecstatic')
var dPackHttp = require('./')

tape('can end replication immediately', function (t) {
  makeTestServer(t, function runTest (dpackDir, destDir, cleanup) {
    var storage = dPackHttp('http://localhost:9988')
    var httpDDrive = ddrive(storage, {latest: true})
    httpDDrive.on('ready', function () {
      DPack(destDir, {key: httpDDrive.key, thin: true}, function (err, dpack) {
        if (err) return t.ifErr(err, 'error')
        var localReplicate = dpack.vault.replicate()
        var httpReplicate = httpDDrive.replicate()
        localReplicate.pipe(httpReplicate).pipe(localReplicate)
        localReplicate.end()
        httpReplicate.end()
        var pending = 2
        localReplicate.on('end', function () {
          console.log('local replicate ended')
          if (--pending === 0) onEnd()
        })
        httpReplicate.on('end', function () {
          console.log('http replicate ended')
          if (--pending === 0) onEnd()
        })
        function onEnd () {
          httpDDrive.close(function () {
            dpack.close(function () {
              cleanup()
            })
          })
        }
      })
    })
  })
})

tape('replicate file', function (t) {
  makeTestServer(t, function runTest (dpackDir, destDir, cleanup) {
    var storage = dPackHttp('http://localhost:9988')
    var httpDDrive = ddrive(storage, {latest: true})
    httpDDrive.on('ready', function () {
      DPack(destDir, {key: httpDDrive.key, thin: true}, function (err, dpack) {
        if (err) return t.ifErr(err, 'error')
        var localReplicate = dpack.vault.replicate()
        localReplicate.pipe(httpDDrive.replicate()).pipe(localReplicate)
        dpack.vault.readFile('/greetings.txt', function (err, content) {
          if (err) return t.ifErr(err, 'error')
          t.equals(content.toString(), 'greetings', 'got greetings')
          t.equals(fs.readFileSync(path.join(destDir, 'greetings.txt')).toString(), 'greetings', 'file exists and matches')
          httpDDrive.close()
          dpack.close()
          cleanup()
        })
      })
    })
  })
})

tape('replicate byte range', function (t) {
  makeTestServer(t, function runTest (dpackDir, destDir, cleanup) {
    var storage = dPackHttp('http://localhost:9988')
    var httpDDrive = ddrive(storage, {latest: true})
    httpDDrive.on('ready', function () {
      DPack(destDir, {key: httpDDrive.key, thin: true}, function (err, dpack) {
        if (err) return t.ifErr(err, 'error')
        var localReplicate = dpack.vault.replicate()
        localReplicate.pipe(httpDDrive.replicate()).pipe(localReplicate)
        var rs = dpack.vault.createReadStream('/numbers.txt', {start: 1000, end: 1099})
        rs.pipe(dwsChain(function (content) {
          t.equals(content.length, 100, 'length 100')
          t.equals(content.readUInt32BE(0), 1000/4, '250')
          t.ok(fs.readFileSync(path.join(destDir, 'numbers.txt')), 'file exists')
          cleanup()
        }))
      })
    })
  })
})

function makeTestServer (t, cb) {
  tmpDPack(t, function (err, dpackDir, dpackCleanup) {
    if (err) t.ifErr(err)
    tmp(function (err, destDir, tmpCleanup) {
      if (err) t.ifErr(err)
      var server = http.createServer(function (req, res) {
        console.log(req.method, req.url, '-', JSON.stringify(req.headers))
        ecstatic({ root: dpackDir })(req, res)
      })
      server.listen(9988, function (err) {
        if (err) t.ifErr(err)
        var cleanup = function () {
          tmpCleanup(function (err) {
            if (err) t.ifErr(err)
            dpackCleanup(function (err) {
              if (err) t.ifErr(err)
              server.close(function () {
                t.end()
              })
            })
          })
        }
        cb(dpackDir, destDir, cleanup)
      })
    })
  })
}

function tmpDPack (t, cb) {
  tmp(function created (err, dir, cleanup) {
    if (err) return cb(err)
    var bigBuf = new Buffer(1024 * 1024 * 10)
    for (var i = 0; i < bigBuf.length; i+=4) bigBuf.writeUInt32BE(i/4, i)
    fs.writeFileSync(path.join(dir, 'numbers.txt'), bigBuf)
    fs.writeFileSync(path.join(dir, 'greetings.txt'), 'greetings')
    DPack(dir, function (err, dpack) {
      if (err) return cb(err)
      dpack.importFiles(function (err) {
        if (err) return cb(err)
        dpack.close(function () {
          cb(null, dir, cleanup)
        })
      })
    })
  })
}
