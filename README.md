# http-over-file-transfer

allow api calls using a file transfer pipe

## decisions

* should the caller be notified about the request status as it happens
  * eg. when server 2 acks
* how to encode response body if it's not json (i.e. binary or non-utf8 text)
  * include content type and whether it's base64-encoded? or just store as latin1 and rely on escapes?
* support for polling and pulling instead of callbacks
  * requires much more state tracking
  * can be a caching wrapper (with ttl) built over the callback system, but vice-versa is also true
  * could also be an event-based system that we build an http layer over
* checksum or sign?
  * checksum - no need for secrets
  * sign - safe against spoofed files dropped by 3rd parties
* encryption optional?
  * encrypted - protects http body, like tls should, but needs shared secrets
  * plaintext - allows virus scans to do their job
* auditing?
  * nah
* dedupe?
  * requires stored state on server 2
* how to ack the ack?
  * or send with a sequence id and ack and increment last seen
  * or send the ack 3 times (in 3 separate files) and accept the overhead of resends
  * or use a crdt / vector clock / ratchet-like algo to count acks seen on both sides
* heartbeat?
* what if we want to run multiple instances in parallel over the same folders?
  * need to identify sender/recipient pairs
  * can also allow for any-to-any messaging, as long as it's whitelisted?
* collation / fragmentation
  * files > 1gb tend to be truncated
    * 500mb was the sweet spot for the mirroring service
  * files < 1mb create overhead, and < 10kb create fairly significant overhead (by proportion)
  * corruption is rare, truncation is the most common issue by far

## how

| caller                                                        | server 1                                                        | server 2                                                                                       | callee          |
|---------------------------------------------------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------|-----------------|
| send http request (with callback url)                         |                                                                 |                                                                                                |                 |
|                                                               | receive request                                                 |                                                                                                |                 |
|                                                               | translate request to file (json, compress, uuid, sign, encrypt) |                                                                                                |                 |
|                                                               | drop file into folder                                           |                                                                                                |                 |
|                                                               |                                                                 | receive, decrypt, validate (or drop without nack), dedupe (or re-ack), decompress, ack or nack |                 |
|                                                               | receive ack or resend after timeout or nack                     |                                                                                                |                 |
|                                                               |                                                                 | forward http request                                                                           |                 |
|                                                               |                                                                 |                                                                                                | receive request |
|                                                               |                                                                 |                                                                                                | process         |
|                                                               |                                                                 |                                                                                                | respond         |
|                                                               |                                                                 | translate response to file                                                                     |                 |
|                                                               |                                                                 | drop into folder                                                                               |                 |
|                                                               | receive, ....                                                   |                                                                                                |                 |
|                                                               |                                                                 | receive ack or resend after timeout                                                            |                 |
|                                                               | call caller's callback                                          |                                                                                                |                 |
| receive callback with response status code, body, and headers |                                                                 |                                                                                                |                 |

## possible libraries to look into

* encrypt, sign
  * [jwcrypto](https://pypi.org/project/jwcrypto/)
  * [python-jose](https://pypi.org/project/python-jose/)
* reference for http proxy
  * [pproxy](https://pypi.org/project/pproxy/)
* 