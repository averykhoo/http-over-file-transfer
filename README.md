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
* symmetric or public key
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
* uuid or sequential id?
  * uuid - more state, but easier to implement
  * sequential - less state stored (single int64), more bandwidth efficient, lower latency 
* custom file format?
  * probably start with json, stuffed into a jwt, and nested into a jwe (sign-then-encrypt)
  * make sure the sender and recipient are in the signed portion (as well as the filename)?
* subfolders?
  * server-2-uuid/server-2--server-1--sequence-id.json.jwt.jwe
* retry partial message success?
* maximum messages simultaneously in transit?

## ~~how (v1)~~

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

## how (v2 - reinventing the ~~wheel~~ osi model)

* layer 0 - unreliable file transfer
  * some folder that sometimes pushes files into the other folder
  * assume the folder is shared among multiple tenants
  * only ways to organize data are by subfolder and filename
* layer 1 - reliable secure message log replication
  * bounded message size, maybe up to 100mb
  * signed and encrypted
  * no partial message retries?
  * maybe send it in a framed format so we can concat multiple short messages using a greedy algorithm
  * base the replication algo on a lamport clock since it's easier to reason about
* layer 2 - http proxy
  * allows http requests to be split into multiple messages if they're too large
  * compression happens here
  * requires callback url
* optional frontend layer - ttl cache
  * allow user to poll and pull instead
* optional backend layer - oauth cache
  * cookies or client id/credentials
  * store and refresh tokens

### server state

* per other server
  * lamport clock
    * own last sent
    * last contiguous received
    * out of order received
  * outbox
    * data to send, id, acked
  * sent
    * nonce, timestamp, message ids (or none), acked
  * inbox
    * data received, id, sent timestamp, received timestamp, ack ack

### layer 1

* jwe
  * jwt
    * random nonce
    * metadata
      * sender uuid
      * recipient uuid
      * timestamp
    * ordered list of messages (possibly empty)
      * data
        * message id - lamport clock tick
        * data
      * control
        * sender's last sent message sequential id
        * last contiguous received message id
        * out of order (non-contiguous) received message ids and associated nonces

## stored state (server 1/2)

* un-acked messages (including un-acked acks perhaps)
* received message ids (for deduplication)
* shared secrets or public/private keys (to sign/verify and encrypt/decrypt)
* cached responses (for polling based system)

## token state

* sender / recipient server id (to de-conflict shared folder usage and prevent "surreptitious forwarding")
* uuid/autoincrement, decompressed size, decompressed checksum/hash (for ack)
* version -> v1
* timestamp
* data-format -> json, uncompressed
* data (request) (can be compressed?)
  * complete http request details, including files
  * caller's callback url
  * caller's ip? (for `x-forwarded-for`)
* data (response) (can be compressed?)
  * complete response details, including files
  * caller's callback url
  * callee's ip or other details?
  * round trip time?
* metadata?
  * acks for last received uuids / sequence ids

## possible libraries to look into

* encrypt, sign
  * [jwcrypto](https://pypi.org/project/jwcrypto/)
  * [python-jose](https://pypi.org/project/python-jose/)
* reference for http proxy
  * [pproxy](https://pypi.org/project/pproxy/)
* 