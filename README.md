# http-over-file-transfer

allow api calls using a file transfer pipe

## decisions

* [ ] should the caller be notified about the request status as it happens
  * eg. when server 2 acks
* [ ] how to encode response body if it's not json (i.e. binary or non-utf8 text)
  * include content type and whether it's base64-encoded? or just store as latin1 and rely on escapes?
* [ ] support for polling and pulling instead of callbacks
  * requires much more state tracking
  * can be a caching wrapper (with ttl) built over the callback system, but vice-versa is also true
  * could also be an event-based system that we build an http layer over
* [x] checksum or sign?
  * checksum - no need for secrets
  * sign - safe against spoofed files dropped by 3rd parties
* [x] encryption optional?
  * encrypted - protects http body, like tls should, but needs shared secrets
  * plaintext - allows virus scans to do their job
* [ ] symmetric or public key
* [ ] logging / auditing / metrics
  * metrics / statistics
    * usage / users / byte size histogram
    * speed / throughput / latency
    * error percentages
  * ignore auditing for now
* [x] dedupe?
  * requires stored state on server 2
* [x] how to ack the ack?
  * or send with a sequence id and ack and increment last seen
  * or send the ack 3 times (in 3 separate files) and accept the overhead of resends
  * or use a crdt / vector clock / ratchet-like algo to count acks seen on both sides
* [ ] heartbeat?
* [x] what if we want to run multiple instances in parallel over the same folders?
  * need to identify sender/recipient pairs
  * can also allow for any-to-any messaging, as long as it's whitelisted?
* [x] collation / fragmentation (handle on layer 2)
  * files > 1gb tend to be truncated
    * 500mb was the sweet spot for the mirroring service
  * files < 1mb create overhead, and < 10kb create fairly significant overhead (by proportion)
  * corruption is rare, truncation is the most common issue by far
* [x] uuid or sequential id?
  * ~~uuid - more state, but easier to implement~~
  * sequential - less state stored (single int64), more bandwidth efficient, lower latency
* [x] custom binary file format?
  * probably start with json, stuffed into a jwt, and nested into a jwe (sign-then-encrypt)
  * make sure the sender and recipient are in the signed portion (as well as the filename)?
* [x] subfolders?
  * server-2-uuid/server-2--server-1--sequence-id.json.jwt.jwe
* [x] retry partial message success?
  * this is not out of order acks (see also tcp sack)
  * may be useful because the most common error is truncation
* [ ] rate limiting
  * maximum messages simultaneously in transit?
  * maximum bandwidth?
  * maximum calls from some callee / bytes per day or something
* [x] transfer optimization by calculating bandwidth (based on throughput / latency)
  * see also TCP Vegas, which attempts something similar, meaning it's not entirely unreasonable to attempt
  * maybe also calculate error rate?
  * truncation stats and sizes to optimize packet size?
  * priority queue?
* [x] how to handle nacks? 
  * not stateful and may need to be retransmitted
  * but we don't want to cause an avalanche of nacks, which could happen if a nack message is nacked
  * also it's optional since we'll eventually hit retransmit timeout
* [x] congestion control? <- no, assume the channel has an infinite queue
  * maybe ignore fairness for now, just hog the channel
  * consider a model based approach like bbr
    * but since we can get the actual network diagram maybe just optimize it for that?
* [x] pessimistic retransmits? <- no
  * not necessary (yet)

## how (v2 - reinventing the ~~wheel~~ osi model)

* layer 0 - unreliable file transfer
  * some folder that sometimes pushes files into the other folder
  * assume the folder is shared among multiple tenants
  * only ways to organize data are by subfolder and filename
  * optionally write multiple sub-files or add error correction
  * compression should be handled here? just write into a gzipfile?
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
  * include full schema+creds+url+query+params, headers, timeout?, verb, cookie
* optional frontend layer - ttl cache
  * allow user to poll and pull instead
* optional backend layer - oauth cache
  * cookies or client id/credentials
  * store and refresh tokens

### server state

* own private key (if using pki)
* per other server
  * other public key / shared secret
  * retransmission timeout
    * plus additional delay for seconds per megabyte or something?
  * own lamport clock
    * own last sent
    * last contiguous received
    * out of order received <- property calculated from inbox?
  * other lamport clock
    * received
    * sent
  * outbox
    * data to send, id, sent timestamp, acked timestamp
  * inbox
    * data received, id, sent timestamp, received timestamp, ack ack timestamp
* maybe a housekeeping script?
  * can remove acked messages from outbox
  * can remove from sent when other lamport clock exceeds it
  * can remove double-acked from inbox
  * (extension) use "processing start timestamp" flag to multithread processing of received messages with timeout
  * (extension) use "processed" flag or clock to determine which messages can be removed

### layer 0

* some folder
  * recipient
    * {sender}--{recipient}--{id}.json
* write as a hidden file with a . prefix then rename/move once done
* read only when sure the file is fully written - either keep state of timestamp and file size bytes or use mtime/ctime
  * handle weird / negative time differences between reading and writing?
  * needs a timeout after last byte is written before we read the file?
    * or just yolo for reading, ignore/skip errors, and use this timeout only to delete invalid files?
* maybe add error correction? 

### layer 1

* jwe
  * jwt
    * metadata (also helps prevent "surreptitious forwarding")
      * sender uuid - expected to match filename?
      * recipient uuid
      * timestamp
      * protocol version
    * ordered list of messages (possibly empty)
      * data
        * message id - lamport clock tick
        * content type? (escaped unicode auto handled by json decoder) plaintext / base64-binary / json / compressed?
        * escaped/base64 data (reject anything above certain size)
    * control (optional?)
      * sender's last sent data message sequential id
      * last contiguous received message id
      * out of order (non-contiguous) received message ids and associated nonces

### layer 2

* data (request) (can be compressed?)
  * complete http request details, including files
  * caller's callback url
  * caller's ip? (for `x-forwarded-for`)
* data (response) (can be compressed?)
  * complete response details, including files
  * caller's callback url
  * callee's ip or other details?
  * round trip time?

## possible libraries to look into

* encrypt, sign
  * [jwcrypto](https://pypi.org/project/jwcrypto/)
  * [python-jose](https://pypi.org/project/python-jose/)
* reference for http proxy
  * [pproxy](https://pypi.org/project/pproxy/)
* alternative: use a custom binary format, handle signing and encryption manually
  * maybe use a known format?
    * protobuf / flatbuffers
    * cbor / messagepack
    * avro / parquet / pickle / ion (amazon) / thrift / 
  * message format a bit like jwe / jwt / jws (jose)
    * header
    * data (signed and encrypted with random key and iv)
    * encrypted random key, iv
    * hmac with random key
* error correction codes
  * raptorq
  * par2cmdline
  * reed-solomon
  * just append nulls (after a 0xFF end flag) since we only really get truncation errors
* binary encoding - maybe try base85? slower but more space efficient, and we're probably network limited