# Liteq

A single-file, persistent queue written only in Python's standard library. Inspired
by [AWS SQS](https://aws.amazon.com/sqs/) and [Huey](https://github.com/coleifer/huey)

## Disclaimer:

* AI agent has been used for coding and researching
* Very early stage of the project, do not use in production

## TODO

[x] long polling in the `pop` function
[] Backport uuidv7 for python<3.14
[] More robust put method to handle uuidv7 conflict, however very low probability
