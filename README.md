# Liteq

A single-file, persistent queue written only in Python's standard library. Inspired
by [AWS SQS](https://aws.amazon.com/sqs/) and [Huey](https://github.com/coleifer/huey)

## Disclaimer:

* AI agent has been used for coding and researching
* Very early stage of the project, do not use in production

## TODO

* [x] long polling in the `pop` function
* [x] Backport uuidv7 for python<3.14
* [x] More robust put method to handle uuid v7 conflict, however very low probability
