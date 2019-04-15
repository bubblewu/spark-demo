#!/bin/sh

createIndexTime=$1

curl --request POST  --url 'http://127.0.0.1:9200/huoli-rec/tips/_delete_by_query?pretty=' --header 'Content-Type: application/json' --data '{"query": {"bool": { "filter": {"range": {"createIndexTime": {"lte": '${createIndexTime}'  }  } }} }}'