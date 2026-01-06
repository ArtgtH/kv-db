status() {
  echo "== kv1"; curl -s localhost:8081/status | jq
  echo "== kv2"; curl -s localhost:8082/status | jq
  echo "== kv3"; curl -s localhost:8083/status | jq
}

leader_port() {
  for p in 8081 8082 8083; do
    st=$(curl -s localhost:$p/status)
    id=$(echo "$st" | jq -r .leader_id)
    addr=$(echo "$st" | jq -r .leader_addr)
    if [ "$id" != "null" ] && [ "$id" != "" ]; then
      echo "$p $id $addr"
      return 0
    fi
  done
  return 1
}

put() { curl -s -X PUT "localhost:$1/kv/$2" -d "{\"value\":\"$3\"}" -H 'Content-Type: application/json' | jq; }
get() { curl -s "localhost:$1/kv/$2" | jq; }
del() { curl -s -X DELETE "localhost:$1/kv/$2" | jq; }
