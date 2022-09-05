cd ~/go/src/mit6/src/shardkv;
for i in {0..10}; do go test -run TestConcurrent3; done
