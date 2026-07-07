.PHONY: single-up single-down sentinel-up cluster-up cluster-init test bench tidy lab

single-up:      ## 起單機 redis + RedisInsight(5540)
	docker compose --profile single up -d

single-down:
	docker compose --profile single down

sentinel-up:    ## 起 1 master + 2 replica + 3 sentinel
	docker compose --profile sentinel up -d

sentinel-down:
	docker compose --profile sentinel down

cluster-up:     ## 起 6 節點 (起完跑 make cluster-init)
	docker compose --profile cluster up -d

cluster-init:   ## 建立 cluster slot 分配
	docker compose --profile cluster exec node1 \
		redis-cli --cluster create \
		node1:6379 node2:6379 node3:6379 node4:6379 node5:6379 node6:6379 \
		--cluster-replicas 1 --cluster-yes

cluster-down:
	docker compose --profile cluster down

tidy:
	go mod tidy

test:           ## 全單測 (用 miniredis, 免 docker)
	go test ./...

bench:          ## 跑 benchmark
	go test -bench=. -benchmem ./...

# 跑某個 lab: make lab N=05-distributed-lock
lab:
	go run ./labs/$(N)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'
