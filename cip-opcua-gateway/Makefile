IMAGE_NAME := cip-opcua-gateway
IMAGE_TAG  := latest
CONFIG_DIR := $(CURDIR)/config

.PHONY: build run run-bg stop logs shell

build:
	podman build -t $(IMAGE_NAME):$(IMAGE_TAG) -f Containerfile .

run:
	podman run --rm -it \
		--name $(IMAGE_NAME) \
		-p 4840:4840 \
		-p 8088:8088 \
		-v $(CONFIG_DIR)/tags.json:/config/tags.json:z \
		-e LOG_LEVEL=INFO \
		$(IMAGE_NAME):$(IMAGE_TAG)

run-bg:
	podman run -d \
		--name $(IMAGE_NAME) \
		-p 4840:4840 \
		-p 8088:8088 \
		-v $(CONFIG_DIR)/tags.json:/config/tags.json:z \
		$(IMAGE_NAME):$(IMAGE_TAG)

stop:
	podman stop $(IMAGE_NAME) && podman rm $(IMAGE_NAME)

logs:
	podman logs -f $(IMAGE_NAME)

shell:
	podman exec -it $(IMAGE_NAME) /bin/bash
