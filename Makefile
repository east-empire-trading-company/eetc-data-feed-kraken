install_python_requirements:
	pip install pip-tools
	pip install -r requirements.txt

update_python_requirements:
	pip install pip-tools
	pip-compile --upgrade

update_and_install_python_requirements: update_python_requirements install_python_requirements

reformat_code:
	black .

compile_kraken_msg_proto:
	protoc -I=. --python_out=. ./kraken_msg.proto

build_docker_kraken_image:
	docker build --tag kraken .

build_and_deploy_docker_kraken_image: build_docker_kraken_image
	gcloud config set project eetc-data-feed-kraken
	gcloud auth configure-docker us-east1-docker.pkg.dev
	docker tag kraken:latest \us-east1-docker.pkg.dev/eetc-data-feed-kraken/docker-kraken-repo/kraken:latest
	docker push us-east1-docker.pkg.dev/eetc-data-feed-kraken/docker-kraken-repo/kraken:latest

