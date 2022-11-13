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
