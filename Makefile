build-docker:
	sh scripts/build-image.sh

deploy-docker:
	docker compose -f docker-compose.yaml --profile processing up -d
	docker compose -f docker-compose.yaml --profile storage up -d
	docker compose -f docker-compose.yaml --profile query up -d
	docker compose -f docker-compose.yaml --profile analytics up -d

undeploy-docker:
	docker compose -f docker-compose.yaml --profile storage down | exit 0
	docker compose -f docker-compose.yaml --profile processing down | exit 0
	docker compose -f docker-compose.yaml --profile query down | exit 0
	docker compose -f docker-compose.yaml --profile analytics down | exit 0
test:
	# rm -rf data/test_src_g data_test_src_l test_write
	# docker build -f test.Dockerfile -t test-env .
	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		coverage run -m pytest --capture=no --log-cli-level=ERROR -p no:cacheprovider --cov=libs \
		-v tests/libs/test_meta.py \
		# -v tests/jobs/silver/test_schema.py
	## && sudo rm -rf data/test/

testcomp:
	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		pytest --capture=no --log-cli-level=ERROR \
		-v tests/libs/utils/test_company_verifications.py

		# -v tests/libs/utils/test_company_verifications_pure_name.py
		# -v tests/libs/utils/test_company_verifications.py
		# -v tests/jobs/silver/s_company_levenshtein_verification
		# -v tests/libs/utils/test_company_verifications.py \
		# -v tests/libs/utils/test_company_verifications.py
		# -v tests/jobs/silver/s_company_levenshtein_verification \
		# docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		# 	coverage run -m pytest --log-cli-level=ERROR \
		# 	-v tests/jobs/silver/s_company_levenshtein_verification/
		# sudo chown $USER lakehouse-silver/

testjob:
	# sudo rm -rf tests/resource/lakehouse-silver
	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		coverage run -m pytest --capture=no --log-cli-level=ERROR -p no:cacheprovider --cov=libs \
		-v tests/jobs/silver/h_company/test_h_company.py \
		-v tests/jobs/silver/s_company_demographic/test_s_company_demographic__src_newyork.py \
		-v tests/jobs/silver/s_company_address/test_s_company_address__src_newyork.py \


test1tm:
	# sudo rm -rf tests/resource/lakehouse-silver
	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		coverage run -m pytest --capture=no --log-cli-level=ERROR -p no:cacheprovider --cov=libs \
		-v tests/jobs/silver/h_company/test_h_company.py \
		-v tests/jobs/silver/s_company_demographic/test_s_company_demographic__src_newyork.py \
		-v tests/jobs/silver/s_company_address/test_s_company_address__src_newyork.py \


testbronze:
	# sudo rm -rf tests/resource/lakehouse-silver
	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		coverage run -m pytest --capture=no --log-cli-level=ERROR -p no:cacheprovider --cov=libs \
		-v tests/jobs/bronze/us/newyork/test_newyork.py
		# -v tests/jobs/bronze/yukon/test_yukon.py

jupyter:
	docker run --rm -p 8886:8888 -it -d -v ${PWD}/data:/opt/workspace/data --name jupyter jupyter-docker-env
	cd tests/resources
	sudo usermod -aG root minh-itc
	sudo chown -R minh-itc:root tests/resources
gendoc:
	pdoc --html libs --http localhost:8002


clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

precommit:
	python3 -m venv .venv
	source .venv/bin/activate
	pip install black pre-commit


rsync:
	# rm -rf libs.zip
	# zip -r data/libs.zip libs/ -x '*__pycache__*' '*ipynb_checkpoints*'
	# zip -r data/metas.zip metas/ -x '*__pycache__*' '*ipynb_checkpoints*'
	# zip -r data/resources.zip resources/ -x '*__pycache__*' '*ipynb_checkpoints*'
	# zip -r data/jobs.zip jobs/ -x '*__pycache__*' '*ipynb_checkpoints*'
	# # unzip jobs.zip
	# # rsync -av . root@10.0.0.4:/home/jupyter/delta-lakehouse-vault

	rsync -av --force  \
		--exclude '.*' \
		--exclude '*__pycache__' \
		--exclude 'test_local_*' \
		--exclude 'data' \
		--exclude '.git' \
		--exclude '1tm_2412' \
		. root@10.0.0.221:/opt/workspace/an_delta_lakehouse_vault
rsync:
	rsync -av --force  \
		--exclude '.*' \
		--exclude '*__pycache__' \
		--exclude 'test_local_*' \
		--exclude 'data' \
		--exclude '.git' \
		--exclude '1tm_2412' \
		--exclude 'tests' \
		. root@10.0.0.221:/opt/workspace/an_delta_lakehouse_vault
msync:
	rsync -av --force  \
		--exclude '.*' \
		--exclude '*__pycache__' \
		--exclude 'test_local_*' \
		--exclude 'data' \
		--exclude '.git' \
		--exclude '1tm_2412' \
		--exclude 'tests' \
		. root@10.0.0.221:/opt/workspace/minhnp_delta-lakehouse-vault

	rsync -av --force  \
		--exclude '.*' \
		--exclude '*__pycache__' \
		--exclude 'test_local_*' \
		--exclude 'data' \
		--exclude '.git' \
		--exclude '1tm_2412' \
		--exclude 'tests' \
		. root@10.0.0.231:/opt/workspace/minhnq_delta-lakehouse-vault

hoai_rsync:
	rsync -av --force  \
		--exclude '.*' \
		--exclude '*_pycache_' \
		--exclude 'test_local_*' \
		--exclude 'data' \
		--exclude '.git' \
		--exclude '1tm_2412' \
		. root@10.0.0.221:/opt/workspace/hoai_delta-lakehouse-vault
run:
	# docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
	# spark-submit /opt/workspace/jobs/silver/h_company/h_company__src_1tm_company.py


	# docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
	# 	spark-submit /opt/workspace/jobs/silver/s_company_address/s_company_address__src_1tm_company_address.py

	docker run --rm -it -v ${PWD}:/opt/workspace test-docker-env \
		spark-submit /opt/workspace/jobs/silver/s_company_demographic/s_company_demographic__src_1tm_company_demographic.py


	# sudo chown -R minh-itc:root tests/resources
