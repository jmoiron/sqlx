ci:
	bash -c '(docker-compose -f docker-compose.test.yml -p sqlx_ci up --build -d) && (docker logs -f sqlx_sut &) && (docker wait sqlx_sut)'
