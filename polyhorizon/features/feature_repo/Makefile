# Load environment variables from .env
ifneq ("$(wildcard .env)","")
    include .env
    export
endif

.PHONY: feast-list feast-apply job-run

# Run Feast commands from the root, pointing to the sub-directory
feast-list:
	uv run feast feature-views list  
# 	-c feature_repo/

feast-apply:
	uv run feast apply
# 	-c feature_repo/

# Run your streaming job
job-run:
	uv run python src/job.py