# dagster_orchestration/dagster_orchestration/assets/__init__.py

import os
from dagster_airbyte import AirbyteResource, AirbyteWorkspace, build_airbyte_assets_definitions, load_assets_from_airbyte_instance
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=os.getenv("DBT_PROJECT_DIR"),
    profiles_dir=os.getenv("DBT_PROFILES_DIR"),
)
dbt_project.prepare_if_dev()

airbyte_workspace = AirbyteWorkspace(
    rest_api_base_url="http://localhost:8000/api/public/v1",
    configuration_api_base_url="http://localhost:8000/api/v1",
    workspace_id=os.getenv("AIRBYTE_WORKSPACE_ID"),
    client_id=os.getenv("AIRBYTE_CLIENT_ID"),
    client_secret=os.getenv("AIRBYTE_CLIENT_SECRET"),
)

resources = {
    "dbt": DbtCliResource(
        project_dir=os.getenv("DBT_PROJECT_DIR"),
        profiles_dir=os.getenv("DBT_PROFILES_DIR"),
    ),
    "airbyte": airbyte_workspace,
}

@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_transformation_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

airbyte_assets = build_airbyte_assets_definitions(workspace=airbyte_workspace)
