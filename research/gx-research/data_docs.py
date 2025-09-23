import great_expectations as gx

context = gx.get_context()

# Define a Data Docs site configuration dictionary
base_directory = "uncommitted/data_docs/local_site/"  # this is the default path (relative to the root folder of the Data Context) but can be changed as required
site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": base_directory,
    },
}

# # Add the Data Docs configuration to the Data Context
# site_name = "my_data_docs_site"
# context.add_data_docs_site(site_name=site_name, site_config=site_config)
#
# # Manually build the Data Docs
# context.build_data_docs(site_names=site_name)

# # Automate Data Docs updates with a Checkpoint Action
# checkpoint_name = "my_checkpoint"
# validation_definition_name = "my_validation_definition"
# validation_definition = context.validation_definitions.get(validation_definition_name)
# actions = [
#     gx.checkpoint.actions.UpdateDataDocsAction(
#         name="update_my_site", site_names=[site_name]
#     )
# ]
# checkpoint = context.checkpoints.add(
#     gx.Checkpoint(
#         name=checkpoint_name,
#         validation_definitions=[validation_definition],
#         actions=actions,
#     )
# )

# result = checkpoint.run()

# View the Data Docs
context.open_data_docs()
