import great_expectations as gx

context = gx.get_context(mode="file")

# Optional. Review the configuration of the returned File Data Context.
print(context)
print(gx.__version__)
