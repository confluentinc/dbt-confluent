{# True if the first T-shirt size is smaller than the second.
   artifact_path: the zipped sdist of udfs/tshirt_sizing/ (rebuild steps in udfs/README.md).
   class: Python module path of the udf inside that package. #}

{{ config(
    language='python',
    artifact_path='udfs/build/tshirt_sizing.zip',
    path='tshirt_sizing.tshirt_sizing.is_smaller',

    on_configuration_change = 'apply',
) }}
