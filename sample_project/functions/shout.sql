{# Upper-cases a string and appends an exclamation point: shout('hi') -> 'HI!'.
   artifact_path: the JAR built from udfs/shout/ (rebuild with `make` in udfs/).
   path: Java class name of the udf inside that JAR. #}

{{ config(
    language='java',
    artifact_path='udfs/build/shout.jar',
    path='example.shout.Shout',

    on_configuration_change = 'apply',
) }}
