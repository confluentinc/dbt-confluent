{{
  config(
    materialized='streaming_source',
    connector='faker',
    with={
      'changelog.mode': 'append',
      'rows-per-second': '5',
      'fields.id.expression': "#{Number.numberBetween '1','1000000'}",
      'fields.name.expression': '#{Name.name}',
      'fields.email.expression': '#{Internet.emailAddress}',
      'fields.city.expression': "#{options.option 'New York','Los Angeles','Chicago','Houston','Phoenix'}"
    }
  )
}}

`id` INT NOT NULL,
`name` STRING NOT NULL,
`email` STRING NOT NULL,
`city` STRING NOT NULL
