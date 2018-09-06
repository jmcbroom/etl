## this script runs locally on the ETL box!

# run the process
python -c "import etl; etl.Process('graphql').update()"

# dump gql schema that's been transformed
pg_dump -d $ETL_DB -t "gql.*" > gql.sql

# send it over to the GraphQL box (set up authorized_keys)
scp gql.sql iotuser@10.208.37.176:~/gql.sql

# drop & create gql db
ssh iotuser@10.208.37.176 "dropdb -U graphql graphql"
ssh iotuser@10.208.37.176 "createdb -U graphql graphql"

# create postgis extension
ssh iotuser@10.208.37.176 "psql -d graphql -U graphql -c 'create extension postgis'"
ssh iotuser@10.208.37.176 "psql -d graphql -U graphql -c 'create schema gql'"


# load the data into the fresh database
ssh iotuser@10.208.37.176 "psql -d graphql -U graphql < ~/gql.sql"
ssh iotuser@10.208.37.176 "rm ~/gql.sql"

# write functions
ssh iotuser@10.208.37.176 "psql -d graphql -U graphql < ~/main.sql"