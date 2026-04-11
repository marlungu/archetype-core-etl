# Remote state backend for the dev environment.
#
# The state bucket and DynamoDB lock table must be provisioned out-of-band
# before `terraform init`. Replace every REPLACE_ME value below with the
# real bootstrap resources for this environment.

terraform {
  backend "s3" {
    bucket         = "REPLACE_ME-archetype-tfstate"
    key            = "archetype-core-etl/dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "REPLACE_ME-archetype-tflocks"
  }
}
