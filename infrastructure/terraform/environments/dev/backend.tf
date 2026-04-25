terraform {
  backend "s3" {
    bucket         = "archetype-core-etl-tfstate"
    key            = "archetype-core-etl/dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "archetype-core-etl-tflocks"
    profile        = "archetype"
  }
}
