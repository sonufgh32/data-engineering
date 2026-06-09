terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

resource "aws_dynamodb_table" "basic-dynamodb-table" {
  name           = "TournamentMatches"
  billing_mode   = "PROVISIONED"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "matchId"

  attribute {
    name = "matchId"
    type = "S"
  }

  attribute {
    name = "tournamentId"
    type = "S"
  }

  attribute {
    name = "region"
    type = "S"
  }

  attribute {
    name = "round"
    type = "S"
  }

  attribute {
    name = "bracket"
    type = "S"
  }

  attribute {
    name = "playerId"
    type = "N"
  }

  attribute {
    name = "matchDate"
    type = "S"
  }

  ttl {
    attribute_name = "TimeToExist"
    enabled        = true
  }

  # GSI with multiple HASH keys and multiple RANGE keys using key_schema
  global_secondary_index {
    name = "TournamentRegionIndex"
    key_schema {
      attribute_name = "tournamentId"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "region"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "round"
      key_type       = "RANGE"
    }
    key_schema {
      attribute_name = "bracket"
      key_type       = "RANGE"
    }
    key_schema {
      attribute_name = "matchId"
      key_type       = "RANGE"
    }
    write_capacity  = 10
    read_capacity   = 10
    projection_type = "ALL"
  }

  # GSI with single HASH key and multiple RANGE keys using key_schema
  global_secondary_index {
    name = "PlayerMatchHistoryIndex"
    key_schema {
      attribute_name = "playerId"
      key_type       = "HASH"
    }
    key_schema {
      attribute_name = "matchDate"
      key_type       = "RANGE"
    }
    key_schema {
      attribute_name = "round"
      key_type       = "RANGE"
    }
    write_capacity  = 10
    read_capacity   = 10
    projection_type = "ALL"
  }

  tags = {
    Name        = "dynamodb-table-1"
    Environment = "production"
  }
}