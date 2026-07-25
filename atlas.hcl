variable "database_url" {
  type    = string
  default = getenv("DATABASE_URL")
}

# Local development (PostgreSQL 18 via docker compose or a local instance).
env "local" {
  src = "file://internal/schema/schema.sql"
  url = "postgres://cp:cp@localhost:5432/cp?sslmode=disable&search_path=public"
  dev = "docker://postgres/18/dev?search_path=public"

  migration {
    dir = "file://migrations"
  }
}

# Production PostgreSQL.
env "postgres" {
  src = "file://internal/schema/schema.sql"
  url = var.database_url
  dev = "docker://postgres/18/dev?search_path=public"

  migration {
    dir = "file://migrations"
  }
}

# CI environment.
env "ci" {
  src = "file://internal/schema/schema.sql"
  url = "postgres://cp:cp@localhost:5432/cp?sslmode=disable&search_path=public"
  dev = "docker://postgres/18/dev?search_path=public"

  migration {
    dir = "file://migrations"
  }
}
