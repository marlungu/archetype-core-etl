resource "aws_db_subnet_group" "main" {
  name       = "${var.project_name}-${var.environment}-db-subnet"
  subnet_ids = var.private_subnet_ids

  tags = { Name = "${var.project_name}-${var.environment}-db-subnet" }
}

resource "aws_security_group" "rds" {
  name        = "${var.project_name}-${var.environment}-rds-sg"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = var.vpc_id

  tags = { Name = "${var.project_name}-${var.environment}-rds-sg" }
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_mwaa" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = var.allowed_security_group_ids[0]
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "rds_all_outbound" {
  security_group_id = aws_security_group.rds.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_db_instance" "main" {
  identifier        = "${var.project_name}-${var.environment}-audit"
  engine            = "postgres"
  engine_version    = "16.3"
  instance_class    = var.instance_class
  allocated_storage = 20
  storage_type      = "gp3"
  storage_encrypted = true

  db_name                     = var.db_name
  username                    = var.db_username
  manage_master_user_password = true

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = false
  publicly_accessible = false

  skip_final_snapshot       = true
  final_snapshot_identifier = "${var.project_name}-${var.environment}-final"
  backup_retention_period   = 1

  tags = { Name = "${var.project_name}-${var.environment}-audit-db" }
}
