

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Create project directory structure
mkdir -p src/{config,models,clients,dto,transformers,etl,repositories,validators,alerts,jobs,database,utils}
mkdir -p tests/{unit,integration}
mkdir -p docs
mkdir -p deployment
mkdir -p logs
mkdir -p alembic

# Create __init__.py files to make directories Python packages
touch src/__init__.py
touch src/config/__init__.py
touch src/models/__init__.py
touch src/clients/__init__.py
touch src/dto/__init__.py
touch src/transformers/__init__.py
touch src/etl/__init__.py
touch src/repositories/__init__.py
touch src/validators/__init__.py
touch src/alerts/__init__.py
touch src/jobs/__init__.py
touch src/database/__init__.py
touch src/utils/__init__.py
touch tests/__init__.py
touch tests/unit/__init__.py
touch tests/integration/__init__.py