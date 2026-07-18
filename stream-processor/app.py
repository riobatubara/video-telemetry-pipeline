from src.config import print_metrics
from src.setup import create_flink_environments
from src.tables import register_tables
from src.pipelines import build_and_run_pipeline

def main():
    # 1. Show diagnostic metrics
    print_metrics()
    
    # 2. Spin up Flink engine
    t_env = create_flink_environments()
    
    # 3. Register sources and sinks
    register_tables(t_env)
    
    # 4. Bind queries and run
    build_and_run_pipeline(t_env)

if __name__ == "__main__":
    main()
