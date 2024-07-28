from create_tables import create_dwh_schema
from etl import fill_dwh_schema
from notebooks.L3_Ex_2_IaC import cluster_down, cluster_up

def main():
    """Main function to run the ETL process.

    This function performs the following steps:
    1. Start the Redshift cluster.
    2. Create the data warehouse schema.
    3. Load and transform the data.
    4. Shut down the Redshift cluster.
    """
    cluster_up()

    create_dwh_schema()
    fill_dwh_schema()

    cluster_down()

if __name__ == "__main__":
    main()
