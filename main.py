from create_tables import create_dwh_schema
from etl import fill_dwh_schema
from notebooks.L3_Ex_2_IaC import cluster_down, cluster_up

if __name__ == "__main__":
    cluster_up()

    create_dwh_schema()
    fill_dwh_schema()

    cluster_down()
