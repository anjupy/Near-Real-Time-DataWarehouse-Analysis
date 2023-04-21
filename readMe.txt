
1)Run Transactional _ MasterData Generator.sql to create master data.

2)Run createDW.sql to create the Data Warehouse.

3)Run mj.java upon running it will ask for database credientials default are
  database name:db
  username: root
  password: ""
  This file will populate the data in the data warehouse after implementing the meshjoin.

4)Run queriesDW.sql to extract information from the Data warehouse using OLAP queries.

5)Open the report to view the project overview, mesh join algorithm, shortcomings of this algorithm and
  the learning outcomes of this project.