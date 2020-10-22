# VMware SD-WAN by VeloCloud Business Intelligence Intake

#### Installation

- Install Python 3.8 or greater
- Download and unzip files to a directory
- Move to the directory with the files in a CLI
- Create a VENV: `python3 -m venv venv`
- Activate the virtual environment: `source venv/bin/activate`
- Install the requirements: `pip3 install -r requirements.txt`
- Run the SQL commands from customer.sql on your against your database

#### Running the scripts

- Move to the base folder for this program
- Update the DataFiles/config.yml and vco_list.yml to have correct/relevant data
- Activate the virtual environment: `source venv/bin/activate`
- run the main script: `python3 ./powerbi_main_script.py --cf=DataFiles/config.yml --logging_file=some_file.log`
- Add a --debug to the above for your first few runs to find uncaught errors

### Description of Files

##### Setup Files
- requirements.txt: file with necessary python requirements that should be installed via pip
- customer.sql: commands for generating the database to interact with these scripts

##### Main Executor files:
- powerbi_main_script.py: Main script responsible for retrieving information from VCO.
- inventory_sla.py: Simple script to count customers and edges. Easy way to check if all customers/edges are getting 
  counted.

##### Variable Files:
- DataFiles/config.yml: primary config file
- Objects/Config.py: Object to store data from the config files
- DataFiles/vco_list.yml: VCO Access and information.
- DataFiles/country.json: standardizaton information for world regions/countries

##### Function Files:
- VCOClient.py:  Provided by VeloCloud Engineering provides basic call api calls for VCO.
- fun_mysql_insert.py: All functions for mysql inserts
- fun_mysql_queries.py: All functions for mysql queries
- Functions/sql_upserts.py: All functions for mysql update/inserts
- Functions/vco_calls.py: Functions that pull data from the VCO API
- Functions/data_sanitization.py: Generic data sanitization functions to help with data integrity
- Functions/helpers.py: Generic helper functions for data conversion etc.
- powerbi_main_fun.py: Provides functions for main powerbi script.




