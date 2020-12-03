#Set Up and Running
* The mysql database is run using docker, so you will first need to install docker.
* Download and unzip the latest page and redirect sql dumps and place them under `docker/database` with file 
names `enwiki-latest-page.sql` and `enwiki-latest-redirect.sql`.
* Run `docker build -f docker/database/Dockerfile -t mysqldb docker/database` to build the database image.
* Run `docker run --name util -e MYSQL_ROOT_PASSWORD=pword -e MYSQL_DATABASE=staging -d -p 3306:3306 mysqldb`
to start the database container. This will load in the sql dump data (warning: this takes a number of hours).
* Install python version 3.8+
* Install the python dependencies: `pip install -r requirements.txt`
* Once the data has loaded, run `python main.py` from within `src`.
* The asked-for queries are in `src/example_queries`

## Improvements
* The loading of the data dump into the staging database is very slow, so with more time I would look into a more efficient way of doing this.
* If the speed of processing is important then this could be parallelised by a number of workers taking a subset of the new redirects from the staging database.
* In a production system, the persistent database would not be run in a docker container but would be a properly managed, resilient,
database - and I would not use the root user.
* Add logging and error handling.
* Pass in configuration such as database parameters via environment variables rather than hard-coded.
