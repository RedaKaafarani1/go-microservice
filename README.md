# GO CSV microservice

This is a CSV processing microservice written purely in GO.

## How it works

This go microservice opens a server that listens for incoming requests through two endpoints that serve different purposes. It's for a very specific personal project and it's used to process huge CSV files containing data about France's population/administrative/crime/Establishments data.
Put simply, this app receives GEOJSON polygons and then finds zones on the map that intersect this polygon (data from the CSV files) and does calculations on them and then returns them in a JSON response. It's implemented to use parallel processing to achieve higher efficiency in data processing.

## How it's used

This microservice is packaged using Docker and then hosted on Github packages. It is then pulled using Docker compose and ran along other containers that serve a website written in PHP (Laravel) and Vue.js. The exposed port is then used to receive requests to process and return associated results.

## Public use

This microservice only serves a specific use case for my personal project and is not intended for public use. However, its use is not prohibited.
