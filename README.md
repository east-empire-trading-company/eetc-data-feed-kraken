# EETC Data Feed - Kraken
Service that streams live data from Kraken using its websocket API. The data is
then distributed further via Zero MQ to EETC Data Feed, which acts as a proxy
that distributes the data to the actual clients.

## System requirements
To run the project locally and work on it, you need the following:
- Python 3.8+

## Project setup
```commandline
sudo apt-get install build-essential
make update_and_install_python_requirements
```

## Adding a new Python package
1. Add the package name to `requirements.in`
2. Run:
```commandline
make update_and_install_python_requirements
```
